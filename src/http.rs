use std::time::Duration;

use axum::{
    BoxError, Json, Router,
    error_handling::HandleErrorLayer,
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
};
use tower::ServiceBuilder;
use tower_http::limit::RequestBodyLimitLayer;

use crate::{
    domain::{DomainError, Event},
    pipeline::EventProducer,
};
use serde::{Deserialize, Serialize};

#[derive(Clone)]
pub struct AppState {
    pub producer: EventProducer,
}

pub fn build_router(state: AppState) -> Router {
    let api_router = Router::new()
        .route("/v1/events", post(create_event_handler))
        .layer(
            ServiceBuilder::new()
                .layer(RequestBodyLimitLayer::new(1024 * 1024))
                .layer(HandleErrorLayer::new(handle_middleware_errors))
                .load_shed()
                .concurrency_limit(100)
                .timeout(Duration::from_secs(5)),
        );

    Router::new()
        .route("/health", get(health))
        .route("/ready", get(ready))
        .merge(api_router)
        .with_state(state)
}

async fn handle_middleware_errors(err: BoxError) -> (StatusCode, String) {
    if err.is::<tower::timeout::error::Elapsed>() {
        return (
            StatusCode::REQUEST_TIMEOUT,
            "Request timed out.".to_string(),
        );
    }

    if err.is::<tower::load_shed::error::Overloaded>() {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            "Server is overloaded. Please try again later.".to_string(),
        );
    }

    (
        StatusCode::INTERNAL_SERVER_ERROR,
        format!("Unhandled middleware error: {}", err),
    )
}

#[derive(Debug, Deserialize)]
pub struct CreateEventRequest {
    pub event_id: String,
    pub tenant_id: String,
    pub event_type: String,
    pub timestamp: u64,
    pub payload: String,
}

// Ответ в случае ошибки валидации
#[derive(Serialize)]
pub struct ApiErrorResponse {
    pub error: String,
}

// Конвертация DTO -> Domain с валидацией
impl TryFrom<CreateEventRequest> for Event {
    type Error = DomainError;

    fn try_from(dto: CreateEventRequest) -> Result<Self, Self::Error> {
        Event::new(
            dto.event_id,
            dto.tenant_id,
            dto.event_type,
            dto.timestamp,
            dto.payload,
        )
    }
}

// Маппинг ошибок домена в HTTP ответы
impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        match self {
            ApiError::Domain(err) => {
                let (status, message) = match err {
                    DomainError::EmptyField(field) => (
                        StatusCode::BAD_REQUEST,
                        format!("Поле '{}' не должно быть пустым", field),
                    ),
                    DomainError::InvalidTimestamp => (
                        StatusCode::BAD_REQUEST,
                        "Временная метка (timestamp) должна быть больше 0".to_string(),
                    ),
                };
                (status, Json(ApiErrorResponse { error: message })).into_response()
            }

            ApiError::PipelineUnavailable => {
                (StatusCode::SERVICE_UNAVAILABLE, "pipeline unavailable").into_response()
            }
        }
    }
}

enum ApiError {
    Domain(DomainError),
    PipelineUnavailable,
}

async fn create_event_handler(
    State(state): State<AppState>,
    Json(payload): Json<CreateEventRequest>,
) -> Result<StatusCode, ApiError> {
    let event: Event = payload.try_into().map_err(ApiError::Domain)?;

    state
        .producer
        .send_event(event)
        .await
        .map_err(|_| ApiError::PipelineUnavailable)?;

    Ok(StatusCode::CREATED)
}

async fn health() -> &'static str {
    "ok"
}

async fn ready() -> &'static str {
    "ok"
}

#[cfg(test)]
mod tests {
    use crate::pipeline::BoundedQueue;

    use super::*;

    use axum::{
        body::Body,
        http::{Request, StatusCode},
    };
    use tokio::task::JoinSet;
    use tower::util::ServiceExt;

    #[tokio::test]
    async fn oversized_body_is_rejected() {
        let (producer, _, _) = BoundedQueue::new(100);
        let shared_state = AppState { producer };

        let app = build_router(shared_state);

        let oversized_bytes = vec![0u8; 1_048_576 + 1];

        let request = axum::http::Request::builder()
            .method(axum::http::Method::POST)
            .uri("/v1/events")
            .header(axum::http::header::CONTENT_TYPE, "application/json")
            .body(axum::body::Body::from(oversized_bytes))
            .unwrap();

        use tower::ServiceExt;
        let response = app.oneshot(request).await.unwrap();

        assert_eq!(response.status(), axum::http::StatusCode::PAYLOAD_TOO_LARGE);
    }

    #[tokio::test]
    async fn slow_request_times_out() {
        let (producer, mut _queue, _worker) = BoundedQueue::new(1);
        let shared_state = AppState { producer };

        let app = build_router(shared_state);

        let request = axum::http::Request::builder()
            .method("POST")
            .uri("/v1/events")
            .header("content-type", "application/json")
            .body(Body::from(r#"{"event_id":"1","tenant_id":"2","event_type":"click","timestamp":1700000000,"payload":"test"}"#))
            .unwrap();

        let clone_app = app.clone();
        let response = app.oneshot(request).await.unwrap();

        assert_eq!(response.status(), StatusCode::CREATED);

        let request = axum::http::Request::builder()
            .method("POST")
            .uri("/v1/events")
            .header("content-type", "application/json")
            .body(Body::from(r#"{"event_id":"2","tenant_id":"2","event_type":"click","timestamp":1700000000,"payload":"test"}"#))
            .unwrap();

        let response = clone_app.oneshot(request).await.unwrap();

        assert_eq!(response.status(), axum::http::StatusCode::REQUEST_TIMEOUT);
    }

    #[tokio::test]
    async fn concurrency_limit_is_enforced() {
        let (producer, mut _queue, _worker) = BoundedQueue::new(1);
        let shared_state = AppState { producer };

        let app = build_router(shared_state);
        let app_clone = app.clone();

        let request = axum::http::Request::builder()
            .method("POST")
            .uri("/v1/events")
            .header("content-type", "application/json")
            .body(Body::from(r#"{"event_id":"1","tenant_id":"2","event_type":"click","timestamp":1700000000,"payload":"test"}"#))
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);

        let mut join_set = JoinSet::new();

        for _ in 0..101 {
            let req_service = app_clone.clone();

            join_set.spawn(async move {
                let request = axum::http::Request::builder()
                    .method("POST")
                    .uri("/v1/events")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"event_id":"1","tenant_id":"2","event_type":"click","timestamp":1700000000,"payload":"test"}"#))
                    .unwrap();

                req_service.oneshot(request).await.unwrap()
            });
        }

        let mut responses = Vec::new();

        while let Some(res) = join_set.join_next().await {
            responses.push(res.unwrap());
        }

        let mut timeout_count = 0;
        let mut overload_count = 0;

        for response in responses {
            match response.status() {
                StatusCode::REQUEST_TIMEOUT => timeout_count += 1,
                StatusCode::SERVICE_UNAVAILABLE => overload_count += 1,
                status => panic!("unexpected status: {status}"),
            }
        }

        assert_eq!(timeout_count, 100);
        assert_eq!(overload_count, 1);
    }

    #[tokio::test]
    async fn health_returns_success() {
        let (producer, _, _) = BoundedQueue::new(100);
        let shared_state = AppState { producer };
        let app = build_router(shared_state);

        let request = Request::builder()
            .method("GET")
            .uri("/health")
            .body(Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn ready_returns_success() {
        let (producer, _, _) = BoundedQueue::new(100);
        let shared_state = AppState { producer };
        let app = build_router(shared_state);

        let request = Request::builder()
            .method("GET")
            .uri("/ready")
            .body(Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn invalid_json_is_rejected() {
        let (producer, _, _) = BoundedQueue::new(100);
        let shared_state = AppState { producer };
        let app = build_router(shared_state);

        let request = Request::builder()
            .method("POST")
            .uri("/v1/events")
            .header("content-type", "application/json")
            .body(Body::from(r#"{"event_id":"1","tenant_id":"#))
            .unwrap();

        let response = app.oneshot(request).await.unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn valid_event_is_accepted() {
        let (producer, mut queue, worker) = BoundedQueue::new(100);
        let shared_state = AppState { producer };
        let sink = worker.sink.clone();

        queue.spawn(worker);

        let app = build_router(shared_state);

        // 4. Construct the HTTP request
        let request = Request::builder()
            .method("POST")
            .uri("/v1/events")
            .header("content-type", "application/json")
            .body(Body::from(r#"{"event_id":"1","tenant_id":"2","event_type":"click","timestamp":1700000000,"payload":"test"}"#))
            .unwrap();

        // 5. Execute the request against the router
        let response = app.oneshot(request).await.unwrap();

        // 6. Assert the response status is 200 OK
        assert_eq!(response.status(), StatusCode::CREATED);
        queue.shutdown().await.unwrap();

        assert_eq!(sink.records.lock().await.len(), 1);
        assert_eq!(
            sink.records.lock().await[0],
            Event::new(
                "1".to_string(),
                "2".to_string(),
                "click".to_string(),
                1700000000,
                "test".to_string()
            )
            .unwrap()
        );
    }

    #[tokio::test]
    async fn closed_or_unavailable_pipeline_is_not_reported_as_success() {
        let (producer, mut queue, worker) = BoundedQueue::new(100);
        let shared_state = AppState { producer };

        drop(worker);

        let app = build_router(shared_state);

        // 4. Construct the HTTP request
        let request = Request::builder()
            .method("POST")
            .uri("/v1/events")
            .header("content-type", "application/json")
            .body(Body::from(r#"{"event_id":"1","tenant_id":"2","event_type":"click","timestamp":1700000000,"payload":"test"}"#))
            .unwrap();

        // 5. Execute the request against the router
        let response = app.oneshot(request).await.unwrap();

        // 6. Assert the response status is 200 OK
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        queue.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn invalid_event_is_rejected() {
        let (producer, mut queue, worker) = BoundedQueue::new(100);
        let shared_state = AppState { producer };
        let sink = worker.sink.clone();
        queue.spawn(worker);

        let app = build_router(shared_state);

        let request = Request::builder()
            .method("POST")
            .uri("/v1/events")
            .header("content-type", "application/json")
            .body(Body::from(r#"{"event_id":"","tenant_id":"2","event_type":"click","timestamp":1700000000,"payload":"test"}"#))
            .unwrap();

        let response = app.oneshot(request).await.unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        queue.shutdown().await.unwrap();

        assert_eq!(sink.records.lock().await.len(), 0);
    }
}
