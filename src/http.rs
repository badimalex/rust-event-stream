use axum::{
    Json, Router,
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
};

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
    Router::new()
        .route("/health", get(health))
        .route("/ready", get(ready))
        .route("/v1/events", post(create_event_handler))
        .with_state(state)
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
    use tower::util::ServiceExt;

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
