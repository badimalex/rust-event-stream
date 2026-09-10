use std::time::Duration;

use axum::{
    BoxError, Json, Router,
    error_handling::HandleErrorLayer,
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
};
use tower::ServiceBuilder;
use tower_http::limit::RequestBodyLimitLayer;

use crate::{
    domain::{DomainError, Event},
    pipeline::{EventProducer, EventSendError},
    storage::Storage,
};
use serde::{Deserialize, Serialize};

#[derive(Clone)]
pub struct AppState<S>
where
    S: Storage + Clone,
{
    pub producer: EventProducer,
    pub storage: S,
}

pub fn build_router<S>(state: AppState<S>) -> Router
where
    S: Storage + Clone + 'static,
{
    let api_router = Router::new()
        .route("/v1/events/{id}", get(get_event::<S>))
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

            ApiError::NotFound => StatusCode::NOT_FOUND.into_response(),

            ApiError::Internal => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
        }
    }
}

enum ApiError {
    Domain(DomainError),
    PipelineUnavailable,
    NotFound,
    Internal,
}

async fn create_event_handler<S>(
    State(state): State<AppState<S>>,
    Json(payload): Json<CreateEventRequest>,
) -> Result<StatusCode, ApiError>
where
    S: Storage + Clone + 'static,
{
    let event: Event = payload.try_into().map_err(ApiError::Domain)?;

    return match state.producer.send_event(event).await {
        Ok(_) => Ok(StatusCode::CREATED),
        Err(EventSendError::QueueClosed | EventSendError::WorkerDropped) => {
            Err(ApiError::PipelineUnavailable)
        }
        Err(EventSendError::Storage(error)) => {
            eprintln!("storage error while creating event: {error}");
            Err(ApiError::Internal)
        }
    };
}

async fn get_event<S>(
    State(state): State<AppState<S>>,
    Path(event_id): Path<String>,
) -> Result<Json<Event>, ApiError>
where
    S: Storage + Clone + 'static,
{
    match state.storage.get_by_id(&event_id).await {
        Ok(Some(event)) => Ok(Json(event)),
        Ok(None) => Err(ApiError::NotFound),
        Err(error) => {
            eprintln!("storage error: {error}");
            Err(ApiError::Internal)
        }
    }
}

async fn health() -> &'static str {
    "ok"
}

async fn ready() -> &'static str {
    "ok"
}

#[cfg(test)]
mod tests {
    use crate::{
        pipeline::BoundedQueue,
        storage::{BlockingStorage, FailingStorage, TestStorage},
    };

    use super::*;

    use axum::{
        body::Body,
        http::{Request, StatusCode},
    };
    use tokio::task::JoinSet;
    use tower::util::ServiceExt;

    #[tokio::test]
    async fn oversized_body_is_rejected() {
        let storage = TestStorage::default();
        let storage_clone = storage.clone();
        let (producer, _, _) = BoundedQueue::new(100, storage, 3);
        let shared_state = AppState {
            producer,
            storage: storage_clone,
        };

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
        let storage = BlockingStorage::default();
        let storage_check = storage.clone();
        let (producer, mut queue, worker) = BoundedQueue::new(1, storage.clone(), 3);

        queue.spawn(worker);

        let shared_state = AppState { producer, storage };

        let app = build_router(shared_state);

        let request = axum::http::Request::builder()
            .method("POST")
            .uri("/v1/events")
            .header("content-type", "application/json")
            .body(Body::from(r#"{"event_id":"1","tenant_id":"2","event_type":"click","timestamp":1700000000,"payload":"test"}"#))
            .unwrap();

        let response_task = tokio::spawn(async move { app.oneshot(request).await.unwrap() });
        storage_check.wait_until_blocked().await;
        let response = response_task.await.unwrap();
        assert_eq!(response.status(), StatusCode::REQUEST_TIMEOUT);

        storage_check.release_first_persist();
        queue.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn concurrency_limit_is_enforced() {
        let storage = BlockingStorage::default();
        let storage_check = storage.clone();
        let (producer, mut queue, worker) = BoundedQueue::new(1, storage.clone(), 3);

        queue.spawn(worker);

        let shared_state = AppState {
            producer,
            storage: storage.clone(),
        };

        let app = build_router(shared_state);
        let app_clone = app.clone();

        let request = axum::http::Request::builder()
            .method("POST")
            .uri("/v1/events")
            .header("content-type", "application/json")
            .body(Body::from(r#"{"event_id":"1","tenant_id":"2","event_type":"click","timestamp":1700000000,"payload":"test"}"#))
            .unwrap();

        let first_handle = tokio::spawn(async move { app_clone.oneshot(request).await.unwrap() });

        storage_check.wait_until_blocked().await;
        let mut join_set = JoinSet::new();

        for _ in 0..100 {
            let req_service = app.clone();

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

        let first_response = first_handle.await.unwrap();
        match first_response.status() {
            StatusCode::REQUEST_TIMEOUT => timeout_count += 1,
            status => panic!("unexpected first response status: {status}"),
        }

        for response in responses {
            match response.status() {
                StatusCode::REQUEST_TIMEOUT => timeout_count += 1,
                StatusCode::SERVICE_UNAVAILABLE => overload_count += 1,
                status => panic!("unexpected status: {status}"),
            }
        }

        assert_eq!(timeout_count, 100);
        assert_eq!(overload_count, 1);

        storage_check.release_first_persist();

        queue.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn health_returns_success() {
        let storage = TestStorage::default();
        let storage_clone = storage.clone();
        let (producer, _, _) = BoundedQueue::new(100, storage, 3);
        let shared_state = AppState {
            producer,
            storage: storage_clone,
        };
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
        let storage = TestStorage::default();
        let storage_clone = storage.clone();
        let (producer, _, _) = BoundedQueue::new(100, storage, 3);
        let shared_state = AppState {
            producer,
            storage: storage_clone,
        };
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
        let storage = TestStorage::default();
        let storage_clone = storage.clone();
        let (producer, _, _) = BoundedQueue::new(100, storage, 3);
        let shared_state = AppState {
            producer,
            storage: storage_clone,
        };
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
        let storage = TestStorage::default();
        let storage_clone = storage.clone();
        let sink = storage.clone();
        let (producer, mut queue, worker) = BoundedQueue::new(100, storage, 3);
        let shared_state = AppState {
            producer,
            storage: storage_clone,
        };

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

        assert_eq!(sink.events.lock().await.len(), 1);
        assert_eq!(
            sink.events.lock().await[0],
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
        let storage = TestStorage::default();
        let storage_clone = storage.clone();
        let (producer, mut queue, worker) = BoundedQueue::new(100, storage, 3);
        let shared_state = AppState {
            producer,
            storage: storage_clone,
        };

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
        let storage = TestStorage::default();
        let storage_clone = storage.clone();
        let sink = storage.clone();
        let (producer, mut queue, worker) = BoundedQueue::new(100, storage, 3);
        let shared_state = AppState {
            producer,
            storage: storage_clone,
        };
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

        assert_eq!(sink.events.lock().await.len(), 0);
    }

    #[tokio::test]
    async fn existing_event_returns_200() {
        let storage = TestStorage::default();
        let _ = storage
            .persist(&[Event::new(
                "evt-123".to_string(),
                "1".to_string(),
                "1".to_string(),
                12345,
                "1".to_string(),
            )
            .unwrap()])
            .await;

        let storage_clone = storage.clone();

        let (producer, mut queue, worker) = BoundedQueue::new(100, storage, 3);
        let shared_state = AppState {
            producer,
            storage: storage_clone,
        };

        queue.spawn(worker);

        let app = build_router(shared_state);

        // 3. Construct the GET request
        let request = Request::builder()
            .uri("/v1/events/evt-123")
            .method("GET")
            .body(axum::body::Body::empty())
            .unwrap();

        // 4. Send the request to the router using `oneshot`
        let response = app.oneshot(request).await.unwrap();

        // 5. Assert the response status is 200 OK
        assert_eq!(response.status(), StatusCode::OK);

        queue.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn unknown_event_returns_404() {
        let storage = TestStorage::default();
        let _ = storage
            .persist(&[Event::new(
                "evt-123".to_string(),
                "1".to_string(),
                "1".to_string(),
                12345,
                "1".to_string(),
            )
            .unwrap()])
            .await;

        let storage_clone = storage.clone();

        let (producer, mut queue, worker) = BoundedQueue::new(100, storage, 3);
        let shared_state = AppState {
            producer,
            storage: storage_clone,
        };

        queue.spawn(worker);

        let app = build_router(shared_state);

        // 3. Construct the GET request
        let request = Request::builder()
            .uri("/v1/events/evt-345")
            .method("GET")
            .body(axum::body::Body::empty())
            .unwrap();

        // 4. Send the request to the router using `oneshot`
        let response = app.oneshot(request).await.unwrap();

        // 5. Assert the response status is 200 OK
        assert_eq!(response.status(), StatusCode::NOT_FOUND);

        queue.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn storage_error_returns_500() {
        let storage = FailingStorage::default();

        let storage_clone = storage.clone();

        let (producer, mut queue, worker) = BoundedQueue::new(100, storage, 3);
        let shared_state = AppState {
            producer,
            storage: storage_clone,
        };

        queue.spawn(worker);

        let app = build_router(shared_state);

        // 3. Construct the GET request
        let request = Request::builder()
            .uri("/v1/events/evt-failed")
            .method("GET")
            .body(axum::body::Body::empty())
            .unwrap();

        // 4. Send the request to the router using `oneshot`
        let response = app.oneshot(request).await.unwrap();

        // 5. Assert the response status is 200 OK
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);

        queue.shutdown().await.unwrap();
    }
}
