use std::time::Duration;

use axum::{
    BoxError, Json, Router,
    body::Body,
    error_handling::HandleErrorLayer,
    extract::MatchedPath,
    extract::{Path, State},
    http::{Request, StatusCode},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post},
};

use metrics::{counter, histogram};
use metrics_exporter_prometheus::PrometheusHandle;

use tokio::time::Instant;
use tracing::{Instrument, info, info_span, warn};
use uuid::Uuid;

use tokio_util::sync::CancellationToken;
use tower::ServiceBuilder;
use tower_http::limit::RequestBodyLimitLayer;

use crate::{
    domain::{DomainError, Event},
    pipeline::{EventProducer, EventSendError},
    storage::{EventOutcome, Storage},
};
use serde::{Deserialize, Serialize};

#[cfg(test)]
use std::sync::OnceLock;

#[cfg(test)]
static TEST_METRICS: OnceLock<PrometheusHandle> = OnceLock::new();

#[derive(Clone)]
pub struct AppState<S>
where
    S: Storage + Clone,
{
    pub producer: EventProducer,
    pub storage: S,
    pub shutdown: CancellationToken,
    pub metrics: PrometheusHandle,
}

#[derive(Clone)]
pub struct AuthConfig {
    pub api_key: String,
}

pub fn build_router<S>(state: AppState<S>, auth_config: AuthConfig) -> Router
where
    S: Storage + Clone + 'static,
{
    let protected_router = Router::new()
        .route("/v1/events/{id}", get(get_event::<S>))
        .route("/v1/events", post(create_event_handler));

    let api_router = Router::new()
        .merge(protected_router)
        .layer(
            ServiceBuilder::new()
                .layer(RequestBodyLimitLayer::new(1024 * 1024))
                .layer(HandleErrorLayer::new(handle_middleware_errors))
                .load_shed()
                .concurrency_limit(100)
                .timeout(Duration::from_secs(5)),
        )
        .layer(middleware::from_fn(trace_request_middleware))
        .layer(middleware::from_fn_with_state(auth_config, require_api_key));

    Router::new()
        .route("/metrics", get(metrics))
        .route("/health", get(health))
        .route("/ready", get(ready::<S>))
        .merge(api_router)
        .with_state(state)
}

pub async fn require_api_key(
    State(auth_config): State<AuthConfig>,
    headers: axum::http::HeaderMap,
    request: axum::extract::Request,
    next: Next,
) -> Result<Response, StatusCode> {
    let provided_key = headers
        .get("X-API-Key")
        .and_then(|value| value.to_str().ok());

    let key = match provided_key {
        Some(k) => k,
        None => return Err(StatusCode::UNAUTHORIZED),
    };

    if key != auth_config.api_key {
        return Err(StatusCode::UNAUTHORIZED);
    }

    let response = next.run(request).await;
    Ok(response)
}

// 3. Кастомный Middleware, выполняющий контракт
async fn trace_request_middleware(req: Request<Body>, next: Next) -> Response<Body> {
    // Шаг 1: Генерируем уникальный request_id
    let start = Instant::now();
    let request_id = Uuid::new_v4().to_string();

    // Шаг 2: Получаем HTTP метод и route (MatchedPath)
    let method = req.method().to_string();
    let route = req
        .extensions()
        .get::<MatchedPath>()
        .map(|mp| mp.as_str().to_string())
        .unwrap_or_else(|| req.uri().path().to_string());

    let span = info_span!(
        "http_request",
        request_id = %request_id,
        method = %method,
        route = %route,
    );

    let mut response = async {
        info!("request started");

        let response = next.run(req).await;

        info!(status = response.status().as_u16(), "request completed");

        let status = response.status().as_u16().to_string();

        counter!(
            "http_requests_total",
            "status" => status
        )
        .increment(1);

        response
    }
    .instrument(span)
    .await;

    // Шаг 5: Добавляем x-request-id в заголовки ответа
    if let Ok(header_value) = request_id.parse() {
        response.headers_mut().insert("x-request-id", header_value);
    }

    let elapsed = start.elapsed().as_secs_f64();
    histogram!("http_request_duration_seconds").record(elapsed);

    response
}

async fn metrics<S>(State(state): State<AppState<S>>) -> String
where
    S: Storage + Clone + 'static,
{
    state.metrics.render()
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

    tracing::error!(error = %err, "unhandled middleware error");

    (
        StatusCode::INTERNAL_SERVER_ERROR,
        "Internal server error".to_string(),
    )
}

#[derive(Debug, Deserialize)]
pub struct CreateEventRequest {
    pub event_id: String,
    pub tenant_id: String,
    pub event_type: String,
    pub timestamp: u64,
    pub payload: serde_json::Value,
}

// Ответ в случае ошибки валидации
#[derive(Serialize)]
pub struct ApiErrorResponse {
    pub error: String,
}

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
                    DomainError::TooLongField { name, max_len } => (
                        StatusCode::BAD_REQUEST,
                        format!("Поле '{}' '{}' не должно быть пустым", name, max_len),
                    ),
                };
                (status, Json(ApiErrorResponse { error: message })).into_response()
            }

            ApiError::PipelineUnavailable => {
                (StatusCode::SERVICE_UNAVAILABLE, "pipeline unavailable").into_response()
            }

            ApiError::NotFound => StatusCode::NOT_FOUND.into_response(),

            ApiError::Internal => StatusCode::INTERNAL_SERVER_ERROR.into_response(),

            ApiError::Conflict => StatusCode::CONFLICT.into_response(),
        }
    }
}

enum ApiError {
    Domain(DomainError),
    PipelineUnavailable,
    NotFound,
    Internal,
    Conflict,
}

async fn create_event_handler<S>(
    State(state): State<AppState<S>>,
    Json(payload): Json<CreateEventRequest>,
) -> Result<StatusCode, ApiError>
where
    S: Storage + Clone + 'static,
{
    let event: Event = match payload.try_into() {
        Ok(event) => event,
        Err(error) => {
            counter!("events_rejected_total").increment(1);
            return Err(ApiError::Domain(error));
        }
    };

    counter!("events_received_total").increment(1);
    return match state.producer.send_event(event).await {
        Ok(EventOutcome::Inserted) => {
            counter!("events_persisted_total").increment(1);
            Ok(StatusCode::CREATED)
        }

        Ok(EventOutcome::Duplicate) => Ok(StatusCode::OK),

        Ok(EventOutcome::Conflict) => {
            counter!("events_rejected_total").increment(1);
            Err(ApiError::Conflict)
        }

        Err(EventSendError::QueueClosed | EventSendError::WorkerDropped) => {
            counter!("events_rejected_total").increment(1);
            Err(ApiError::PipelineUnavailable)
        }

        Err(EventSendError::Storage(error)) => {
            counter!("events_rejected_total").increment(1);

            tracing::error!(
                error = %error,
                "storage error while creating event"
            );

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
            tracing::error!(error = %error, "storage error");
            Err(ApiError::Internal)
        }
    }
}

async fn health() -> &'static str {
    "ok"
}

async fn ready<S>(State(state): State<AppState<S>>) -> StatusCode
where
    S: Storage + Clone,
{
    if state.shutdown.is_cancelled() {
        warn!(reason = "app shut down", "readiness failure");
        return StatusCode::SERVICE_UNAVAILABLE;
    }

    if state.producer.is_closed() {
        warn!(reason = "producer error", "readiness failure");
        return StatusCode::SERVICE_UNAVAILABLE;
    }

    if let Err(error) = state.storage.health_check().await {
        warn!(reason = "storage error", error = %error, "readiness failure");
        return StatusCode::SERVICE_UNAVAILABLE;
    }

    StatusCode::OK
}

#[cfg(test)]
mod tests {
    use crate::{
        pipeline::BoundedQueue,
        storage::{BlockingStorage, FailingStorage, PanicStorage, TestStorage},
    };

    use super::*;

    use axum::{
        body::{Body, to_bytes},
        http::{Request, StatusCode},
    };
    use metrics_exporter_prometheus::PrometheusBuilder;
    use serde_json::json;
    use tokio::task::JoinSet;
    use tower::util::ServiceExt;

    fn test_metrics_handle() -> PrometheusHandle {
        TEST_METRICS
            .get_or_init(|| {
                PrometheusBuilder::new()
                    .install_recorder()
                    .expect("failed to install test metrics recorder")
            })
            .clone()
    }

    fn test_auth_config() -> AuthConfig {
        AuthConfig {
            api_key: "test-api-key".to_string(),
        }
    }

    #[tokio::test]
    async fn oversized_body_is_rejected() {
        let storage = TestStorage::default();
        let storage_clone = storage.clone();
        let (producer, queue, _) = BoundedQueue::new(100, storage, 3);

        let shared_state = AppState {
            metrics: test_metrics_handle(),
            producer,
            storage: storage_clone,
            shutdown: queue.cancel_token.clone(),
        };

        let app = build_router(shared_state, test_auth_config());

        let oversized_bytes = vec![0u8; 1_048_576 + 1];

        let request = axum::http::Request::builder()
            .method(axum::http::Method::POST)
            .uri("/v1/events")
            .header(axum::http::header::CONTENT_TYPE, "application/json")
            .header("X-API-Key", "test-api-key")
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

        let shared_state = AppState {
            metrics: test_metrics_handle(),
            producer,
            storage,
            shutdown: queue.cancel_token.clone(),
        };

        let app = build_router(shared_state, test_auth_config());

        let request = axum::http::Request::builder()
            .method("POST")
            .uri("/v1/events")
            .header("content-type", "application/json")
            .header("X-API-Key", "test-api-key")
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
            metrics: test_metrics_handle(),
            producer,
            storage: storage.clone(),
            shutdown: queue.cancel_token.clone(),
        };

        let app = build_router(shared_state, test_auth_config());
        let app_clone = app.clone();

        let request = axum::http::Request::builder()
            .method("POST")
            .uri("/v1/events")
            .header("content-type", "application/json")
            .header("X-API-Key", "test-api-key")
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
            .header("X-API-Key", "test-api-key")
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
        let (producer, queue, _) = BoundedQueue::new(100, storage, 3);
        let shared_state = AppState {
            metrics: test_metrics_handle(),
            producer,
            storage: storage_clone,
            shutdown: queue.cancel_token.clone(),
        };
        let app = build_router(shared_state, test_auth_config());

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
        let (producer, mut queue, worker) = BoundedQueue::new(100, storage, 3);
        let shared_state = AppState {
            metrics: test_metrics_handle(),
            producer,
            storage: storage_clone,
            shutdown: queue.cancel_token.clone(),
        };

        queue.spawn(worker);

        let app = build_router(shared_state, test_auth_config());

        let request = Request::builder()
            .method("GET")
            .uri("/ready")
            .body(Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn readiness_is_503_when_storage_is_unavailable() {
        let storage = FailingStorage::default();

        let storage_clone = storage.clone();

        let (producer, mut queue, worker) = BoundedQueue::new(100, storage, 3);
        let shared_state = AppState {
            metrics: test_metrics_handle(),
            producer,
            storage: storage_clone,
            shutdown: queue.cancel_token.clone(),
        };

        queue.spawn(worker);

        let app = build_router(shared_state, test_auth_config());
        let app_clone = app.clone();

        let request = Request::builder()
            .method("GET")
            .uri("/ready")
            .body(Body::empty())
            .unwrap();

        // 4. Send the request to the router using `oneshot`
        let response = app.oneshot(request).await.unwrap();

        // 5. Assert the response status is 200 OK
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);

        let request = Request::builder()
            .method("GET")
            .uri("/health")
            .body(Body::empty())
            .unwrap();

        let response = app_clone.oneshot(request).await.unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        queue.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn readiness_is_503_when_worker_is_panic() {
        let storage = PanicStorage::default();

        let storage_clone = storage.clone();

        let (producer, mut queue, worker) = BoundedQueue::new(100, storage, 3);
        let shared_state = AppState {
            metrics: test_metrics_handle(),
            producer,
            storage: storage_clone,
            shutdown: queue.cancel_token.clone(),
        };

        queue.spawn(worker);

        let app = build_router(shared_state, test_auth_config());
        let app_clone = app.clone();
        let app_clone2 = app.clone();

        // 4. Construct the HTTP request
        let request = Request::builder()
            .method("POST")
            .uri("/v1/events")
            .header("content-type", "application/json")
            .header("X-API-Key", "test-api-key")
            .body(Body::from(r#"{"event_id":"1","tenant_id":"2","event_type":"click","timestamp":1700000000,"payload":"test"}"#))
            .unwrap();

        // 5. Execute the request against the router
        let response = app.oneshot(request).await.unwrap();

        // 6. Assert the response status is 503
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);

        let request = Request::builder()
            .method("GET")
            .uri("/ready")
            .body(Body::empty())
            .unwrap();

        // 4. Send the request to the router using `oneshot`
        let response = app_clone.oneshot(request).await.unwrap();

        // 5. Assert the response status is 503
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);

        let request = Request::builder()
            .method("GET")
            .uri("/health")
            .body(Body::empty())
            .unwrap();

        let response = app_clone2.oneshot(request).await.unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        let res_shutdown = queue.shutdown().await.unwrap_err();
        assert!(res_shutdown.is_panic());
    }

    #[tokio::test]
    async fn readiness_is_503_when_shutdown_token_cancelled() {
        let storage = TestStorage::default();

        let storage_clone = storage.clone();

        let (producer, mut queue, worker) = BoundedQueue::new(100, storage, 3);
        let shared_state = AppState {
            metrics: test_metrics_handle(),
            producer,
            storage: storage_clone,
            shutdown: queue.cancel_token.clone(),
        };

        queue.spawn(worker);

        let app = build_router(shared_state, test_auth_config());
        let app_clone = app.clone();
        let app_clone2 = app.clone();

        let request = Request::builder()
            .method("GET")
            .uri("/ready")
            .body(Body::empty())
            .unwrap();

        // 4. Send the request to the router using `oneshot`
        let response = app.oneshot(request).await.unwrap();

        // 5. Assert the response status is 503
        assert_eq!(response.status(), StatusCode::OK);

        queue.cancel_token.cancel();

        let request = Request::builder()
            .method("GET")
            .uri("/ready")
            .body(Body::empty())
            .unwrap();

        // 4. Send the request to the router using `oneshot`
        let response = app_clone.oneshot(request).await.unwrap();

        // 5. Assert the response status is 503
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);

        let request = Request::builder()
            .method("GET")
            .uri("/health")
            .body(Body::empty())
            .unwrap();

        let response = app_clone2.oneshot(request).await.unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        queue.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn invalid_json_is_rejected() {
        let storage = TestStorage::default();
        let storage_clone = storage.clone();
        let (producer, queue, _) = BoundedQueue::new(100, storage, 3);
        let shared_state = AppState {
            metrics: test_metrics_handle(),
            producer,
            storage: storage_clone,
            shutdown: queue.cancel_token.clone(),
        };
        let app = build_router(shared_state, test_auth_config());

        let request = Request::builder()
            .method("POST")
            .uri("/v1/events")
            .header("content-type", "application/json")
            .header("X-API-Key", "test-api-key")
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
            metrics: test_metrics_handle(),
            producer,
            storage: storage_clone,
            shutdown: queue.cancel_token.clone(),
        };

        queue.spawn(worker);

        let app = build_router(shared_state, test_auth_config());

        // 4. Construct the HTTP request
        let request = Request::builder()
            .method("POST")
            .uri("/v1/events")
            .header("content-type", "application/json")
            .header("X-API-Key", "test-api-key")
            .body(Body::from(r#"{"event_id":"1","tenant_id":"2","event_type":"click","timestamp":1700000000,"payload":{"1":"1"}}"#))
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
                json!({
                    "1":"1"
                }),
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
            metrics: test_metrics_handle(),
            producer,
            storage: storage_clone,
            shutdown: queue.cancel_token.clone(),
        };

        drop(worker);

        let app = build_router(shared_state, test_auth_config());

        // 4. Construct the HTTP request
        let request = Request::builder()
            .method("POST")
            .uri("/v1/events")
            .header("content-type", "application/json")
            .header("X-API-Key", "test-api-key")
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
            metrics: test_metrics_handle(),
            producer,
            storage: storage_clone,
            shutdown: queue.cancel_token.clone(),
        };
        queue.spawn(worker);

        let app = build_router(shared_state, test_auth_config());

        let request = Request::builder()
            .method("POST")
            .uri("/v1/events")
            .header("content-type", "application/json")
            .header("X-API-Key", "test-api-key")
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
                json!({
                    "1":"1"
                }),
            )
            .unwrap()])
            .await;

        let storage_clone = storage.clone();

        let (producer, mut queue, worker) = BoundedQueue::new(100, storage, 3);
        let shared_state = AppState {
            metrics: test_metrics_handle(),
            producer,
            storage: storage_clone,
            shutdown: queue.cancel_token.clone(),
        };

        queue.spawn(worker);

        let app = build_router(shared_state, test_auth_config());

        // 3. Construct the GET request
        let request = Request::builder()
            .uri("/v1/events/evt-123")
            .method("GET")
            .header("X-API-Key", "test-api-key")
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
                json!({
                    "1":"1"
                }),
            )
            .unwrap()])
            .await;

        let storage_clone = storage.clone();

        let (producer, mut queue, worker) = BoundedQueue::new(100, storage, 3);
        let shared_state = AppState {
            metrics: test_metrics_handle(),
            producer,
            storage: storage_clone,
            shutdown: queue.cancel_token.clone(),
        };

        queue.spawn(worker);

        let app = build_router(shared_state, test_auth_config());

        // 3. Construct the GET request
        let request = Request::builder()
            .uri("/v1/events/evt-345")
            .method("GET")
            .header("X-API-Key", "test-api-key")
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
            metrics: test_metrics_handle(),
            producer,
            storage: storage_clone,
            shutdown: queue.cancel_token.clone(),
        };

        queue.spawn(worker);

        let app = build_router(shared_state, test_auth_config());

        // 3. Construct the GET request
        let request = Request::builder()
            .uri("/v1/events/evt-failed")
            .method("GET")
            .header("X-API-Key", "test-api-key")
            .body(axum::body::Body::empty())
            .unwrap();

        // 4. Send the request to the router using `oneshot`
        let response = app.oneshot(request).await.unwrap();

        // 5. Assert the response status is 200 OK
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);

        queue.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn equivalent_retry_returns_200() {
        let storage = TestStorage::default();
        let storage_clone = storage.clone();
        let sink = storage.clone();
        let (producer, mut queue, worker) = BoundedQueue::new(100, storage, 3);
        let shared_state = AppState {
            metrics: test_metrics_handle(),
            producer,
            storage: storage_clone,
            shutdown: queue.cancel_token.clone(),
        };

        queue.spawn(worker);

        let app = build_router(shared_state, test_auth_config());
        let clone_app = app.clone();

        // 4. Construct the HTTP request
        let request = Request::builder()
            .method("POST")
            .uri("/v1/events")
            .header("content-type", "application/json")
            .header("X-API-Key", "test-api-key")
            .body(Body::from(r#"{"event_id":"evt-123","tenant_id":"2","event_type":"click","timestamp":1700000000,"payload":{"1":"1"}}"#))
            .unwrap();

        // 5. Execute the request against the router
        let response = app.oneshot(request).await.unwrap();

        // 6. Assert the response status is 201 CREATED
        assert_eq!(response.status(), StatusCode::CREATED);

        // 4. Construct the HTTP request
        let request2 = Request::builder()
            .method("POST")
            .uri("/v1/events")
            .header("content-type", "application/json")
            .header("X-API-Key", "test-api-key")
            .body(Body::from(r#"{"event_id":"evt-123","tenant_id":"2","event_type":"click","timestamp":1700000000,"payload":{"1":"1"}}"#))
            .unwrap();

        // 5. Execute the request against the router
        let response2 = clone_app.oneshot(request2).await.unwrap();

        // 6. Assert the response status is 200 OK
        assert_eq!(response2.status(), StatusCode::OK);

        queue.shutdown().await.unwrap();

        assert_eq!(sink.events.lock().await.len(), 1);
        assert_eq!(
            sink.events.lock().await[0],
            Event::new(
                "evt-123".to_string(),
                "2".to_string(),
                "click".to_string(),
                1700000000,
                json!({
                    "1":"1"
                }),
            )
            .unwrap()
        );
    }

    #[tokio::test]
    async fn http_conflict_test() {
        let storage = TestStorage::default();
        let storage_clone = storage.clone();
        let sink = storage.clone();
        let (producer, mut queue, worker) = BoundedQueue::new(100, storage, 3);
        let shared_state = AppState {
            metrics: test_metrics_handle(),
            producer,
            storage: storage_clone,
            shutdown: queue.cancel_token.clone(),
        };

        queue.spawn(worker);

        let app = build_router(shared_state, test_auth_config());
        let clone_app = app.clone();

        // 4. Construct the HTTP request
        let request = Request::builder()
            .method("POST")
            .uri("/v1/events")
            .header("content-type", "application/json")
            .header("X-API-Key", "test-api-key")
            .body(Body::from(r#"{"event_id":"evt-123","tenant_id":"2","event_type":"click","timestamp":1700000000,"payload":{"1":"user-a"}}"#))
            .unwrap();

        // 5. Execute the request against the router
        let response = app.oneshot(request).await.unwrap();

        // 6. Assert the response status is 201 CREATED
        assert_eq!(response.status(), StatusCode::CREATED);

        // 4. Construct the HTTP request
        let request2 = Request::builder()
            .method("POST")
            .uri("/v1/events")
            .header("content-type", "application/json")
            .header("X-API-Key", "test-api-key")
            .body(Body::from(r#"{"event_id":"evt-123","tenant_id":"2","event_type":"click","timestamp":1700000000,"payload":"user-2"}"#))
            .unwrap();

        // 5. Execute the request against the router
        let response2 = clone_app.oneshot(request2).await.unwrap();

        // Assert the response status is 409 CONFLICT
        assert_eq!(response2.status(), StatusCode::CONFLICT);

        queue.shutdown().await.unwrap();

        assert_eq!(sink.events.lock().await.len(), 1);
        assert_eq!(
            sink.events.lock().await[0],
            Event::new(
                "evt-123".to_string(),
                "2".to_string(),
                "click".to_string(),
                1700000000,
                json!({
                    "1":"user-a"
                }),
            )
            .unwrap()
        );
    }

    #[tokio::test]
    async fn client_cancelation() {
        let storage = BlockingStorage::default();
        let storage_check = storage.clone();
        let sink = storage.clone();
        let (producer, mut queue, worker) = BoundedQueue::new(1, storage.clone(), 3);

        queue.spawn(worker);

        let shared_state = AppState {
            metrics: test_metrics_handle(),
            producer,
            storage,
            shutdown: queue.cancel_token.clone(),
        };

        let app = build_router(shared_state, test_auth_config());

        let request = axum::http::Request::builder()
            .method("POST")
            .uri("/v1/events")
            .header("content-type", "application/json")
            .header("X-API-Key", "test-api-key")
            .body(Body::from(r#"{"event_id":"evt-12341","tenant_id":"2","event_type":"click","timestamp":1700000000,"payload":{"1":"test"}}"#))
            .unwrap();

        let response_task = tokio::spawn(async move { app.oneshot(request).await.unwrap() });
        storage_check.wait_until_blocked().await;
        response_task.abort();

        let err = response_task.await.unwrap_err();
        assert!(err.is_cancelled());

        storage_check.release_first_persist();

        queue.shutdown().await.unwrap();
        assert_eq!(sink.events.lock().await.len(), 1);
        assert_eq!(
            sink.events.lock().await[0],
            Event::new(
                "evt-12341".to_string(),
                "2".to_string(),
                "click".to_string(),
                1700000000,
                json!({
                    "1":"test"
                }),
            )
            .unwrap()
        );
    }

    #[tokio::test]
    async fn shutdown_does_not_lose_accepted_batch() {
        let storage = BlockingStorage::default();
        let storage_check = storage.clone();
        let sink = storage.clone();
        let (producer, mut queue, worker) = BoundedQueue::new(1, storage.clone(), 3);

        queue.spawn(worker);

        let shared_state = AppState {
            metrics: test_metrics_handle(),
            producer,
            storage,
            shutdown: queue.cancel_token.clone(),
        };

        let app = build_router(shared_state, test_auth_config());

        let request = axum::http::Request::builder()
            .method("POST")
            .uri("/v1/events")
            .header("content-type", "application/json")
            .header("X-API-Key", "test-api-key")
            .body(Body::from(r#"{"event_id":"evt-12341","tenant_id":"2","event_type":"click","timestamp":1700000000,"payload":{"1":"test"}}"#))
            .unwrap();

        let response_task = tokio::spawn(async move { app.oneshot(request).await.unwrap() });
        storage_check.wait_until_blocked().await;

        let shutdown_task = tokio::spawn(async move { queue.shutdown().await.unwrap() });
        assert!(!shutdown_task.is_finished());
        storage_check.release_first_persist();

        let response = response_task.await.unwrap();

        assert_eq!(response.status(), StatusCode::CREATED);
        shutdown_task.await.unwrap();

        assert_eq!(sink.events.lock().await.len(), 1);
        assert_eq!(
            sink.events.lock().await[0],
            Event::new(
                "evt-12341".to_string(),
                "2".to_string(),
                "click".to_string(),
                1700000000,
                json!({
                    "1":"test"
                }),
            )
            .unwrap()
        );
    }

    #[tokio::test]
    async fn metrics_endpoint_is_available() {
        // 1. Инициализируем окружение и состояние (по вашему шаблону)
        let storage = TestStorage::default();
        let storage_clone = storage.clone();
        let (producer, mut queue, worker) = BoundedQueue::new(100, storage, 3);

        let shared_state = AppState {
            metrics: test_metrics_handle(), // Убедитесь, что этот хэндлер регистрирует метрики
            producer,
            storage: storage_clone,
            shutdown: queue.cancel_token.clone(),
        };

        queue.spawn(worker);
        let app = build_router(shared_state, test_auth_config());
        let app_clone = app.clone();

        // 4. Construct the HTTP request
        let request = Request::builder()
            .method("POST")
            .uri("/v1/events")
            .header("content-type", "application/json")
            .header("X-API-Key", "test-api-key")
            .body(Body::from(r#"{"event_id":"1","tenant_id":"2","event_type":"click","timestamp":1700000000,"payload":"test"}"#))
            .unwrap();

        // 5. Execute the request against the router
        let response1 = app_clone.oneshot(request).await.unwrap();

        // 6. Assert the response status is 200 OK
        assert_eq!(response1.status(), StatusCode::CREATED);

        // 2. Формируем GET запрос к эндпоинту /metrics
        let request = Request::builder()
            .method("GET")
            .uri("/metrics")
            .body(Body::empty())
            .unwrap();

        // 3. Отправляем запрос в приложение
        let response = app.oneshot(request).await.unwrap();

        // 4. Проверяем HTTP статус 200 OK
        assert_eq!(response.status(), StatusCode::OK);

        // 5. Читаем тело ответа (байт-буфер ограничиваем разумным лимитом, например 2MB)
        let body_bytes = to_bytes(response.into_body(), 2 * 1024 * 1024)
            .await
            .unwrap();
        let body_string = String::from_utf8(body_bytes.to_vec()).unwrap();
        println!("body_string {}", body_string);
        // 6. Проверяем наличие ожидаемой метрики в тексте ответа
        assert!(
            body_string.contains("http_requests_total"),
            "Тело ответа не содержит ожидаемых метрик. Получено:\n{}",
            body_string
        );
    }

    #[tokio::test]
    async fn request_id_is_present() {
        // 1. Инициализируем окружение и состояние (по вашему шаблону)
        let storage = TestStorage::default();
        let storage_clone = storage.clone();
        let (producer, mut queue, worker) = BoundedQueue::new(100, storage, 3);

        let shared_state = AppState {
            metrics: test_metrics_handle(), // Убедитесь, что этот хэндлер регистрирует метрики
            producer,
            storage: storage_clone,
            shutdown: queue.cancel_token.clone(),
        };

        queue.spawn(worker);
        let app = build_router(shared_state, test_auth_config());
        let app_clone = app.clone();

        // 4. Construct the HTTP request
        let request = Request::builder()
            .method("POST")
            .uri("/v1/events")
            .header("content-type", "application/json")
            .header("X-API-Key", "test-api-key")
            .body(Body::from(r#"{"event_id":"1","tenant_id":"2","event_type":"click","timestamp":1700000000,"payload":"test"}"#))
            .unwrap();

        // 5. Execute the request against the router
        let response1 = app_clone.oneshot(request).await.unwrap();

        // 6. Assert the response status is 200 OK
        assert_eq!(response1.status(), StatusCode::CREATED);

        let request_id_header = response1.headers().get("x-request-id");

        assert!(
            request_id_header.is_some(),
            "Заголовок x-request-id не вернулся"
        );
    }

    fn metric_value(body: &str, name: &str) -> f64 {
        body.lines()
            .find_map(|line| {
                let mut parts = line.split_whitespace();

                if parts.next()? == name {
                    parts.next()?.parse().ok()
                } else {
                    None
                }
            })
            .unwrap_or(0.0)
    }

    async fn get_metrics(app: Router) -> String {
        let request = Request::builder()
            .method("GET")
            .uri("/metrics")
            .body(Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        let body = to_bytes(response.into_body(), 2 * 1024 * 1024)
            .await
            .unwrap();

        String::from_utf8(body.to_vec()).unwrap()
    }

    #[tokio::test]
    async fn successful_request_updates_metrics() {
        let storage = TestStorage::default();
        let storage_clone = storage.clone();

        let (producer, mut queue, worker) = BoundedQueue::new(100, storage, 3);

        let shared_state = AppState {
            metrics: test_metrics_handle(),
            producer,
            storage: storage_clone,
            shutdown: queue.cancel_token.clone(),
        };

        queue.spawn(worker);

        let app = build_router(shared_state, test_auth_config());

        // BEFORE
        let before = get_metrics(app.clone()).await;

        let received_before = metric_value(&before, "events_received_total");

        let persisted_before = metric_value(&before, "events_persisted_total");

        // ACTION
        let request = Request::builder()
            .method("POST")
            .uri("/v1/events")
            .header("content-type", "application/json")
            .header("X-API-Key", "test-api-key")
            .body(Body::from(
                r#"{
                "event_id":"successful-metrics-test",
                "tenant_id":"2",
                "event_type":"click",
                "timestamp":1700000000,
                "payload":"test"
            }"#,
            ))
            .unwrap();

        let response = app.clone().oneshot(request).await.unwrap();

        assert_eq!(response.status(), StatusCode::CREATED);

        // AFTER
        let after = get_metrics(app.clone()).await;

        let received_after = metric_value(&after, "events_received_total");

        let persisted_after = metric_value(&after, "events_persisted_total");

        assert!(
            received_after >= received_before + 1.0,
            "events_received_total did not increase"
        );

        assert!(
            persisted_after >= persisted_before + 1.0,
            "events_persisted_total did not increase"
        );
    }

    #[tokio::test]
    async fn failed_request_updates_error_metrics() {
        let storage = FailingStorage::default();
        let storage_clone = storage.clone();

        let (producer, mut queue, worker) = BoundedQueue::new(100, storage, 3);

        let shared_state = AppState {
            metrics: test_metrics_handle(),
            producer,
            storage: storage_clone,
            shutdown: queue.cancel_token.clone(),
        };

        queue.spawn(worker);

        let app = build_router(shared_state, test_auth_config());

        // BEFORE
        let before = get_metrics(app.clone()).await;

        let received_before = metric_value(&before, "events_rejected_total");

        let persisted_before = metric_value(&before, "db_errors_total");

        // ACTION
        let request = Request::builder()
            .method("POST")
            .uri("/v1/events")
            .header("content-type", "application/json")
            .header("X-API-Key", "test-api-key")
            .body(Body::from(
                r#"{
                "event_id":"successful-metrics-test",
                "tenant_id":"2",
                "event_type":"click",
                "timestamp":1700000000,
                "payload":"test"
            }"#,
            ))
            .unwrap();

        let response = app.clone().oneshot(request).await.unwrap();

        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);

        // AFTER
        let after = get_metrics(app.clone()).await;

        let received_after = metric_value(&after, "events_rejected_total");

        let persisted_after = metric_value(&after, "db_errors_total");

        assert!(
            received_after == received_before + 1.0,
            "events_rejected_total did not increase"
        );

        assert!(
            persisted_after == persisted_before + 1.0,
            "db_errors_total did not increase"
        );
    }

    // [todo]
    #[tokio::test]
    async fn queue_depth_metric_changes() {}

    #[tokio::test]
    async fn missing_auth_is_rejected() {
        let storage = TestStorage::default();
        let (producer, queue, _) = BoundedQueue::new(100, storage.clone(), 3);
        let shared_state = AppState {
            metrics: test_metrics_handle(),
            producer,
            storage,
            shutdown: queue.cancel_token.clone(),
        };

        let app = build_router(shared_state, test_auth_config());

        let request = Request::builder()
        .method("POST")
        .uri("/v1/events")
        .header("content-type", "application/json")
        .body(Body::from(r#"{"event_id":"1","tenant_id":"2","event_type":"click","timestamp":1700000000,"payload":{"1":"1"}}"#))
        .unwrap();

        let response = app.oneshot(request).await.unwrap();

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn invalid_auth_is_rejected() {
        let storage = TestStorage::default();
        let (producer, queue, _) = BoundedQueue::new(100, storage.clone(), 3);
        let shared_state = AppState {
            metrics: test_metrics_handle(),
            producer,
            storage,
            shutdown: queue.cancel_token.clone(),
        };

        let app = build_router(shared_state, test_auth_config());

        let request = Request::builder()
        .method("POST")
        .uri("/v1/events")
        .header("content-type", "application/json")
        .header("X-API-Key", "wrong-key")  
        .body(Body::from(r#"{"event_id":"1","tenant_id":"2","event_type":"click","timestamp":1700000000,"payload":{"1":"1"}}"#))
        .unwrap();

        let response = app.oneshot(request).await.unwrap();

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn valid_auth_reaches_handler() {
        let storage = TestStorage::default();
        let storage_clone = storage.clone();
        let sink = storage.clone();
        let (producer, mut queue, worker) = BoundedQueue::new(100, storage, 3);
        let shared_state = AppState {
            metrics: test_metrics_handle(),
            producer,
            storage: storage_clone,
            shutdown: queue.cancel_token.clone(),
        };

        queue.spawn(worker);

        let app = build_router(shared_state, test_auth_config());

        let request = Request::builder()
        .method("POST")
        .uri("/v1/events")
        .header("content-type", "application/json")
        .header("X-API-Key", "test-api-key") // Валидный ключ
        .body(Body::from(r#"{"event_id":"1","tenant_id":"2","event_type":"click","timestamp":1700000000,"payload":{"1":"1"}}"#))
        .unwrap();

        let response = app.oneshot(request).await.unwrap();

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
                json!({"1":"1"}),
            )
            .unwrap()
        );
    }
}
