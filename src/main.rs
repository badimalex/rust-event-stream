mod domain;
mod http;
mod pipeline;
mod storage;

use crate::{
    http::{AppState, AuthConfig, build_router},
    pipeline::BoundedQueue,
    storage::PostgresStorage,
};

use sqlx::postgres::PgPoolOptions;
use std::time::Duration;
use tracing::info;

use metrics_exporter_prometheus::PrometheusBuilder;

const BUFFER_SIZE: usize = 100;
const BATCH_SIZE: usize = 50;

#[tokio::main]
async fn main() -> Result<(), sqlx::Error> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("rust_event_stream=info".parse().unwrap()),
        )
        .init();

    dotenvy::dotenv().ok();

    let database_url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set");
    let api_key = std::env::var("API_KEY").expect("API_KEY must be set");

    let pool = PgPoolOptions::new()
        .max_connections(50)
        .acquire_timeout(Duration::from_secs(3))
        .connect(&database_url)
        .await?;
    let storage = PostgresStorage::new(pool);
    let worker_storage = storage.clone();

    let (producer, mut queue, worker) = BoundedQueue::new(BUFFER_SIZE, storage, BATCH_SIZE);

    let shutdown_token = queue.cancel_token.clone();
    let metrics_handle = PrometheusBuilder::new().install_recorder().unwrap();

    let shared_state = AppState {
        producer,
        storage: worker_storage,
        shutdown: shutdown_token,
        metrics: metrics_handle,
    };

    queue.spawn(worker);

    let auth_config = AuthConfig { api_key };
    let app = build_router(shared_state, auth_config);
    let server_addr = "127.0.0.1:3000";
    let listener = tokio::net::TcpListener::bind(server_addr).await.unwrap();
    info!(address = %server_addr, "server startup");

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .unwrap();

    queue.shutdown().await.unwrap();

    Ok(()) // [todo] незнаю насколько оправдано ведь выше shutdown awa,t server awit и тд ошибки не обрабатываются
}

async fn shutdown_signal() {
    tokio::signal::ctrl_c()
        .await
        .expect("failed to listen for Ctrl+C");
}
