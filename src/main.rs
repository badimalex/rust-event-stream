mod domain;
mod http;
mod pipeline;
mod storage;

use crate::{
    http::{AppState, build_router},
    pipeline::BoundedQueue,
    storage::PostgresStorage,
};

use sqlx::postgres::PgPoolOptions;
use std::time::Duration;

const BUFFER_SIZE: usize = 100;

#[tokio::main]
async fn main() -> Result<(), sqlx::Error> {
    dotenvy::dotenv().expect("Не удалось загрузить .env файл");

    let database_url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set");

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .acquire_timeout(Duration::from_secs(3))
        .connect(&database_url)
        .await?;
    let storage = PostgresStorage::new(pool);
    let worker_storage = storage.clone();

    let (producer, mut queue, worker) = BoundedQueue::new(BUFFER_SIZE, storage);

    let shared_state = AppState {
        producer,
        storage: worker_storage,
    };

    queue.spawn(worker);

    let app = build_router(shared_state);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:3000")
        .await
        .unwrap();
    println!("Сервер запущен на http://127.0.0.1:3000");

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
