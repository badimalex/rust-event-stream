mod domain;
mod http;
mod pipeline;

use crate::{
    http::{AppState, build_router},
    pipeline::BoundedQueue,
};

#[tokio::main]
async fn main() {
    let (producer, mut queue, worker) = BoundedQueue::new(100);

    let shared_state = AppState { producer };

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
}

async fn shutdown_signal() {
    tokio::signal::ctrl_c()
        .await
        .expect("failed to listen for Ctrl+C");
}
