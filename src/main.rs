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
const BATCH_SIZE: usize = 50;

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

    let (producer, mut queue, worker) = BoundedQueue::new(BUFFER_SIZE, storage, BATCH_SIZE);

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

/*#[tokio::main]
async fn main12() -> Result<(), sqlx::Error> {
    // 1. Инициализация пула и таблицы
    // let pool = PgPool::connect("postgresql://dmitriybadichan@localhost:5432/postgres/test_db").await?;
    // dotenvy::dotenv().expect("Не удалось загрузить .env файл");

    let database_url = "postgresql://dmitriybadichan@localhost:5432/test_db";

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .acquire_timeout(Duration::from_secs(3))
        .connect(database_url)
        .await?;

    sqlx::query("DROP TABLE IF EXISTS events")
        .execute(&pool)
        .await?;
    sqlx::query(
        "CREATE TABLE events (
            event_id TEXT NOT NULL UNIQUE,
            tenant_id TEXT NOT NULL,
            event_type TEXT NOT NULL,
            event_timestamp BIGINT NOT NULL,
            payload JSONB NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )",
    )
    .execute(&pool)
    .await?;


    // Генерируем 1000 тестовых событий
    let events: Vec<Event> = (0..1000)
        .map(|i| Event {
            event_id: format!("evt_{}", i),
            tenant_id: "tenant_1".to_string(),
            event_type: "user_login".to_string(),
            event_timestamp: 1672531199 + i as u64,
            payload: Value::from("hello"),
        })
        .collect();

    // ==========================================
    // Эксперимент B: Multi-row INSERT (10 x 100)
    // ==========================================
    let batch_size = 100;
    let start_batch = Instant::now();

    for chunk in events.chunks(batch_size) {
        let mut query_builder = QueryBuilder::new(
            "INSERT INTO events (event_id, tenant_id, event_type, event_timestamp, payload) ",
        );

        query_builder.push_values(chunk, |mut b, event| {
            b.push_bind(&event.event_id)
                .push_bind(&event.tenant_id)
                .push_bind(&event.event_type)
                .push_bind(event.event_timestamp as i64)
                .push_bind(&event.payload);
        });

        let query = query_builder.build();
        query.execute(&pool).await?;
    }
    let duration_batch = start_batch.elapsed();

    // Очищаем таблицу перед вторым тестом
    sqlx::query("TRUNCATE TABLE events").execute(&pool).await?;


    // ==========================================
    // Эксперимент A: 1000 отдельных INSERT
    // ==========================================
    let start_individual = Instant::now();
    for event in &events {
        sqlx::query(
            "INSERT INTO events (event_id, tenant_id, event_type, event_timestamp, payload)
             VALUES ($1, $2, $3, $4, $5)",
        )
        .bind(&event.event_id)
        .bind(&event.tenant_id)
        .bind(&event.event_type)
        .bind(event.event_timestamp as i64) // PG BIGINT — это i64
        .bind(&event.payload)
        .execute(&pool)
        .await?;
    }
    let duration_individual = start_individual.elapsed();

    // Вывод результатов в требуемом формате
    println!(
        "CH2 individual: 1000 rows 1000 DB operations time: {:.1}ms",
        duration_individual.as_secs_f64() * 1000.0
    );
    println!(
        "batch: 1000 rows 10 DB operations × 100 rows time: {:.1}ms",
        duration_batch.as_secs_f64() * 1000.0
    );

    Ok(())
}*/

async fn shutdown_signal() {
    tokio::signal::ctrl_c()
        .await
        .expect("failed to listen for Ctrl+C");
}
