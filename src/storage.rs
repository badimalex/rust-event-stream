use sqlx::{PgPool, Row};

use crate::domain::Event;

#[derive(Debug)]
pub enum StorageError {
    #[cfg(test)]
    Unavailable(String),
    Database(sqlx::Error),
}

impl std::fmt::Display for StorageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StorageError::Database(e) => write!(f, "Database error: {e}"),

            #[cfg(test)]
            StorageError::Unavailable(msg) => write!(f, "Storage Unavailable: {msg}"),
        }
    }
}

impl std::error::Error for StorageError {}

impl From<sqlx::Error> for StorageError {
    fn from(err: sqlx::Error) -> Self {
        StorageError::Database(err)
    }
}

pub trait Storage: Send + Sync {
    fn persist<'a>(
        &'a self,
        event: &'a Event,
    ) -> impl Future<Output = Result<(), StorageError>> + Send + 'a;

    fn get_by_id<'a>(
        &'a self,
        event_id: &'a str,
    ) -> impl Future<Output = Result<Option<Event>, StorageError>> + Send + 'a;
}

#[derive(Clone)]
pub struct PostgresStorage {
    pool: PgPool,
}

impl PostgresStorage {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

impl Storage for PostgresStorage {
    async fn persist(&self, event: &Event) -> Result<(), StorageError> {
        sqlx::query(
            r#"
            INSERT INTO events (event_id, tenant_id, event_type, event_timestamp, payload)
            VALUES ($1, $2, $3, $4, $5::jsonb)
            "#,
        )
        .bind(&event.event_id)
        .bind(&event.tenant_id)
        .bind(&event.event_type)
        .bind(event.event_timestamp as i64)
        .bind(&event.payload)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn get_by_id(&self, event_id: &str) -> Result<Option<Event>, StorageError> {
        let row = sqlx::query(
            r#"
            SELECT event_id, tenant_id, event_type, event_timestamp, payload 
            FROM events 
            WHERE event_id = $1
            "#,
        )
        .bind(event_id)
        .fetch_optional(&self.pool)
        .await?;

        let event = row.map(|r| Event {
            event_id: r.get("event_id"),
            tenant_id: r.get("tenant_id"),
            event_type: r.get("event_type"),
            event_timestamp: r.get::<i64, _>("event_timestamp") as u64,
            payload: r.get("payload"),
        });

        Ok(event)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn mock_event(id: &str) -> Event {
        Event {
            event_id: id.to_string(),
            tenant_id: "tenant-123".to_string(),
            event_type: "user.signed_up".to_string(),
            event_timestamp: 1700000000,
            payload: serde_json::json!({"user_id": 42}),
        }
    }

    #[sqlx::test]
    async fn event_is_persisted(pool: sqlx::PgPool) {
        let storage = PostgresStorage::new(pool);
        let event = mock_event("evt_001");

        let result = storage.persist(&event).await;
        assert!(result.is_ok());

        let event = sqlx::query!(
            "SELECT event_id FROM events WHERE tenant_id = $1",
            "tenant-123"
        )
        .fetch_one(&storage.pool)
        .await
        .unwrap();

        assert_eq!(event.event_id, "evt_001");
    }

    #[sqlx::test]
    async fn event_can_be_loaded_by_id(pool: sqlx::PgPool) {
        let storage = PostgresStorage::new(pool);
        let event = mock_event("evt_002");

        storage.persist(&event).await.unwrap();

        let loaded = storage.get_by_id("evt_002").await.unwrap();
        assert_eq!(loaded, Some(event));
    }

    #[sqlx::test]
    async fn unknown_id_returns_none(pool: sqlx::PgPool) {
        let storage = PostgresStorage::new(pool);

        let loaded = storage.get_by_id("non_existent_id").await.unwrap();
        assert!(loaded.is_none());
    }

    #[sqlx::test]
    async fn test_duplicate_event_id_is_rejected(pool: sqlx::PgPool) {
        let storage = PostgresStorage::new(pool);

        let event_1 = mock_event("event-3");
        let event_2 = mock_event("event-3"); // Тот же ID

        // Первый раз сохраняется успешно
        storage.persist(&event_1).await.unwrap();

        // Второй раз ожидаем ошибку дублирования
        let result = storage.persist(&event_2).await;

        assert!(result.is_err());
        // match result.unwrap_err() {
        //     StorageError::DuplicateKey(_) => {} // Тест пройден успешно
        //     other => panic!("Ожидалась ошибка DuplicateKey, но получена: {:?}", other),
        // }
    }
}

#[cfg(test)]
use std::sync::Arc;
#[cfg(test)]
use tokio::sync::Mutex;

#[cfg(test)]
#[derive(Clone, Default)]
pub(crate) struct TestStorage {
    pub events: Arc<Mutex<Vec<Event>>>,
}

#[cfg(test)]
#[derive(Clone, Default)]
pub(crate) struct BlockingStorage {
    pub events: Arc<Mutex<Vec<Event>>>,
    started: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Notify>,
    persist_calls: Arc<tokio::sync::Mutex<usize>>,
}

#[cfg(test)]
impl Storage for BlockingStorage {
    async fn persist(&self, event: &Event) -> Result<(), StorageError> {
        let is_first = {
            let mut calls = self.persist_calls.lock().await;
            let is_first = *calls == 0;
            *calls += 1;
            is_first
        };

        if is_first {
            self.started.notify_one();
            self.release.notified().await;
        }

        self.events.lock().await.push(event.clone());

        Ok(())
    }

    async fn get_by_id(&self, event_id: &str) -> Result<Option<Event>, StorageError> {
        let events = self.events.lock().await;

        Ok(events
            .iter()
            .find(|event| event.event_id == event_id)
            .cloned())
    }
}

#[cfg(test)]
impl BlockingStorage {
    pub(crate) async fn wait_until_blocked(&self) {
        self.started.notified().await;
    }

    pub(crate) fn release_first_persist(&self) {
        self.release.notify_one();
    }
}

#[cfg(test)]
#[derive(Clone, Default)]
pub(crate) struct FailingStorage {}

#[cfg(test)]
impl Storage for FailingStorage {
    async fn persist(&self, _event: &Event) -> Result<(), StorageError> {
        Err(StorageError::Unavailable("forced test failure".to_string()))
    }

    async fn get_by_id(&self, _event_id: &str) -> Result<Option<Event>, StorageError> {
        Err(StorageError::Unavailable("forced test failure".to_string()))
    }
}

#[cfg(test)]
impl Storage for TestStorage {
    async fn persist(&self, event: &Event) -> Result<(), StorageError> {
        self.events.lock().await.push(event.clone());

        Ok(())
    }

    async fn get_by_id(&self, event_id: &str) -> Result<Option<Event>, StorageError> {
        let events = self.events.lock().await;

        Ok(events
            .iter()
            .find(|event| event.event_id == event_id)
            .cloned())
    }
}
