use sqlx::{PgPool, Row};

use crate::domain::Event;

#[derive(Debug)]
pub enum StorageError {
    Database(sqlx::Error),
    ContractViolation(String),

    #[cfg(test)]
    Unavailable(String),
}

#[derive(Debug, PartialEq)]
pub enum EventOutcome {
    Inserted,
    Duplicate,
    Conflict,
}

impl std::fmt::Display for StorageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StorageError::Database(e) => write!(f, "Database error: {e}"),

            #[cfg(test)]
            StorageError::Unavailable(msg) => write!(f, "Storage Unavailable: {msg}"),
            StorageError::ContractViolation(msg) => write!(f, "Contract Violation: {msg}"),
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
        events: &'a [Event],
    ) -> impl Future<Output = Result<Vec<EventOutcome>, StorageError>> + Send + 'a;

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
    async fn persist(&self, events: &[Event]) -> Result<Vec<EventOutcome>, StorageError> {
        if events.is_empty() {
            return Ok(vec![]);
        }

        let mut seen = HashSet::with_capacity(events.len());

        let candidates: Vec<&Event> = events
            .iter()
            .filter(|event| seen.insert(event.event_id.as_str()))
            .collect();

        let mut event_ids = Vec::with_capacity(candidates.len());
        let mut tenant_ids = Vec::with_capacity(candidates.len());
        let mut event_types = Vec::with_capacity(candidates.len());
        let mut event_timestamps = Vec::with_capacity(candidates.len());
        let mut payloads = Vec::with_capacity(candidates.len());

        for event in &candidates {
            event_ids.push(&event.event_id);
            tenant_ids.push(&event.tenant_id);
            event_types.push(&event.event_type);
            event_timestamps.push(event.event_timestamp as i64);
            payloads.push(&event.payload);
        }

        let mut tx = self.pool.begin().await?;

        let inserted_ids: Vec<String> = sqlx::query_scalar(
            r#"
                INSERT INTO events (event_id, tenant_id, event_type, event_timestamp, payload)
                SELECT * FROM UNNEST($1, $2, $3, $4, $5::jsonb[])
                ON CONFLICT (event_id) DO NOTHING
                RETURNING event_id
                "#,
        )
        .bind(&event_ids)
        .bind(&tenant_ids)
        .bind(&event_types)
        .bind(&event_timestamps)
        .bind(&payloads)
        .fetch_all(&mut *tx)
        .await?;
        let inserted_ids: HashSet<String> = inserted_ids.into_iter().collect();

        let not_inserted_ids: Vec<&String> = event_ids
            .iter()
            .copied()
            .filter(|id| !inserted_ids.contains(id.as_str()))
            .collect();
        let rows = sqlx::query(
            r#"
                SELECT event_id, tenant_id, event_type, event_timestamp, payload
                FROM events
                WHERE event_id = ANY($1)
            "#,
        )
        .bind(&not_inserted_ids)
        .fetch_all(&mut *tx)
        .await?;

        let existing_events: Vec<Event> = rows
            .into_iter()
            .map(|r| Event {
                event_id: r.get("event_id"),
                tenant_id: r.get("tenant_id"),
                event_type: r.get("event_type"),
                event_timestamp: r.get::<i64, _>("event_timestamp") as u64,
                payload: r.get("payload"),
            })
            .collect();

        let existing_by_id: HashMap<String, Event> = existing_events
            .into_iter()
            .map(|event| (event.event_id.clone(), event))
            .collect();

        let mut outcomes = Vec::with_capacity(events.len());

        let mut first_index_by_id = HashMap::with_capacity(events.len());

        for (index, event) in events.iter().enumerate() {
            first_index_by_id
                .entry(event.event_id.as_str())
                .or_insert(index);
        }

        for (index, event) in events.iter().enumerate() {
            if inserted_ids.contains(&event.event_id) {
                let first_index = first_index_by_id[event.event_id.as_str()];
                let canonical = &events[first_index];

                if !same_event(canonical, event) {
                    outcomes.push(EventOutcome::Conflict);
                } else if index == first_index {
                    outcomes.push(EventOutcome::Inserted);
                } else {
                    outcomes.push(EventOutcome::Duplicate);
                }
            } else {
                let existing = existing_by_id
                    .get(&event.event_id)
                    .expect("existing row must exist");

                if same_event(existing, event) {
                    outcomes.push(EventOutcome::Duplicate);
                } else {
                    outcomes.push(EventOutcome::Conflict);
                }
            }
        }

        tx.commit().await?;
        Ok(outcomes)
    }

    /*async fn persist(&self, event: &Event) -> Result<(), StorageError> {
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
    }*/

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

fn same_event(a: &Event, b: &Event) -> bool {
    a.tenant_id == b.tenant_id
        && a.event_type == b.event_type
        && a.event_timestamp == b.event_timestamp
        && a.payload == b.payload
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

    fn mock_event_user(id: &str, user_id: &str) -> Event {
        Event {
            event_id: id.to_string(),
            tenant_id: "tenant-123".to_string(),
            event_type: "user.signed_up".to_string(),
            event_timestamp: 1700000000,
            payload: serde_json::json!({"user_id": user_id}),
        }
    }

    #[sqlx::test]
    async fn test_persist_batch_saves_multiple_events(pool: sqlx::PgPool) {
        let storage = PostgresStorage::new(pool);

        let empty_batch: Vec<Event> = vec![];
        let result = storage.persist(&empty_batch).await;
        assert!(
            result.is_ok(),
            "Пустой батч должен обрабатываться без ошибок"
        );

        let event_1 = mock_event("evt_batch_1");
        let event_2 = mock_event("evt_batch_2");
        let event_3 = mock_event("evt_batch_3");

        let batch = vec![event_1.clone(), event_2.clone(), event_3.clone()];

        let persist_result = storage.persist(&batch).await;
        assert!(persist_result.is_ok(), "Пакетная вставка вернула ошибку");

        let saved_event_ids: Vec<String> =
            sqlx::query!("SELECT event_id FROM events ORDER BY event_id ASC",)
                .fetch_all(&storage.pool)
                .await
                .unwrap()
                .into_iter()
                .map(|row| row.event_id)
                .collect();

        assert_eq!(saved_event_ids.len(), 3);
        assert_eq!(
            saved_event_ids,
            vec!["evt_batch_1", "evt_batch_2", "evt_batch_3"]
        );

        let loaded_event = storage.get_by_id("evt_batch_2").await.unwrap();
        assert_eq!(
            loaded_event,
            Some(event_2),
            "Данные события изменились при сохранении"
        );
    }

    #[sqlx::test]
    async fn event_is_persisted(pool: sqlx::PgPool) {
        let storage = PostgresStorage::new(pool);
        let event = mock_event("evt_001");

        let result = storage.persist(&[event]).await;
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

        storage.persist(std::slice::from_ref(&event)).await.unwrap();

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
    async fn same_event_retry_does_not_create_second_record(pool: sqlx::PgPool) {
        let storage = PostgresStorage::new(pool);
        let event = mock_event("evt_new");

        let result = storage.persist(&[event]).await;
        assert!(result.is_ok());

        let event_2 = mock_event("evt_new"); // Тот же ID

        let result = storage.persist(&[event_2]).await;

        assert!(result.is_ok());

        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM events WHERE event_id = $1")
            .bind("evt_new")
            .fetch_one(&storage.pool)
            .await
            .unwrap();
        assert_eq!(
            count, 1,
            "Scenario 2 failed: Count must still be 1 after inserting exact duplicate"
        );
    }

    #[sqlx::test]
    async fn test_persist_conflict_duplicate_returns_error_and_does_not_modify(pool: sqlx::PgPool) {
        let storage = PostgresStorage::new(pool);
        let event = mock_event_user("evt_new_1", "first");

        let result = storage.persist(&[event]).await;
        assert!(result.is_ok());

        let event_2 = mock_event_user("evt_new_1", "999");

        let result = storage.persist(&[event_2]).await.unwrap();

        assert_eq!(result, vec![EventOutcome::Conflict]);

        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM events WHERE event_id = $1")
            .bind("evt_new_1")
            .fetch_one(&storage.pool)
            .await
            .unwrap();
        assert_eq!(count, 1, "Count must remain 1 after conflicting duplicate");

        let event = sqlx::query!(
            "SELECT payload FROM events WHERE event_id = $1",
            "evt_new_1"
        )
        .fetch_one(&storage.pool)
        .await
        .unwrap();

        assert_eq!(event.payload, serde_json::json!({"user_id": "first"}));
    }

    #[sqlx::test]
    async fn duplicate_inside_batch_is_handled(pool: sqlx::PgPool) {
        let storage = PostgresStorage::new(pool);
        let event1 = mock_event("evt_A");
        let event2 = mock_event("evt_A");

        let result = storage.persist(&[event1, event2]).await;
        assert!(result.is_ok());

        assert_eq!(
            result.unwrap(),
            vec![EventOutcome::Inserted, EventOutcome::Duplicate]
        );

        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM events WHERE event_id = $1")
            .bind("evt_A")
            .fetch_one(&storage.pool)
            .await
            .unwrap();
        assert_eq!(
            count, 1,
            "Scenario 2 failed: Count must still be 1 after inserting exact duplicate"
        );
    }

    #[sqlx::test]
    async fn mixed_batch_with_conflict(pool: sqlx::PgPool) {
        let storage = PostgresStorage::new(pool);
        let event = mock_event_user("A", "OLD");

        let result = storage.persist(&[event]).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), vec![EventOutcome::Inserted]);

        let event_2 = mock_event_user("B", "NEW");
        let event_3 = mock_event_user("A", "NEW");
        let event_4 = mock_event_user("C", "NEW");

        let result = storage.persist(&[event_2, event_3, event_4]).await.unwrap();

        assert_eq!(
            result,
            vec![
                EventOutcome::Inserted,
                EventOutcome::Conflict,
                EventOutcome::Inserted
            ]
        );

        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM events")
            .fetch_one(&storage.pool)
            .await
            .unwrap();
        assert_eq!(count, 3, "Count must remain 3 after conflicting duplicate");

        let stored_a = storage.get_by_id("A").await.unwrap().unwrap();

        assert_eq!(stored_a, mock_event_user("A", "OLD"));
        assert!(storage.get_by_id("B").await.unwrap().is_some());
        assert!(storage.get_by_id("C").await.unwrap().is_some());
    }

    #[sqlx::test]
    async fn duplicate_does_not_fail_unrelated_valid_event(pool: sqlx::PgPool) {
        let storage = PostgresStorage::new(pool);
        let event1 = mock_event("evt_A");

        let result = storage.persist(&[event1]).await.unwrap();
        assert_eq!(result, vec![EventOutcome::Inserted]);

        let event2 = mock_event("evt_B");
        let event3 = mock_event("evt_A");
        let event4 = mock_event("evt_C");

        let result = storage.persist(&[event2, event3, event4]).await;
        assert!(result.is_ok());

        assert_eq!(
            result.unwrap(),
            vec![
                EventOutcome::Inserted,
                EventOutcome::Duplicate,
                EventOutcome::Inserted,
            ]
        );

        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM events")
            .fetch_one(&storage.pool)
            .await
            .unwrap();
        assert_eq!(
            count, 3,
            "Scenario 2 failed: Count must still be 3 after inserting exact duplicate"
        );
    }

    #[sqlx::test]
    async fn duplicate_and_conflict_inside_same_batch_are_classified(pool: sqlx::PgPool) {
        let storage = PostgresStorage::new(pool);

        let event2 = mock_event_user("evt_B", "user1");
        let event3 = mock_event_user("evt_B", "user1");
        let event4 = mock_event_user("evt_B", "user2");

        let result = storage.persist(&[event2, event3, event4]).await;
        assert!(result.is_ok());

        assert_eq!(
            result.unwrap(),
            vec![
                EventOutcome::Inserted,
                EventOutcome::Duplicate,
                EventOutcome::Conflict,
            ]
        );

        let stored = sqlx::query!("SELECT payload FROM events WHERE event_id = $1", "evt_B")
            .fetch_one(&storage.pool)
            .await
            .unwrap();

        assert_eq!(stored.payload, serde_json::json!({"user_id": "user1"}));
    }

    #[sqlx::test]
    async fn concurrent_duplicate_requests_create_one_logical_record(pool: sqlx::PgPool) {
        let storage = PostgresStorage::new(pool);

        let mut tasks = Vec::new();

        for _ in 0..10 {
            let clone_storage = storage.clone();
            tasks.push(tokio::spawn(async move {
                clone_storage.persist(&[mock_event("evt_concurrent")]).await
            }));
        }

        let mut inserted_count = 0;
        let mut duplicate_count = 0;
        let mut conflict_count = 0;

        for task in tasks {
            let result = task.await.unwrap().unwrap();
            assert_eq!(result.len(), 1);

            match &result[0] {
                EventOutcome::Inserted => inserted_count += 1,
                EventOutcome::Duplicate => duplicate_count += 1,
                EventOutcome::Conflict => conflict_count += 1,
            }
        }
        assert_eq!(inserted_count, 1);
        assert_eq!(duplicate_count, 9);
        assert_eq!(conflict_count, 0);

        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM events")
            .fetch_one(&storage.pool)
            .await
            .unwrap();
        assert_eq!(
            count, 1,
            "Scenario 2 failed: Count must still be 1 after inserting exact duplicate"
        );
    }
}

use std::collections::{HashMap, HashSet};
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
    async fn persist(&self, events: &[Event]) -> Result<Vec<EventOutcome>, StorageError> {
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

        self.events.lock().await.extend_from_slice(events);
        let outcomes = events.iter().map(|_| EventOutcome::Inserted).collect();

        Ok(outcomes)
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
    async fn persist(&self, _event: &[Event]) -> Result<Vec<EventOutcome>, StorageError> {
        Err(StorageError::Unavailable("forced test failure".to_string()))
    }

    async fn get_by_id(&self, _event_id: &str) -> Result<Option<Event>, StorageError> {
        Err(StorageError::Unavailable("forced test failure".to_string()))
    }
}

#[cfg(test)]
impl Storage for TestStorage {
    async fn persist(&self, events: &[Event]) -> Result<Vec<EventOutcome>, StorageError> {
        let mut outcomes = Vec::with_capacity(events.len());
        let mut stored = self.events.lock().await;
        for event in events {
            match stored.iter().find(|saved| saved.event_id == event.event_id) {
                None => {
                    stored.push(event.clone());
                    outcomes.push(EventOutcome::Inserted);
                }
                Some(saved) if saved == event => {
                    outcomes.push(EventOutcome::Duplicate);
                }
                Some(_) => {
                    outcomes.push(EventOutcome::Conflict);
                }
            }
        }

        Ok(outcomes)
    }

    async fn get_by_id(&self, event_id: &str) -> Result<Option<Event>, StorageError> {
        let events = self.events.lock().await;

        Ok(events
            .iter()
            .find(|event| event.event_id == event_id)
            .cloned())
    }
}
