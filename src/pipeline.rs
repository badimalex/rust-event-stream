use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc::{Receiver, Sender, channel};
use tokio::sync::oneshot;
use tokio::task::{JoinError, JoinHandle};
use tokio_util::sync::CancellationToken;

use crate::domain::Event;
use crate::storage::{Storage, StorageError};

type ReplyTx = tokio::sync::oneshot::Sender<Result<(), Arc<StorageError>>>;

struct PersistRequest {
    event: Event,
    reply_to: ReplyTx,
}

#[derive(Clone)]
pub struct EventProducer {
    tx: Sender<PersistRequest>,
}

#[derive(Debug)]
pub enum EventSendError {
    QueueClosed,
    WorkerDropped,
    Storage(Arc<StorageError>),
}

impl EventProducer {
    pub async fn send_event(&self, event: Event) -> Result<(), EventSendError> {
        let (reply_to, ack_rx) = oneshot::channel();

        let request = PersistRequest { event, reply_to };

        self.tx
            .send(request)
            .await
            .map_err(|_| EventSendError::QueueClosed)?;

        match ack_rx.await {
            Ok(Ok(())) => Ok(()),

            Ok(Err(error)) => Err(EventSendError::Storage(error)),

            Err(_) => Err(EventSendError::WorkerDropped),
        }
    }
}

pub struct BoundedQueue {
    cancel_token: CancellationToken,
    worker_handle: Option<JoinHandle<()>>,
}

impl BoundedQueue {
    pub fn new<S>(
        buffer_size: usize,
        storage: S,
        batch_size: usize,
    ) -> (EventProducer, Self, Worker<S>)
    where
        S: Storage,
    {
        let (tx, rx) = channel(buffer_size);
        let cancel_token = CancellationToken::new();

        let producer = EventProducer { tx };

        let app = Self {
            worker_handle: None,
            cancel_token: cancel_token.clone(),
        };

        let worker = Worker {
            rx,
            storage,
            cancel_token,
            batch_size,
            flush_interval: Duration::from_millis(50),
            buffer: Vec::new(),
            flush_deadline: None,
        };

        (producer, app, worker)
    }

    pub fn spawn<S>(&mut self, worker: Worker<S>)
    where
        S: Storage + 'static,
    {
        self.worker_handle = Some(tokio::spawn(worker.run()));
    }

    pub async fn shutdown(&mut self) -> Result<(), JoinError> {
        if !self.cancel_token.is_cancelled() {
            self.cancel_token.cancel();
        }

        if let Some(handle) = self.worker_handle.take() {
            handle.await?;
        }
        Ok(())
    }
}

pub struct Worker<S>
where
    S: Storage,
{
    rx: Receiver<PersistRequest>,
    storage: S,
    cancel_token: CancellationToken,

    batch_size: usize,
    flush_interval: Duration,
    flush_deadline: Option<tokio::time::Instant>,
    buffer: Vec<PersistRequest>,
}

impl<S> Worker<S>
where
    S: Storage,
{
    pub async fn run(mut self) {
        loop {
            let deadline = self.flush_deadline;

            tokio::select! {
                _ = self.cancel_token.cancelled() => {
                    break;
                }

                Some(request) = self.rx.recv() => {
                    if self.buffer.is_empty() {
                        self.flush_deadline = Some(tokio::time::Instant::now() + self.flush_interval);
                    }

                    self.buffer.push(request);

                    if self.buffer.len() >= self.batch_size {
                        self.flush_buffer().await;
                        self.flush_deadline = None;
                    }
                }

                _ = async {
                    match deadline {
                        Some(deadline) => tokio::time::sleep_until(deadline).await,
                        None => std::future::pending::<()>().await,
                    }
                } => {
                    self.flush_buffer().await;
                    self.flush_deadline = None;
                }

                else => {
                    break;
                }
            }
        }

        self.rx.close();

        while let Some(request) = self.rx.recv().await {
            self.buffer.push(request);

            if self.buffer.len() >= self.batch_size {
                self.flush_buffer().await;
            }
        }

        self.flush_buffer().await;
    }

    async fn flush_buffer(&mut self) {
        if self.buffer.is_empty() {
            return;
        }

        let batch = std::mem::replace(&mut self.buffer, Vec::with_capacity(self.batch_size));

        let (events, reply_tos): (Vec<Event>, Vec<ReplyTx>) =
            batch.into_iter().map(|p| (p.event, p.reply_to)).unzip();

        let result = self.storage.persist(&events).await;
        match result {
            Ok(_) => {
                for reply_to in reply_tos {
                    let _ = reply_to.send(Ok(()));
                }
            }
            Err(error) => {
                let error = Arc::new(error);
                for reply_to in reply_tos {
                    let _ = reply_to.send(Err(Arc::clone(&error)));
                }
            }
        };
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::storage::{BlockingStorage, FailingStorage, TestStorage};

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

    #[tokio::test]
    async fn worker_sends_events_to_sink() {
        let storage = TestStorage::default();
        let (producer, mut queue, worker) = BoundedQueue::new(2, storage, 3);

        let sink = worker.storage.clone();

        queue.spawn(worker);

        producer.send_event(mock_event("1")).await.unwrap();
        producer.send_event(mock_event("2")).await.unwrap();
        producer.send_event(mock_event("3")).await.unwrap();
        producer.send_event(mock_event("4")).await.unwrap();

        queue.shutdown().await.unwrap();

        assert_eq!(sink.events.lock().await.len(), 4);
        assert_eq!(sink.events.lock().await[0], mock_event("1"));
        assert_eq!(sink.events.lock().await[1], mock_event("2"));
        assert_eq!(sink.events.lock().await[2], mock_event("3"));
        assert_eq!(sink.events.lock().await[3], mock_event("4"));
    }

    #[tokio::test]
    async fn worker_completion_is_observable() {
        let storage = TestStorage::default();
        let (producer, mut queue, worker) = BoundedQueue::new(2, storage, 3);

        let sink = worker.storage.clone();

        queue.spawn(worker);
        let _ = producer.send_event(mock_event("1")).await;
        let _ = producer
            .send_event(
                Event::new(
                    "2".to_string(),
                    "1".to_string(),
                    "1".to_string(),
                    12345,
                    "1".to_string(),
                )
                .unwrap(),
            )
            .await;
        let _ = producer
            .send_event(
                Event::new(
                    "3".to_string(),
                    "1".to_string(),
                    "1".to_string(),
                    12345,
                    "1".to_string(),
                )
                .unwrap(),
            )
            .await;
        let _ = producer
            .send_event(
                Event::new(
                    "4".to_string(),
                    "1".to_string(),
                    "1".to_string(),
                    12345,
                    "1".to_string(),
                )
                .unwrap(),
            )
            .await;

        queue.shutdown().await.unwrap();

        assert_eq!(sink.events.lock().await.len(), 4);
        assert_eq!(sink.events.lock().await[0], mock_event("1"));
        assert_eq!(
            sink.events.lock().await[3],
            Event::new(
                "4".to_string(),
                "1".to_string(),
                "1".to_string(),
                12345,
                "1".to_string()
            )
            .unwrap()
        );
    }

    #[tokio::test]
    async fn batch_flushes_when_interval_expires() {
        let storage = BlockingStorage::default();
        let storage_check = storage.clone();

        let (producer, mut queue, worker) = BoundedQueue::new(10, storage, 3);

        queue.spawn(worker);

        let producer_a = producer.clone();
        let task_a = tokio::spawn(async move { producer_a.send_event(mock_event("A")).await });

        tokio::task::yield_now().await;

        storage_check.wait_until_blocked().await;
        storage_check.release_first_persist();

        assert!(task_a.await.unwrap().is_ok());
        assert_eq!(storage_check.events.lock().await.len(), 1);
    }

    #[tokio::test]
    async fn batch_flushes_when_size_is_reached() {
        let storage = BlockingStorage::default();
        let storage_check = storage.clone();

        let (producer, mut queue, worker) = BoundedQueue::new(10, storage, 3);

        queue.spawn(worker);

        let producer_a = producer.clone();
        let task_a = tokio::spawn(async move { producer_a.send_event(mock_event("A")).await });

        let producer_b = producer.clone();
        let task_b = tokio::spawn(async move { producer_b.send_event(mock_event("B")).await });

        let producer_c = producer.clone();
        let task_c = tokio::spawn(async move { producer_c.send_event(mock_event("C")).await });

        storage_check.wait_until_blocked().await;

        assert!(!task_a.is_finished());
        assert!(!task_b.is_finished());
        assert!(!task_c.is_finished());

        storage_check.release_first_persist();

        assert!(task_a.await.unwrap().is_ok());
        assert!(task_b.await.unwrap().is_ok());
        assert!(task_c.await.unwrap().is_ok());

        assert_eq!(storage_check.events.lock().await.len(), 3);
    }

    #[tokio::test]
    async fn send_waits_when_queue_is_full() {
        let storage = BlockingStorage::default();
        let storage_check = storage.clone();

        // Создаем очередь емкостью 1, чтобы второе событие вызвало блокировку
        let (producer, mut queue, worker) = BoundedQueue::new(1, storage, 3);
        let producer_a = producer.clone();
        let producer_b = producer.clone();
        let producer_c = producer.clone();

        queue.spawn(worker);

        let send_a = tokio::spawn(async move { producer_a.send_event(mock_event("1")).await });
        storage_check.wait_until_blocked().await;

        assert!(
            !send_a.is_finished(),
            "first send must wait for storage acknowledgement"
        );

        let send_b = tokio::spawn(async move { producer_b.send_event(mock_event("2")).await });

        tokio::time::timeout(Duration::from_secs(1), async {
            while producer.tx.capacity() != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("queue did not become full");

        let mut send_c = tokio::spawn(async move { producer_c.send_event(mock_event("3")).await });

        let blocked = tokio::time::timeout(Duration::from_millis(50), &mut send_c).await;

        assert!(
            blocked.is_err(),
            "send must wait while bounded queue is full"
        );

        storage_check.release_first_persist();
        assert!(send_a.await.unwrap().is_ok());
        assert!(send_b.await.unwrap().is_ok());
        assert!(send_c.await.unwrap().is_ok());

        queue.shutdown().await.unwrap();

        assert_eq!(storage_check.events.lock().await.len(), 3);
        assert_eq!(storage_check.events.lock().await[0], mock_event("1"));
        assert_eq!(storage_check.events.lock().await[1], mock_event("2"));
    }

    #[tokio::test]
    async fn shutdown_drains_accepted_events() {
        let storage = TestStorage::default();
        let (producer, mut queue, worker) = BoundedQueue::new(50, storage, 3);

        let sink = worker.storage.clone();

        queue.spawn(worker);

        let len = 100;
        for _ in 1..len {
            producer.send_event(mock_event("2")).await.unwrap();
        }
        queue.shutdown().await.unwrap();

        assert_eq!(sink.events.lock().await.len(), len as usize - 1);
    }

    #[tokio::test]
    async fn new_work_is_not_accepted_after_shutdown() {
        let storage = TestStorage::default();
        let (producer, mut queue, worker) = BoundedQueue::new(50, storage, 3);

        queue.spawn(worker);

        let res1 = producer.send_event(mock_event("1")).await;
        assert!(res1.is_ok(),);

        queue.shutdown().await.unwrap();

        let res2 = producer.send_event(mock_event("2")).await;
        assert!(res2.is_err(),);
    }

    #[tokio::test]
    async fn repeated_shutdown_does_not_hang() {
        let storage = TestStorage::default();
        let (producer, mut queue, worker) = BoundedQueue::new(50, storage, 3);

        queue.spawn(worker);

        let res1 = producer.send_event(mock_event("1")).await;
        assert!(res1.is_ok(),);

        queue.shutdown().await.unwrap();

        let res = tokio::time::timeout(Duration::from_millis(250), queue.shutdown()).await;
        assert!(res.is_ok());
    }

    #[tokio::test]
    async fn shutdown_with_empty_queue() {
        let storage = TestStorage::default();
        let (_, mut queue, worker) = BoundedQueue::new(50, storage, 3);
        let sink = worker.storage.clone();
        queue.spawn(worker);

        let shutdown_result =
            tokio::time::timeout(Duration::from_millis(250), queue.shutdown()).await;

        assert!(shutdown_result.is_ok(), "shutdown завис");
        assert!(sink.events.lock().await.is_empty());
    }

    #[tokio::test]
    async fn batch_failure_returns_error_to_callers() {
        let (producer, mut queue, worker) = BoundedQueue::new(2, FailingStorage {}, 3);

        queue.spawn(worker);

        let producer_a = producer.clone();
        let task_a = tokio::spawn(async move { producer_a.send_event(mock_event("A")).await });

        let producer_b = producer.clone();
        let task_b = tokio::spawn(async move { producer_b.send_event(mock_event("B")).await });

        let producer_c = producer.clone();
        let task_c = tokio::spawn(async move { producer_c.send_event(mock_event("C")).await });

        let result_a = task_a.await.unwrap();
        let result_b = task_b.await.unwrap();
        let result_c = task_c.await.unwrap();

        assert!(matches!(result_a, Err(EventSendError::Storage(_))));
        assert!(matches!(result_b, Err(EventSendError::Storage(_))));
        assert!(matches!(result_c, Err(EventSendError::Storage(_))));

        let result = queue.shutdown().await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn shutdown_flushes_partial_batch() {
        let storage = BlockingStorage::default();
        let storage_check = storage.clone();

        let (producer, mut queue, worker) = BoundedQueue::new(10, storage, 3);

        queue.spawn(worker);

        let producer_a = producer.clone();
        let task_a = tokio::spawn(async move { producer_a.send_event(mock_event("A")).await });

        let producer_b = producer.clone();
        let task_b = tokio::spawn(async move { producer_b.send_event(mock_event("B")).await });

        tokio::task::yield_now().await;

        let shutdown_task = tokio::spawn(async move { queue.shutdown().await });

        storage_check.wait_until_blocked().await;
        storage_check.release_first_persist();

        let shutdown_res = shutdown_task.await.unwrap();
        let result_a = task_a.await.unwrap();
        let result_b = task_b.await.unwrap();

        assert!(shutdown_res.is_ok());

        assert!(result_a.is_ok());
        assert!(result_b.is_ok());

        assert_eq!(storage_check.events.lock().await.len(), 2);
        assert_eq!(storage_check.events.lock().await[0], mock_event("A"));
        assert_eq!(storage_check.events.lock().await[1], mock_event("B"));
    }

    #[tokio::test]
    async fn storage_error_does_not_panic_worker() {
        let (producer, mut queue, worker) = BoundedQueue::new(2, FailingStorage {}, 3);

        queue.spawn(worker);

        let result = producer.send_event(mock_event("1")).await;

        assert!(matches!(result, Err(EventSendError::Storage(_))));

        let result = queue.shutdown().await;

        assert!(result.is_ok());
    }
}
