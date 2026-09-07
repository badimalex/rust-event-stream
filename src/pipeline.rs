use tokio::sync::mpsc::{Receiver, Sender, channel};
use tokio::sync::oneshot;
use tokio::task::{JoinError, JoinHandle};
use tokio_util::sync::CancellationToken;

use crate::domain::Event;
use crate::storage::{Storage, StorageError};

type ReplyTx = tokio::sync::oneshot::Sender<Result<(), StorageError>>;

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
    Storage(StorageError),
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
    pub fn new<S>(buffer_size: usize, storage: S) -> (EventProducer, Self, Worker<S>)
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
}

impl<S> Worker<S>
where
    S: Storage,
{
    pub async fn run(mut self) {
        loop {
            tokio::select! {
                _ = self.cancel_token.cancelled() => {
                    break;
                }

                Some(request) = self.rx.recv() => {
                    let result = self.storage.persist(&request.event).await;

                    if let Err(error) = &result {
                        eprintln!("failed to persist event: {error}");
                    }

                    let _ = request.reply_to.send(result);
                }

                else => {
                    break;
                }
            }
        }

        self.rx.close();

        while let Some(request) = self.rx.recv().await {
            let result = self.storage.persist(&request.event).await;

            if let Err(error) = &result {
                eprintln!("failed to persist event: {error}");
            }

            let _ = request.reply_to.send(result);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::storage::{BlockingStorage, FailingStorage, TestStorage};

    use super::*;

    #[tokio::test]
    async fn worker_sends_events_to_sink() {
        let storage = TestStorage::default();
        let (producer, mut queue, worker) = BoundedQueue::new(2, storage);

        let sink = worker.storage.clone();

        queue.spawn(worker);

        producer
            .send_event(
                Event::new(
                    "1".to_string(),
                    "1".to_string(),
                    "1".to_string(),
                    12345,
                    "1".to_string(),
                )
                .unwrap(),
            )
            .await
            .unwrap();
        producer
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
            .await
            .unwrap();
        producer
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
            .await
            .unwrap();
        producer
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
            .await
            .unwrap();

        queue.shutdown().await.unwrap();

        assert_eq!(sink.events.lock().await.len(), 4);
        assert_eq!(
            sink.events.lock().await[0],
            Event::new(
                "1".to_string(),
                "1".to_string(),
                "1".to_string(),
                12345,
                "1".to_string()
            )
            .unwrap()
        );
        assert_eq!(
            sink.events.lock().await[1],
            Event::new(
                "2".to_string(),
                "1".to_string(),
                "1".to_string(),
                12345,
                "1".to_string()
            )
            .unwrap()
        );
        assert_eq!(
            sink.events.lock().await[2],
            Event::new(
                "3".to_string(),
                "1".to_string(),
                "1".to_string(),
                12345,
                "1".to_string()
            )
            .unwrap()
        );
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
    async fn worker_completion_is_observable() {
        let storage = TestStorage::default();
        let (producer, mut queue, worker) = BoundedQueue::new(2, storage);

        let sink = worker.storage.clone();

        queue.spawn(worker);
        let _ = producer
            .send_event(
                Event::new(
                    "1".to_string(),
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
        assert_eq!(
            sink.events.lock().await[0],
            Event::new(
                "1".to_string(),
                "1".to_string(),
                "1".to_string(),
                12345,
                "1".to_string()
            )
            .unwrap()
        );
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
    async fn send_waits_when_queue_is_full() {
        let storage = BlockingStorage::default();
        let storage_check = storage.clone();

        // Создаем очередь емкостью 1, чтобы второе событие вызвало блокировку
        let (producer, mut queue, worker) = BoundedQueue::new(1, storage);
        let producer_a = producer.clone();
        let producer_b = producer.clone();
        let producer_c = producer.clone();

        queue.spawn(worker);

        let send_a = tokio::spawn(async move {
            producer_a
                .send_event(
                    Event::new(
                        "1".to_string(),
                        "1".to_string(),
                        "1".to_string(),
                        12345,
                        "1".to_string(),
                    )
                    .unwrap(),
                )
                .await
        });
        storage_check.wait_until_blocked().await;

        assert!(
            !send_a.is_finished(),
            "first send must wait for storage acknowledgement"
        );

        let send_b = tokio::spawn(async move {
            producer_b
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
                .await
        });

        tokio::time::timeout(Duration::from_secs(1), async {
            while producer.tx.capacity() != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("queue did not become full");

        let mut send_c = tokio::spawn(async move {
            producer_c
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
                .await
        });

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
        assert_eq!(
            storage_check.events.lock().await[0],
            Event::new(
                "1".to_string(),
                "1".to_string(),
                "1".to_string(),
                12345,
                "1".to_string()
            )
            .unwrap()
        );
        assert_eq!(
            storage_check.events.lock().await[1],
            Event::new(
                "2".to_string(),
                "1".to_string(),
                "1".to_string(),
                12345,
                "1".to_string()
            )
            .unwrap()
        );
    }

    #[tokio::test]
    async fn shutdown_drains_accepted_events() {
        let storage = TestStorage::default();
        let (producer, mut queue, worker) = BoundedQueue::new(50, storage);

        let sink = worker.storage.clone();

        queue.spawn(worker);

        let len = 100;
        for _ in 1..len {
            producer
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
                .await
                .unwrap();
        }
        queue.shutdown().await.unwrap();

        assert_eq!(sink.events.lock().await.len(), len as usize - 1);
    }

    #[tokio::test]
    async fn new_work_is_not_accepted_after_shutdown() {
        let storage = TestStorage::default();
        let (producer, mut queue, worker) = BoundedQueue::new(50, storage);

        queue.spawn(worker);

        let res1 = producer
            .send_event(
                Event::new(
                    "1".to_string(),
                    "1".to_string(),
                    "1".to_string(),
                    12345,
                    "1".to_string(),
                )
                .unwrap(),
            )
            .await;
        assert!(res1.is_ok(),);

        queue.shutdown().await.unwrap();

        let res2 = producer
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
        assert!(res2.is_err(),);
    }

    #[tokio::test]
    async fn repeated_shutdown_does_not_hang() {
        let storage = TestStorage::default();
        let (producer, mut queue, worker) = BoundedQueue::new(50, storage);

        queue.spawn(worker);

        let res1 = producer
            .send_event(
                Event::new(
                    "1".to_string(),
                    "1".to_string(),
                    "1".to_string(),
                    12345,
                    "1".to_string(),
                )
                .unwrap(),
            )
            .await;
        assert!(res1.is_ok(),);

        queue.shutdown().await.unwrap();

        let res = tokio::time::timeout(Duration::from_millis(250), queue.shutdown()).await;
        assert!(res.is_ok());
    }

    #[tokio::test]
    async fn shutdown_with_empty_queue() {
        let storage = TestStorage::default();
        let (_, mut queue, worker) = BoundedQueue::new(50, storage);
        let sink = worker.storage.clone();
        queue.spawn(worker);

        let shutdown_result =
            tokio::time::timeout(Duration::from_millis(250), queue.shutdown()).await;

        assert!(shutdown_result.is_ok(), "shutdown завис");
        assert!(sink.events.lock().await.is_empty());
    }

    #[tokio::test]
    async fn storage_error_does_not_panic_worker() {
        let (producer, mut queue, worker) = BoundedQueue::new(2, FailingStorage {});

        queue.spawn(worker);

        let result = producer
            .send_event(
                Event::new(
                    "1".to_string(),
                    "1".to_string(),
                    "1".to_string(),
                    12345,
                    "1".to_string(),
                )
                .unwrap(),
            )
            .await;

        assert!(matches!(result, Err(EventSendError::Storage(_))));

        let result = queue.shutdown().await;

        assert!(result.is_ok());
    }
}
