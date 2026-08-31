use tokio::sync::mpsc::error::SendError;
use tokio::sync::mpsc::{Receiver, Sender, channel};
use tokio::task::{JoinError, JoinHandle};
use tokio_util::sync::CancellationToken;

use std::sync::Arc;
use tokio::sync::Mutex;

use crate::domain::Event;

#[derive(Clone, Default)]
pub struct TemporarySink {
    pub records: Arc<Mutex<Vec<Event>>>,
}

impl TemporarySink {
    pub fn new() -> Self {
        Self {
            records: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub async fn push(&self, event: Event) {
        let mut records = self.records.lock().await;
        records.push(event);
    }
}

#[derive(Clone)]
pub struct EventProducer {
    tx: Sender<Event>,
}

impl EventProducer {
    pub async fn send_event(&self, event: Event) -> Result<(), SendError<Event>> {
        self.tx.send(event).await
    }
}

pub struct BoundedQueue {
    cancel_token: CancellationToken,
    worker_handle: Option<JoinHandle<()>>,
}

impl BoundedQueue {
    pub fn new(buffer_size: usize) -> (EventProducer, Self, Worker) {
        let (tx, rx) = channel(buffer_size);
        let cancel_token = CancellationToken::new();

        let producer = EventProducer { tx };

        let app = Self {
            worker_handle: None,
            cancel_token: cancel_token.clone(),
        };

        let worker = Worker {
            rx,
            sink: TemporarySink::new(),
            cancel_token,
        };

        (producer, app, worker)
    }

    pub fn spawn(&mut self, worker: Worker) {
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

pub struct Worker {
    rx: Receiver<Event>,
    pub sink: TemporarySink,
    cancel_token: CancellationToken,
}

impl Worker {
    pub async fn run(mut self) {
        loop {
            tokio::select! {
                _ = self.cancel_token.cancelled() => {
                    break;
                }

                Some(event) = self.rx.recv() => {
                     self.sink.push(event).await;
                }

                else => {
                    break;
                }
            }
        }

        self.rx.close();
        while let Some(event) = self.rx.recv().await {
            self.sink.push(event).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[tokio::test]
    async fn worker_sends_events_to_sink() {
        let (producer, mut queue, worker) = BoundedQueue::new(2);

        let sink = worker.sink.clone();

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

        assert_eq!(sink.records.lock().await.len(), 4);
        assert_eq!(
            sink.records.lock().await[0],
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
            sink.records.lock().await[1],
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
            sink.records.lock().await[2],
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
            sink.records.lock().await[3],
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
        let (producer, mut queue, worker) = BoundedQueue::new(2);

        let sink = worker.sink.clone();

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

        assert_eq!(sink.records.lock().await.len(), 4);
        assert_eq!(
            sink.records.lock().await[0],
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
            sink.records.lock().await[3],
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
    async fn second_send_waits_when_queue_is_full() {
        // Создаем очередь емкостью 1, чтобы второе событие вызвало блокировку
        let (producer, mut queue, worker) = BoundedQueue::new(1);
        let producer_2 = producer.clone();

        let sink = worker.sink.clone();

        let res = producer
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
        assert!(res.is_ok());

        let mut send_handle = tokio::spawn(async move {
            let _ = producer_2
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
        });

        let check_blocked = tokio::time::timeout(Duration::from_millis(50), &mut send_handle).await;
        assert!(
            check_blocked.is_err(),
            "Ошибка: send_handle завершился, хотя очередь должна быть полна!"
        );

        queue.spawn(worker);

        tokio::time::timeout(Duration::from_secs(1), send_handle)
            .await
            .expect("Send timed out")
            .expect("Send task panicked");

        queue.shutdown().await.unwrap();

        assert_eq!(sink.records.lock().await.len(), 2);
        assert_eq!(
            sink.records.lock().await[0],
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
            sink.records.lock().await[1],
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
        let (producer, mut queue, worker) = BoundedQueue::new(50);

        let sink = worker.sink.clone();

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

        assert_eq!(sink.records.lock().await.len(), len as usize - 1);
    }

    #[tokio::test]
    async fn new_work_is_not_accepted_after_shutdown() {
        let (producer, mut queue, worker) = BoundedQueue::new(50);

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
        let (producer, mut queue, worker) = BoundedQueue::new(50);

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
        let (_, mut queue, worker) = BoundedQueue::new(50);
        let sink = worker.sink.clone();
        queue.spawn(worker);

        let shutdown_result =
            tokio::time::timeout(Duration::from_millis(250), queue.shutdown()).await;

        assert!(shutdown_result.is_ok(), "shutdown завис");
        assert!(sink.records.lock().await.is_empty());
    }
}
