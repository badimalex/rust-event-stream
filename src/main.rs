use std::sync::Arc;

use tokio::sync::Mutex;
use tokio::sync::mpsc::{Receiver, Sender, channel};
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone, PartialEq)]
pub struct Event {
    pub id: u64,
}

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

    pub async fn contains(&self, event: &Event) -> bool {
        let records = self.records.lock().await;
        records.contains(event)
    }
}

pub struct BoundedQueue {
    sender: Sender<Event>,
    receiver: Mutex<Receiver<Event>>,
    pub temporary_sink: TemporarySink,
    cancel_token: CancellationToken,
}

impl BoundedQueue {
    pub fn new(buffer_size: usize) -> Self {
        let (sender, receiver) = channel(buffer_size);
        let cancel_token = CancellationToken::new();
        Self {
            sender,
            receiver: Mutex::new(receiver),
            temporary_sink: TemporarySink::new(),
            cancel_token,
        }
    }

    pub async fn send_event(&self, event: Event) -> Result<(), String> {
        self.sender
            .send(event)
            .await
            .map_err(|e| format!("Не удалось отправить, канал закрыт: {}", e))
    }

    pub async fn recv_event(&self) -> Option<Event> {
        let mut rx = self.receiver.lock().await;
        rx.recv().await
    }

    pub async fn run_worker(&self) {
        loop {
            tokio::select! {
                Some(event) = self.recv_event() => {
                    self.temporary_sink.push(event).await;
                }
                _ = self.cancel_token.cancelled() => {
                    break;
                } else => {
                    break;
                }
            }
        }
    }

    pub fn shutdown(&self) {
        self.cancel_token.cancel();
    }
}

#[tokio::main]
async fn main() {}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[tokio::test]
    async fn bounded_queue_can_send_and_receive_event() {
        let worker = BoundedQueue::new(2);
        worker.send_event(Event { id: 1 }).await.unwrap();
        worker.send_event(Event { id: 2 }).await.unwrap();

        let event = worker.recv_event().await.unwrap();
        assert_eq!(event, Event { id: 1 });

        let event = worker.recv_event().await.unwrap();
        assert_eq!(event, Event { id: 2 });
    }

    #[tokio::test]
    async fn worker_sends_events_to_sink() {
        let worker = Arc::new(BoundedQueue::new(2));

        let worker_task = worker.clone();

        let handle = tokio::spawn(async move {
            worker_task.run_worker().await;
        });

        worker.send_event(Event { id: 1 }).await.unwrap();
        worker.send_event(Event { id: 2 }).await.unwrap();
        worker.send_event(Event { id: 3 }).await.unwrap();
        worker.send_event(Event { id: 4 }).await.unwrap();

        tokio::time::sleep(Duration::from_millis(50)).await;

        assert_eq!(worker.temporary_sink.records.lock().await.len(), 4);
        assert_eq!(
            worker.temporary_sink.records.lock().await[0],
            Event { id: 1 }
        );
        assert_eq!(
            worker.temporary_sink.records.lock().await[3],
            Event { id: 4 }
        );

        handle.abort();
    }

    #[tokio::test]
    async fn worker_can_shutdown() {
        let worker = Arc::new(BoundedQueue::new(2));

        let worker_task = worker.clone();

        let handle = tokio::spawn(async move {
            worker_task.run_worker().await;
        });

        worker.send_event(Event { id: 1 }).await.unwrap();
        worker.send_event(Event { id: 2 }).await.unwrap();
        worker.send_event(Event { id: 3 }).await.unwrap();
        worker.send_event(Event { id: 4 }).await.unwrap();

        tokio::time::sleep(Duration::from_millis(50)).await;

        worker.shutdown();
        let result = handle.await;
        assert!(
            result.is_ok(),
            "Worker JoinHandle должен завершиться успешно"
        );
        assert_eq!(worker.temporary_sink.records.lock().await.len(), 4);
        assert_eq!(
            worker.temporary_sink.records.lock().await[0],
            Event { id: 1 }
        );
        assert_eq!(
            worker.temporary_sink.records.lock().await[3],
            Event { id: 4 }
        );
    }

    #[tokio::test]
    async fn second_send_waits_when_queue_is_full() {
        // Создаем очередь емкостью 1, чтобы второе событие вызвало блокировку
        let worker = std::sync::Arc::new(BoundedQueue::new(1));

        // 1. Первая отправка — должна пройти успешно
        worker.send_event(Event { id: 1 }).await.unwrap();

        // 2. Начинаем отправку Event 2 в отдельной задаче, так как она заблокируется
        let worker_clone = worker.clone();
        let send_handle = tokio::spawn(async move {
            worker_clone.send_event(Event { id: 2 }).await.unwrap();
        });

        // Даем задаче время запуститься и попытаться отправить данные
        tokio::time::sleep(Duration::from_millis(10)).await;

        // Проверяем, что задача отправки все еще не завершилась (очередь заблокирована)
        assert!(
            !send_handle.is_finished(),
            "Send should be blocked because the queue is full"
        );

        // 3. Читаем Event 1 — освобождаем место в очереди
        let event = worker.recv_event().await.unwrap();
        assert_eq!(event, Event { id: 1 });

        // 4. Ждем завершения задачи отправки Event 2 — теперь она должна успешно завершиться
        tokio::time::timeout(Duration::from_secs(1), send_handle)
            .await
            .expect("Send timed out")
            .expect("Send task panicked");

        // 5. Проверяем, что Event 2 теперь можно успешно прочитать
        let event = worker.recv_event().await.unwrap();
        assert_eq!(event, Event { id: 2 });
    }
}
