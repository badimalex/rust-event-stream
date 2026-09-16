# Failure Modes

| Failure           | Behaviour                                                                                                                                                                     |
| ----------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| PostgreSQL down   | Сервер продолжает жить. POST получает `500`. `/health = 200`, `/ready = 503`. После возврата PostgreSQL restart не нужен.                                                     |
| PostgreSQL slow   | Worker ждёт `storage.persist().await`, очередь заполняется. При полной очереди новые `send().await` ждут место. HTTP может получить timeout, хотя event потом сохранится.     |
| DB pool exhausted | PostgreSQL жив, но все connections заняты. SQLx ждёт свободное соединение, после `acquire_timeout` возвращает `PoolTimedOut`. После освобождения connection restart не нужен. |
| Queue full        | Event сразу не теряется: `tx.send().await` ждёт свободное место. Если HTTP timeout случился до успешного send, event не принят и может не сохраниться.                        |
| Worker panic      | Текущий waiter получает `WorkerDropped`, новые sends — `QueueClosed`. `/health = 200`, `/ready = 503`. Panic виден через `JoinHandle`.                                        |
| Client cancelled  | Если event уже попал в queue, worker продолжает обработку. Event может сохраниться. ACK может не дойти, потому что `ack_rx` уже dropped. Worker из-за этого не падает.        |
| Graceful shutdown | Shutdown ждёт завершения worker. Уже принятый event не бросается и должен быть обработан до завершения shutdown.                                                              |

## Data loss windows

1. **До queue admission**
   Если `tx.send().await` не завершился и HTTP request отменился — event не принят и может потеряться.

2. **После queue admission**
   Event уже принят. Даже если клиент ушёл, worker продолжит обработку и event может сохраниться.

3. **До DB commit**
   Если процесс упал до commit — сохранение не гарантируется.

4. **После DB commit, до ACK**
   Event уже сохранён, но клиент этого может не знать. Retry с тем же `event_id` безопасен: будет `Duplicate`, а не вторая запись.

5. **Во время graceful shutdown**
   Уже принятый event должен завершить обработку до окончания shutdown.
