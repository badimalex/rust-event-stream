# Benchmark

Environment: Mac mini, Intel i7-4578U, 8 GB RAM, macOS 12.7.6, PostgreSQL 16.15 local, release build, k6 on the same machine.

| Concurrency | Batch size | Successful events/s | p95 | Errors |
| ---: | ---: | ---: | ---: | ---: |
| 10 | 50 | 115 | 219 ms | 0% |
| 50 | 50 | 693 | 225 ms | 0% |
| 100 | 50 | 374 | 750 ms | 0% |
| 100 | 100 | 1,033 | 261 ms | 0% |
| 200 | 50 | 70 | 2.62 s | 94.5% |

Saturation first appeared at 100 VUs with `batch_size=50`: throughput dropped and queue depth increased. At 200 VUs the queue reached capacity and overload protection rejected excess traffic, mostly with `503`.

Changing only `batch_size` from 50 to 100 increased throughput at 100 VUs from 374 to 1,033 persisted events/s.

During PostgreSQL failure `/ready` returned `503`; after PostgreSQL recovery it returned `200` without restarting the application.

Likely bottleneck: the single-worker persistence path with too-small batches under higher concurrency.

Limitation: k6, PostgreSQL and the application ran on the same machine.