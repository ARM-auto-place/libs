# worker

Общие воркер-примитивы для сервисов монетизации.

## Компоненты

### `reader-worker.go` — `TickerWorker` (батчевая обработка, рекомендуемый)

`NewReaderWorker[Req, Res]` возвращает `TickerWorker`. На каждом тике он читает
батч элементов (`reader.Read`), обрабатывает каждый элемент и пишет результат.

- Если передан `getTransaction`, весь батч выполняется внутри **одной
  постгрес-транзакции**: элементы читаются (например, `FOR UPDATE SKIP LOCKED`),
  обрабатываются и записываются, после чего транзакция коммитится один раз.
  При ошибке транзакция откатывается.
- Параллелизм контролируется `WorkerCount`: у каждого воркера своя
  транзакция и свой батч. **Внутри одной транзакции батч обрабатывается
  последовательно** — соединение pgx не потокобезопасно, иначе падает с
  `Watch already in progress`.
- Без транзакции батч обрабатывается конкурентно (по одной горутине на
  элемент).

Релевантные типы: `Reader`, `Processor`, `Writer` (см. `interfaces.go`),
`ReaderWorkerParams`.

#### Конструктор `NewReaderWorker`

```go
func NewReaderWorker[Req any, Res any](
    m *metrics.WorkerMetrics,
    params ReaderWorkerParams,
    getTransaction func(ctx context.Context, isMaster bool) (pgx.Tx, error),
) TickerWorker[Req, Res]
```

| Параметр | Что делает |
|---|---|
| `m` | Метрики воркера (`metrics.WorkerMetrics`). Если `nil` — используется no-op реализация, метрики не собираются. |
| `params` | Настройки воркера (см. таблицу `ReaderWorkerParams` ниже). |
| `getTransaction` | Фабрика транзакции БД. Если `nil` — воркер работает без транзакции и обрабатывает батч конкурентно. Если передан — каждый батч выполняется в одной транзакции, элементы обрабатываются последовательно. |

##### Поля `ReaderWorkerParams`

| Поле | Что делает |
|---|---|
| `Logger` | Логгер воркера (`*zerolog.Logger`). |
| `Ticker` | Тикер, по которому запускается обработка батчей (`*time.Ticker`). |
| `BatchSize` | Максимальное количество элементов, читаемых за один тик. |
| `WorkerType` | Имя воркера — используется как имя метрики (`name`-label). |
| `WorkerCount` | Количество параллельных воркеров. Каждый воркер получает свою транзакцию (если `getTransaction` передан) и свой батч. По умолчанию `1`. |
| `RunImmediately` | Запускать ли первую обработку сразу (без ожидания первого тика). |
| `ContextTimeout` | Общий таймаут на один тик. |
| `ItemProcessTimeout` | Таймаут на обработку одного элемента. Если `0` — берётся значение `ContextTimeout`. |

### `poll_worker/*` — батчевый poll-воркер на franz-go

Батчевое чтение из Kafka (franz-go) с опциональной транзакцией pgx на каждое
сообщение. См. [`poll_worker/README.md`](poll_worker/README.md).

### `poll-worker.go` — LEGACY poll-воркер (confluent-kafka) — не использовать в новом коде

Оставлен только для обратной совместимости. Он тянет **одно сообщение** за тик
через confluent-kafka, не использует транзакции БД и заменён на
`reader-worker.go` или саб-пакет `poll_worker/`.

### Остальные файлы

- `interfaces.go` — базовые интерфейсы (`Reader`, `Processor`, `Writer`,
  `TickerWorker`, `Poller`, `PollWorker`, `ReaderWorkerParams`, `Message`).
- `kafka-poller.go` — Kafka poller (confluent-kafka) для legacy-воркера.
- `poll-worker-dl.go` — хелперы dead-letter для legacy-воркера.
- `nil-writer.go` — no-op `Writer`.
- `worker_test.go` — ручные smoke-тесты.