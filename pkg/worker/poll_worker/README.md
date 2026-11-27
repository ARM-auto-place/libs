# poll_worker

Батчевый Kafka poll-воркер на franz-go.

`NewPollWorker[Req, Res]` возвращает `PollWorker`, который:

- забирает батч сообщений (`Poller.Poll` → `[]Message[T]`),
- для каждого сообщения вызывает `worker.Processor.Process` +
  `worker.Writer.Write`,
- опционально внутри транзакции pgx (`getTransaction`) — одна транзакция на
  сообщение, коммитится только после успешной записи результата,
- коммитит сообщение в Kafka только после успешной обработки и записи,
- отправляет упавшие сообщения в retry-топик и отдаёт потребительский lag
  через метрики.

## Типы

- `Message[T]` — полезная нагрузка + метаданные `kgo.Record`.
- `Poller[T]` — `Poll`, `Commit`, `GetLag`, `Shutdown`.
- `PollWorker` — `Start`, `Stop`.
- `Producer` — `Produce`, `ProduceAsync`, `Shutdown` (используется как цель retry-топика).

## Конструкторы

### `NewPollWorker`

```go
func NewPollWorker[Req any, Res any](
    logger *zerolog.Logger,
    tickTimeout time.Duration,
    maxTickRetries uint64,
    m *metrics.WorkerMetrics,
    name string,
    opts ...PollWorkerOption[Req, Res],
) PollWorker[Req, Res]
```

| Параметр | Что делает |
|---|---|
| `logger` | Логгер воркера. |
| `tickTimeout` | Таймаут на обработку одного сообщения. Если `0` — обработка без ограничения по времени (для топиков с бесконечными ретраями). |
| `maxTickRetries` | Максимальное число попыток обработки одного сообщения (через exponential backoff). |
| `m` | Метрики воркера. Если `nil` — no-op. |
| `name` | Имя воркера (label `name` в метриках). |
| `opts` | Дополнительные опции: `WithDedup` (дедупликация сообщений), `WithGetTransaction` (транзакция pgx), `WithRetry` (retry-топик). |

### Опции

#### `WithGetTransaction`

```go
func WithGetTransaction[Req any, Res any](
    getTransaction func(ctx context.Context, isMaster bool) (pgx.Tx, error),
) PollWorkerOption[Req, Res]
```

Задаёт фабрику транзакции pgx: на каждое сообщение открывается своя транзакция,
коммитится после успешной записи результата. Если опция не передана — воркер
работает без транзакции.

#### `WithRetry`

```go
func WithRetry[Req any, Res any](
    producer Producer,
    topic string,
) PollWorkerOption[Req, Res]
```

Задаёт продюсер Kafka и топик, в который отправляются упавшие сообщения
(retry-топик) после исчерпания попыток обработки. Если опция не передана —
упавшие сообщения не отправляются в retry.

#### `WithDedup`

```go
func WithDedup[Req any, Res any](
    dedupStore DedupStore,
    keyFunc func(ctx context.Context, msg Req) (string, error),
) PollWorkerOption[Req, Res]
```

Опция `WithDedup` включает дедупликацию сообщений Kafka:

- `dedupStore` — хранилище атомарного check-and-set (например, Redis) для пометки
  обработанных сообщений. Должно реализовывать `SetValueNX` и `DeleteValue`.
- `keyFunc` — функция, по которой из полезной нагрузки `Req` выводится ключ
  дедупликации (например, ID сообщения).

Поведение:

- перед обработкой сообщения воркер атомарно помечает ключ как обработанный
  (`SetValueNX`); если ключ уже существовал — сообщение считается дубликатом и
  пропускается;
- при успешной обработке ключ остаётся (хранится 24 часа);
- при ошибке обработки ключ удаляется, чтобы сообщение могло быть обработано
  повторно при ретрае.

`DedupStore` достаточно узкий, чтобы его можно было реализовать поверх
существующего Redis-клиента сервиса — например, интерфейс
`limits/internal/storage/redis.Cache` (методы `SetValueNX`/`DeleteValue`) уже
подходит без изменений.

### `NewKafkaPoller`

```go
func NewKafkaPoller[Req any](
    pollCtx context.Context,
    pollCtxTimeout time.Duration,
    logger *zerolog.Logger,
    kafkaCfg config.KafkaConfig,
    handleMessage func(ctx context.Context, m *kgo.Record) (*Req, error),
) (Poller[Req], error)
```

| Параметр | Что делает |
|---|---|
| `pollCtx` | Родительский контекст для чтения батча из Kafka. |
| `pollCtxTimeout` | Таймаут на `PollFetches` — ограничивает время ожидания сообщений. |
| `logger` | Логгер. |
| `kafkaCfg` | Конфигурация Kafka (`config.KafkaConfig`) — брокеры, группа, топики и т.д. |
| `handleMessage` | Функция, превращающая сырое `*kgo.Record` в полезную нагрузку `Req`. Возвращает ошибку, если сообщение нельзя обработать (тогда чтение прерывается). |

`NewKafkaPoller` поднимает `kgo.Client`, строит конфиг через
`config.MakeConsumerKafkaConfigMap` и возвращает объект, реализующий
`Poller[Req]` (`Poll` / `Commit` / `GetLag` / `Shutdown`).

### `NewKafkaProducer`

```go
func NewKafkaProducer(logger *zerolog.Logger, kafkaConfig config.KafkaConfig) (Producer, error)
```

Создаёт продюсера Kafka на franz-go (`ProduceSync` + `Close`). Используется
как `producer` в `NewPollWorker`.

### Producer: `Produce` и `ProduceAsync`

`Producer` позволяет отправлять сообщения в Kafka двумя способами:

```go
type Producer interface {
    Produce(ctx context.Context, topic string, key []byte, value []byte) error
    ProduceAsync(ctx context.Context, topic string, key []byte, value []byte) error
    Shutdown()
}
```

#### `Produce` (синхронная)

Выполняет `kgo.Client.ProduceSync`: блокирует поток до тех пор, пока сообщение
не будет доставлено в Kafka (или не произойдёт ошибка), и возвращает ошибку
непосредственно в вызывающем горутин. Подходит, когда важен порядок и нужно
гарантированно дождаться результата перед следующим шагом:

```go
if err := producer.Produce(ctx, topic, key, value); err != nil {
    // сообщение не доставлено — можно ретраить
}
```

#### `ProduceAsync` (асинхронная)

Выполняет `kgo.Client.Produce`: ставит сообщение в очередь и немедленно
возвращает `nil`, не дожидаясь доставки. Результат не блокирует вызывающего —
подходит для «fire-and-forget», когда нужно быстро отправлять много сообщений и
не ждать подтверждения каждого. Ошибки доставки не возвращаются из метода, а
логируются фоновой колбэк-функцией через логгер продюсера:

```go
if err := producer.ProduceAsync(ctx, topic, key, value); err == nil {
    // сообщение принято в очередь; доставка произойдёт асинхронно
}
```

Использование:

- сообщение кладётся в буфер и уходит в Kafka асинхронно;
- при ошибке доставки она пишется в лог (`async produce failed`) и не
  пробрасывается в вызывающую горутину — ошибку нужно обрабатывать отдельно
  (например, через собственный ретрай/мониторинг);
- при завершении работы продюсера (`Shutdown` → `kgo.Client.Close`) все
  неотправленные из очереди сообщения досылаются (франц-го флашит буфер перед
  закрытием).

`ProduceAsync` удобно использовать для высоконагруженной отправки (например,
в retry-топик), где синхронное подтверждение каждого сообщения было бы узким
местом.

### `GetBaseHandleMessageFunc[Req]`

```go
func GetBaseHandleMessageFunc[Req any](validate *validator.Validate) func(ctx context.Context, m *kgo.Record) (*Req, error)
```

Возвращает стандартную `handleMessage`: десериализует `m.Value` (JSON) в `Req`
и, если передан `validate`, проверяет структуру валидатором. Удобно передавать
в `NewKafkaPoller` как дефолтную обработку сообщения.