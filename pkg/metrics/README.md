# metrics

Метрики Prometheus для разных сценариев: HTTP API, воркеры, БД, кэш.

## HTTP API (`api_metrics.go`)

```go
func CreateMetrics(namespace, subsystem string, labels prometheus.Labels) *Metrics
func NewMetrics(serviceName string) *Metrics
```

`Metrics` содержит:

- `RequestLatency *prometheus.HistogramVec` (метрика `<ns>_<subsystem>_http_latency`).
- `HttpCollector *prometheus.CounterVec` (метрика `<ns>_<subsystem>_http_collector`).

Метрики регистрируются один раз и кэшируются по `namespace/subsystem`.

## Воркеры (`metrics_worker.go`)

```go
func NewWorkerMetrics(namespace, subsystem string) (*WorkerMetrics, error)
func WorkerMetricsOrNoop(m *WorkerMetrics) IWorkerMetrics
```

Создаёт набор метрик воркера (`processed_total`, `process_duration_seconds`,
`queue_length`, `active_workers`, `step_duration_seconds`, `errors_total`,
`retry_total`, `retry_delay_seconds`, `health`, `restart_total`, `kafka_lag`).
`WorkerMetricsOrNoop(nil)` возвращает no-op реализацию (`IWorkerMetrics`), если
метрики не нужны.

## БД (`db_metrics.go`)

```go
func NewDBMetrics(opts DBMetricsOptions) *DBMetrics
func NewDBPoolStatsCollector[T any](...)
```

`DBMetricsOptions` описывает метрики запросов (`QueryDuration`, `QueryErrors`,
`Transactions`) и состояния пула (`ConnectionsActive`, `ConnectionsIdle`,
`ConnectionsMax`). Только опции с непустым `Name` будут созданы.

## Кэш (`metrics.go`, `metrics_options.go`)

```go
func NewMetricsCollector[T any](source *cache.RistrettoCache[T], namespace, subsystem string, constLabels prometheus.Labels) (*Collector[T], error)
```

Собирает метрики кэша из статистики Ristretto. Набор метрик настраивается
опциями `WithNamespace`, `WithSubsystem`, `WithConstLabels`,
`WithHitsCounterMetric`, `WithMissesCounterMetric`, `WithHitsRatioGaugeMetric`,
`WithKeysEvictedMetric`, `WithCostAddedMetric`, `WithCostEvictedMetric`,
`WithMetric(Desc[T])`. По умолчанию добавляются hits/misses/ratio/keys_added.

Ошибка `ErrDuplicateMetricName` — если два `Desc` дают одинаковое FQName.

## Общее

`noop-worker-metrics.go` — `NoopWorkerMetrics` (заглушка `IWorkerMetrics`).