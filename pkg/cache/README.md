# cache

Обёртки над иными кэшами с типизацией и метриками.

## `RistrettoCache[T]`

Кэш на базе [dgraph-io/ristretto/v2](https://github.com/dgraph-io/ristretto).

```go
func NewRistrettoCache[T any](config *config.Config) (*RistrettoCache[T], error)
```

| Метод | Что делает |
|---|---|
| `Get(ctx, key) (*T, error)` | Достаёт значение; ошибка `value not found in cache`, если нет. |
| `Set(ctx, key, value, ttl) error` | Кладёт значение с TTL. |
| `Wait()` | Дожидается применения всех операций. |
| `Remove(key)` / `Clear()` | Удаляет ключ / очищает кэш. |
| `Hits()`, `Misses()`, `Ratio()` | Статистика обращений. |
| `KeysEvicted()`, `KeysAdded()`, `CostAdded()`, `CostEvicted()` | Статистика вытеснений. |

## `WithFallbackCache[T]`

Кэш с fallback на источник данных при промахе. При `Get`, если значения нет в
основном кэше, вызывается `Fallback`, а результат кладётся в кэш на 10 минут.

```go
func NewFallbackCache[T any](
    cache interfaces.Cache[T],
    fallbackFunc func(ctx context.Context, key string) (*T, error),
) *WithFallbackCache[T]
```

## `MockCache[T]`

`go:generate`-мок `interfaces.Cache[T]` (создаётся через `NewMockCache[T](ctrl)`).

## Связанное

Интерфейс кэша описан в `pkg/interfaces` (`Cache[T]`), метрики кэша — в
`pkg/metrics` (`NewMetricsCollector`).