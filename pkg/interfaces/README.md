# interfaces

Общие интерфейсы библиотеки.

## `Cache[T]`

```go
type Cache[T any] interface {
    Get(ctx context.Context, key string) (*T, error)
    Set(ctx context.Context, key string, value T, ttl time.Duration) error
    Wait()
    Remove(key string)
    Clear()
    Misses() uint64
    Hits() uint64
    Ratio() float64
    KeysEvicted() uint64
    CostAdded() uint64
    CostEvicted() uint64
    KeysAdded() uint64
}
```

Реализуется пакетами `pkg/cache` (`RistrettoCache[T]`, `WithFallbackCache[T]`).

## Подпакеты

- [`user-info`](user-info/README.md) — клиент к сервису пользовательской
  информации.