# locker

Распределённые блокировки на базе Redis (`redsync`).

```go
func NewLocker(redisCli goredislib.UniversalClient) (Locker, error)
```

## Интерфейсы

```go
type Locker interface {
    AcquireLock(ctx context.Context, key string, ttl time.Duration) (Lock, error)
}

type Lock interface {
    UnlockContext(context.Context) (bool, error)
}
```

| Метод | Что делает |
|---|---|
| `AcquireLock(ctx, key, ttl)` | Пытается захватить мьютекс по `key` с TTL. При недоступности Redis возвращает `ErrRedisUnavailable`. |
| `Lock.UnlockContext(ctx)` | Освобождает блокировку; возвращает `(true, nil)` при успехе. |

## Ошибки

- `ErrRedisUnavailable` — Redis недоступен (закрытое соединение, таймаут или
  сетевой timeout).

## `MockLocker` / `MockLock`

`go:generate`-моки для тестирования.