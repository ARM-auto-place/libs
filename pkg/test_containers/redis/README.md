# redis

Хелпер для запуска Redis-контейнера в тестах.

```go
func SetupRedis(ctx context.Context) (testcontainers.Container, *redis.Client, error)
```

Поднимает контейнер `redis:latest`, ждёт строку `Ready to accept
connections`, подключается к нему (`redis.NewClient`) и пингует. Возвращает
контейнер и клиент.

> Для локального запуска можно выставить `LOCAL_RUN=true` — тогда адрес
> хоста будет `localhost`.