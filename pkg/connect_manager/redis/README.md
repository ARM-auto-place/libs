# redis

Создание клиента Redis (`go-redis`).

```go
func NewRedisClient(config config.RedisConfig) redis.UniversalClient
```

Поведение зависит от `config.RedisUseSentinel`:

- `true` — `redis.NewClient` (одиночный адрес).
- `false` — `redis.NewClusterClient` (кластер по `RedisAddrs`).

Использует пароль, username и количество ретраев из конфига.