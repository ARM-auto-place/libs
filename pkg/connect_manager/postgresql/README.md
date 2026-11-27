# postgresql

Менеджер подключений к PostgreSQL (pgxpool): мастер и реплика.

```go
func New(log zerolog.Logger, cfg config.PostgresConfig) (ConnectManager, error)
```

## Интерфейс

```go
type ConnectManager interface {
    GetMasterConn() *pgxpool.Pool
    GetReplicaConn() *pgxpool.Pool
}
```

| Метод | Возвращает |
|---|---|
| `GetMasterConn()` | Пул подключений к мастеру (для записи). |
| `GetReplicaConn()` | Пул подключений к реплике (для чтения). |

`New` поднимает два пула по `config.PostgresConfig` (адрес, БД, пользователь,
пароль, `MaxConn`, время жизни соединений). Если реплика не сконфигурирована —
вернёт ошибку.