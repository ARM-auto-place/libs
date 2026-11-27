# postgresql

Хелперы для запуска PostgreSQL-контейнера в тестах.

```go
func NewTestConfig() config.TestPostgresConfig
func SetupPostgres(ctx context.Context, config *config.TestPostgresConfig) (testcontainers.Container, *pgxpool.Pool, *pgxpool.Config, error)
```

| Функция | Что делает |
|---|---|
| `NewTestConfig()` | Возвращает дефолтный тестовый конфиг (`docker:5432`, `postgres`, `limits`). |
| `SetupPostgres(ctx, config)` | Поднимает контейнер `postgres:14.2`, ждёт готовности, подключается и возвращает контейнер, пул и конфиг пула. |

После запуска `config.Host`/`config.Port` обновляются на реальные
(mapped port) хост/порт контейнера.