# config

Конфигурация и утилиты: структуры конфигов, логгер, билд-инфо

## Структуры конфигов (`models.go`)

| Тип | Назначение |
|---|---|
| `Config` | Параметры кэша (`NumCounters`, `MaxCost`, `BufferItems`, `DefaultTTL`, `Metrics`). |
| `PostgresConfig` | Параметры подключения к PostgreSQL (мастер + реплика). |
| `TestPostgresConfig` | Параметры тестового PostgreSQL-контейнера. |
| `RedisConfig` | Параметры Redis (кластер/sentinel, адреса, ретраи). |

Все поля заполняются из переменных окружения (`env:"..."`).

## Функции

| Функция | Что делает |
|---|---|
| `MustParse(cfg any)` | Парсит переменные окружения в переданную структуру; при ошибке паникует. |
| `NewLogger(level, format, serviceName string) zerolog.Logger` | Создаёт `zerolog`-логгер с уровнем, форматом (`json`/console) и полем `serviceName`. |
| `GetHostName() (string, error)` | Возвращает hostname, нормализованный для использования в имени. |
| `BuildInfo` + `Set(...)` | Хранит метаданные о сборке (serviceName, gitSHA, version, nodeName, buildStamp, buildNumber). |



Строят набор `kgo.Opt` (franz-go): брокеры, группа, offset, SASL (SCRAM), TLS,
балансировка и т.д. Плюс формируют `clientID` и `groupInstanceID` из hostname и
имя сервиса.