# config

Конфигурация и утилиты: структуры конфигов, логгер, билд-инфо, построение
Kafka-опций.

## Структуры конфигов (`models.go`)

| Тип | Назначение |
|---|---|
| `Config` | Параметры кэша (`NumCounters`, `MaxCost`, `BufferItems`, `DefaultTTL`, `Metrics`). |
| `KafkaConfig` | Агрегирует consumer/producer конфиг + `HostName`/`ClientName`. |
| `ConsumerKafkaConfig` | Настройки консьюмера (адреса, группа, топик, SASL, offset). |
| `ProducerKafkaConfig` | Настройки продюсера. |
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

## Kafka (`kafka_config.go`)

```go
func MakeConsumerKafkaConfigMap(logger *zerolog.Logger, kafkaConfig KafkaConfig) []kgo.Opt
func MakeProducerKafkaConfigMap(logger *zerolog.Logger, kafkaConfig KafkaConfig) []kgo.Opt
func GetClientIDAndInstance(client, group, hostName, clientName string) (string, string)
```

Строят набор `kgo.Opt` (franz-go): брокеры, группа, offset, SASL (SCRAM), TLS,
балансировка и т.д. Плюс формируют `clientID` и `groupInstanceID` из hostname и
имя сервиса.