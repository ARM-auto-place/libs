# errors

Пакет `github.com/ARM-auto-place/libs/pkg/errors` предоставляет единый способ работы с ошибками для микросервисов монетизации.

Основная цель — разделить **внутреннее** (подробное) и **публичное** (безопасное) представление ошибки. Внутренние ошибки пишутся в логи и хранят детали отладки, а публичный текст возвращается клиентам API.

## Основной тип

### Error

```go
type Error struct {
    ErrorText  string
    Cause      map[string]any
    publicText string
    wrap       error
    statusCode int
}
```

- `ErrorText` — внутреннее, подробное описание ошибки.
- `Cause` — ключевые причины/контекст, который помогает при отладке.
- `publicText` — машиночитаемый публичный код ошибки (не показывает детали).
- `statusCode` — HTTP-статус, который нужно вернуть клиенту.
- `wrap` — исходная ошибка, сохраняется для `errors.Unwrap`.

## Создание ошибок
Дополнительно, для кастомных ошибок можно создать фабрику.
Каждая ошибка создается через фабрику. Глобальная фабрика по умолчанию настраивается один раз в `main`:

```go
errors.SetServiceInfo("report", "external-api")
```

Это добавляет в `publicText` префикс вида `monetization.errors.report.externalApi.someError`.

### New

Создает новую ошибку с указанным статусом:

```go
err := errors.New("failed to create report", http.StatusBadRequest)
```

### Wrap

Оборачивает ошибку, сохраняя ее текст и заполняя `Cause`:

```go
err := errors.Wrap(dbErr, "repository")
```

## Public vs Internal

- **Internal** — `ErrorText` + `Cause`. Эти данные предназначены для логов и отладки, могут содержать детали реализации.
- **Public** — `publicText` и HTTP-статус. Возвращается наружу через REST/JSON-RPC, не содержит чувствительной информации.

Для JSON-RPC ошибок используется `JsonRPCError`, который хранит кастомную ошибку в поле `Data`.

## Вспомогательные функции

- `Is(errOne, errTwo error) bool` — сравнивает ошибки по `ErrorText`.
- `HasCause(e error, cause string) bool` — проверяет, есть ли в `Cause` указанный ключ/значение.
- `ErrorDecoder` — декодирует JSON-RPC ошибку из `json.RawMessage`.

## Подпакет response

Пакет `github.com/ARM-auto-place/libs/pkg/errors/response` формирует HTTP-ответ для Fiber:

```go
response.SendResponse(ctx, log, data, err)
```

Если передана `*errors.Error`, в теле ответа используется ее публичный текст и статус-код. Внутренние детали остаются только в логах сервиса.
