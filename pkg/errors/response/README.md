# response

Утилиты для отправки HTTP-ответов в воркерах/сервисах на Fiber.

## `RestResponse`

Структура ответа:

```go
type RestResponse struct {
    Data             any            `json:"data"`
    Error            bool           `json:"error"`
    ErrorText        string         `json:"errorText"`
    AdditionalErrors map[string]any `json:"additionalErrors"`
}
```

## Функции

| Функция | Что делает |
|---|---|
| `SendResponse(ctx *fiber.Ctx, log zerolog.Logger, data any, respError error)` | Сериализует ответ в JSON и отправляет. При ошибке: HTTP 500 (или статус из `customErr.GetStatusCode()`), `ErrorText` = публичный текст либо `DefaultInternalErrorText`. |
| `HandelResponse(ctx, method, path, methodName, serviceName, fields, call)` | Обёртка над обработчиком: выполняет `call()`, замеряет время (`took`), пишет лог и метрики (`http_latency`, `http_collector`), отправляет ответ через `SendResponse`. Обязателен `defer`-замер метрик. |

Тип ошибки `apierrors.Error` (из `pkg/errors`) поддерживается через
`errors.As`: учитываются публичный текст и HTTP-статус.