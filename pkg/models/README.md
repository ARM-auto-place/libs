# models

Общие модели данных библиотеки.

## `UserInfo`

```go
type UserInfo struct {
    UserID      string `json:"ID"`
    UserName    string `json:"FirstName"`
    UserSurname string `json:"MiddleName"`
    LastName    string `json:"LastName"`
    Phone       int64  `json:"Phone"`
    EmployeeID  uint64 `json:"EmployeeID"`
}
```

Данные о пользователе.

## `ResponseUserInfo`

Обёртка ответа сервиса:

```go
type ResponseUserInfo struct {
    ErrorCode        string   `json:"error_code"`
    ErrorDescription string   `json:"error_description"`
    Result           UserInfo `json:"result"`
}
```

Используется как модель ответа при обращении к сервису пользовательской
информации.