package models

import "encoding/json"

type ResponseUserInfo struct {
	ErrorCode        string   `json:"error_code"`
	ErrorDescription string   `json:"error_description"`
	Result           UserInfo `json:"result"`
}
type UserInfo struct {
	UserID      string  `json:"ID"`
	UserName    *string `json:"FirstName"`
	UserSurname *string `json:"MiddleName"`
	LastName    *string `json:"LastName"`
	Phone       *int64  `json:"Phone"`
	EmployeeID  uint64  `json:"EmployeeID"`
}

func (u *UserInfo) UnmarshalJSON(data []byte) error {
	type userInfoAlias UserInfo
	var decoded struct {
		userInfoAlias
		EmployeeID json.Number `json:"EmployeeID"`
	}
	if err := json.Unmarshal(data, &decoded); err != nil {
		return err
	}
	*u = UserInfo(decoded.userInfoAlias)
	if decoded.EmployeeID == "" {
		return nil
	}
	id, err := decoded.EmployeeID.Int64()
	if err != nil {
		return err
	}
	if id > 0 {
		u.EmployeeID = uint64(id)
	}
	return nil
}
