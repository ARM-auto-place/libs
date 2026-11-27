package errors

import (
	"errors"
	"fmt"
	"strings"
)

type Error struct {
	ErrorText  string
	Cause      map[string]any
	publicText string
	wrap       error

	statusCode int
	errorCode  *int
}

func New(msg string, statusCode int) *Error {
	return defaultFactory.New(msg, statusCode)
}

func Wrap(err error, publicReason string) *Error {
	return defaultFactory.Wrap(err, publicReason)
}

func (e *Error) Error() string {
	var cause string
	if e.Cause != nil && len(e.Cause) > 0 {
		cause = fmt.Sprintf(". Causes: %v", e.Cause)
	}
	return e.ErrorText + cause
}

func Is(errOne, errTwo error) bool {
	custErrOne, custErrTwo, ok := getCustomError(errOne, errTwo)

	if ok {
		return custErrOne.ErrorText == custErrTwo.ErrorText
	}

	return errOne.Error() == errTwo.Error()
}

func HasCause(e error, cause string) bool {
	var custErr *Error
	ok := errors.As(e, &custErr)
	if !ok {
		return false
	}

	for key, value := range custErr.Cause {
		if strings.Contains(key, cause) {
			return true
		}
		if s, ok := value.(string); ok && strings.Contains(s, cause) {
			return true
		}
	}

	return false
}

func (e *Error) SetStatusCode(code int) *Error {
	e.statusCode = code
	return e
}

func (e *Error) GetStatusCode() int {
	return e.statusCode
}
func (e *Error) Code() int {
	if e.errorCode != nil {
		return *e.errorCode
	}
	return e.statusCode
}

func (e *Error) SetErrorCode(code int) *Error {
	e.errorCode = &code
	return e
}

func EqualErrorCode(err error, target int) bool {
	var custErr *Error
	if !errors.As(err, &custErr) || custErr.errorCode == nil {
		return false
	}
	return *custErr.errorCode == target
}

func (e *Error) GetPublicText() string {
	return e.publicText
}

func (e *Error) WithWrapped(err error, publicReason string) *Error {
	e.wrap = err
	return e.addCause(publicReason, err.Error())
}

func (e *Error) Unwrap() error {
	return e.wrap
}
