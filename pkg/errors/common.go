package errors

import (
	"errors"
	"fmt"
	"strings"
	"unicode"

	"github.com/google/uuid"
	"github.com/valyala/fasthttp"
)

const publicTextPrefix = "monetization.errors"

var (
	AccessDeniedError     = func() *Error { return New("access denied", fasthttp.StatusForbidden) }
	ForbiddenError        = func() *Error { return New("forbidden", fasthttp.StatusForbidden) }
	MethodNotAllowedError = func() *Error { return New("method not allowed", fasthttp.StatusMethodNotAllowed) }
	InternalServerError   = func() *Error {
		return New("internal server error", fasthttp.StatusInternalServerError)
	}
	NotFound                = func() *Error { return New("not found", fasthttp.StatusNotFound) }
	BadMetaValue            = func() *Error { return New("bad meta", fasthttp.StatusBadRequest) }
	InvalidRequest          = func() *Error { return New("invalid request", fasthttp.StatusBadRequest) }
	NotInCache              = func() *Error { return New("value not found in cache", fasthttp.StatusBadRequest) }
	InvalidTokenError       = func() *Error { return New("invalid token", fasthttp.StatusUnauthorized) }
	GetSubscriptionError    = func() *Error { return New("could not get subscription", fasthttp.StatusConflict) }
	UpdateSubscriptionError = func() *Error {
		return New("could not update subscription", fasthttp.StatusConflict)
	}
	StatusCode403  = func() *Error { return New("status code 403", fasthttp.StatusForbidden) }
	NotEnoughMoney = func() *Error { return New("not enough money", fasthttp.StatusPaymentRequired) }
	ObjectInUse    = func() *Error { return New("object in use", fasthttp.StatusConflict) }
	ConflictError  = func() *Error { return New("conflict creating operation. id already exsist", fasthttp.StatusConflict) }

	LimitExceededError      = func() *Error { return New("limit exceeded", fasthttp.StatusTooManyRequests) }
	BadRequestError         = func() *Error { return New("bad request", fasthttp.StatusBadRequest) }
	ZeroCountError          = func() *Error { return New("count should be greater than zero", fasthttp.StatusConflict) }
	ErrSubscriptionNotFound = func() *Error { return New("subscription not found", fasthttp.StatusConflict) }
	ErrEventsNotFound       = func() *Error { return New("events not found", fasthttp.StatusConflict) }
	ErrAlreadyPremiumTariff = func() *Error { return New("subscription is already activated by premium", fasthttp.StatusConflict) }
	ErrAlreadyHandled       = func() *Error { return New("subscription is already activated", fasthttp.StatusConflict) }

	ErrUnprocessableEntity = func() *Error {
		return New("unprocessable entity", fasthttp.StatusUnprocessableEntity)
	}

	NotFoundSubscription = func() *Error { return New("can't get subscription", fasthttp.StatusNotFound) }
	AlreadyExists        = func() *Error { return New("key already exists", fasthttp.StatusConflict) }

	PaymentAlreadyExists = func(paymentID uuid.UUID) *Error {
		return New(fmt.Sprintf("payment with ID %s alredy exists", paymentID.String()), fasthttp.StatusConflict).
			SetErrorCode(AlreadyExistsErrorCode)
	}
	PaymentAlreadyInProcess = func(paymentID, previousID uuid.UUID, method string) *Error {
		return New(fmt.Sprintf("payment with ID %s can`t be processed", paymentID.String()), fasthttp.StatusConflict).
			SetErrorCode(AlreadyInProcessErrorCode).
			WithWrapped(fmt.Errorf("payment for this tariff id already exists: previous %s, method %s", previousID.String(), method), "reason")
	}

	PromotionValidateError = func() *Error {
		return New("promotion validate error", fasthttp.StatusUnprocessableEntity)
	}
	PromotionDoesntExist           = func() *Error { return New("promotion doesnt exist", fasthttp.StatusNotFound) }
	PromotionIsNotActive           = func() *Error { return New("promotion is not active", fasthttp.StatusNotFound) }
	PromotionHasActiveSubscription = func() *Error {
		return New("already has active subscription", fasthttp.StatusConflict)
	}
	ErrFailedGetOldSupplierID = func() *Error {
		return New("failed to get old supplier id", fasthttp.StatusInternalServerError)
	}
	ErrFailedCreateTariff = func() *Error {
		return New("failed to create tariff", fasthttp.StatusInternalServerError)
	}
	ErrFailedGetTariffs = func() *Error {
		return New("failed to get tariffs", fasthttp.StatusInternalServerError)
	}
	ErrFailedGetTariffByID = func() *Error {
		return New("failed to get tariff by id", fasthttp.StatusInternalServerError)
	}
	ErrFailedUpdateTariff = func() *Error {
		return New("failed to update tariff", fasthttp.StatusInternalServerError)
	}
	ErrFailedDeleteTariff = func() *Error {
		return New("failed to delete tariff", fasthttp.StatusInternalServerError)
	}
	ErrFailedCreateOption = func() *Error {
		return New("failed to create option", fasthttp.StatusInternalServerError)
	}
	ErrFailedGetOptions = func() *Error {
		return New("failed to get options", fasthttp.StatusInternalServerError)
	}
	ErrFailedGetOptionByID = func() *Error {
		return New("failed to get option by id", fasthttp.StatusInternalServerError)
	}
	ErrFailedUpdateOption = func() *Error {
		return New("failed to update option", fasthttp.StatusInternalServerError)
	}
	ErrFailedDeleteOption = func() *Error {
		return New("failed to delete option", fasthttp.StatusInternalServerError)
	}
	ErrFailedDeleteOptionInUse = func() *Error {
		return New("cannot delete option because it is in use", fasthttp.StatusConflict)
	}
	ErrInvalidCurrencyCode = func() *Error { return New("invalid currency code", fasthttp.StatusBadRequest) }
	ErrEmptyCurrencyCode   = func() *Error { return New("currency code is empty", fasthttp.StatusBadRequest) }
	ErrEmptyOptionIDs      = func() *Error { return New("option ids are empty", fasthttp.StatusBadRequest) }
	ErrEmptyTitle          = func() *Error { return New("title is empty", fasthttp.StatusBadRequest) }
	ErrInvalidOptionIDs    = func() *Error {
		return New("invalid option ids: option does not exist or was deleted", fasthttp.StatusBadRequest)
	}
	ErrInvalidType                     = func() *Error { return New("invalid type", fasthttp.StatusBadRequest) }
	ErrActivatedAtAndIsActiveCollision = func() *Error {
		return New("isActive flag and activatedAt are both set", fasthttp.StatusBadRequest)
	}
	ErrInvalidTimestamp           = func() *Error { return New("invalid timestamp format", fasthttp.StatusBadRequest) }
	ErrGetTariffsWithSameCategory = func() *Error {
		return New("failed to get tariffs with same category", fasthttp.StatusInternalServerError)
	}
	ErrGetTariffsWithSameCategoryByTariffType = func() *Error {
		return New("failed to get tariffs with same category by tariff type", fasthttp.StatusInternalServerError)
	}
	ErrGetSlaveTariffs = func() *Error {
		return New("failed to get slave tariffs", fasthttp.StatusInternalServerError)
	}
	ErrGetTariffsByCategory = func() *Error {
		return New("failed to get tariffs by category", fasthttp.StatusInternalServerError)
	}
	ErrFailedGetTariffInfoByIDStrict = func() *Error {
		return New("failed to get tariff info by id", fasthttp.StatusInternalServerError)
	}
	ErrFailedGetAliasedTariff = func() *Error {
		return New("failed to get id by alias", fasthttp.StatusInternalServerError)
	}
	ErrFailedGetTariffIDsForPromo = func() *Error {
		return New("failed to get tariff ids for promo", fasthttp.StatusInternalServerError)
	}

	UnavailablePeriod     = func() *Error { return New("such period is unavailable", fasthttp.StatusBadRequest) }
	UnavailableInDemo     = func() *Error { return New("unavailable in demo", fasthttp.StatusBadRequest) }
	TariffReadOnly        = func() *Error { return New("tariff is read-only", fasthttp.StatusBadRequest) }
	PromoUnavailableError = func() *Error {
		return New("promo code is unavailable", fasthttp.StatusBadRequest)
	}
	PremiumOptionUnavailableError = func() *Error {
		return New("premium option is unavailable", fasthttp.StatusBadRequest)
	}
	TariffNotAllowedError = func() *Error {
		return New("tariff is not allowed for this service name", fasthttp.StatusForbidden)
	}
	PaymentStatusError = func() *Error {
		return New("payment status is not available", fasthttp.StatusConflict)
	}
	PaymentMethodError = func() *Error {
		return New("payment method is not available", fasthttp.StatusConflict)
	}
	SupplierBalanceError = func() *Error {
		return New("supplier balance error", fasthttp.StatusNotFound)
	}
)

func GetWrappedErrorForConfig(err error, paramName, via string) error {
	return Wrap(err, fmt.Sprintf("BrokenConfig - %s has wrong format, must be parsed via `%s`", paramName, via))
}

func buildPublicText(serviceName, subsystem, msg string) string {
	parts := []string{publicTextPrefix}
	if serviceName != "" {
		parts = append(parts, serviceName)
	}
	if subsystem != "" {
		parts = append(parts, subsystem)
	}
	parts = append(parts, toCamelCase(msg))
	return strings.Join(parts, ".")
}

func toCamelCase(s string) string {
	words := strings.FieldsFunc(s, func(r rune) bool {
		return !unicode.IsLetter(r) && !unicode.IsNumber(r)
	})
	if len(words) == 0 {
		return s
	}

	result := strings.ToLower(words[0])
	for _, w := range words[1:] {
		result += capitalize(strings.ToLower(w))
	}

	return result
}

func capitalize(s string) string {
	if s == "" {
		return ""
	}
	r := []rune(s)
	r[0] = unicode.ToUpper(r[0])
	return string(r)
}

func getCustomError(err1 error, err2 error) (*Error, *Error, bool) {
	var e1, e2 *Error

	var jsonErr1 *JsonRPCError
	ok := errors.As(err1, &jsonErr1)
	if ok {
		e1 = jsonErr1.Data
	} else {
		ok = errors.As(err1, &e1)
		if !ok {
			return e1, e2, false
		}
	}

	var jsonErr2 *JsonRPCError
	ok = errors.As(err2, &jsonErr2)
	if ok {
		e2 = jsonErr2.Data
	} else {
		ok = errors.As(err2, &e2)
		if !ok {
			return e1, e2, false
		}
	}

	return e1, e2, true
}

func (e *Error) addCause(publicReason, internalReason string) *Error {
	if e.Cause == nil {
		e.Cause = make(map[string]any)
	}
	e.Cause[publicReason] = internalReason
	return e
}
