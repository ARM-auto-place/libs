package errors

import stderrors "errors"

var defaultFactory = &Factory{}

func SetServiceInfo(svc, sub string) {
	defaultFactory.serviceName = svc
	defaultFactory.subsystem = sub
}

type Factory struct {
	serviceName string
	subsystem   string
}

// Если в рамках сервиса есть несколько subsystem, но придется передавать его
func NewFactory(serviceName, subsystem string) *Factory {
	return &Factory{
		serviceName: serviceName,
		subsystem:   subsystem,
	}
}

func (f *Factory) New(msg string, statusCode int) *Error {
	return &Error{
		ErrorText:  msg,
		statusCode: statusCode,
		publicText: buildPublicText(f.serviceName, f.subsystem, msg),
	}
}

func (f *Factory) Wrap(err error, publicReason string) *Error {
	if err == nil {
		return nil
	}

	e := &Error{
		Cause:      make(map[string]any),
		wrap:       err,
		ErrorText:  err.Error(),
		publicText: buildPublicText(f.serviceName, f.subsystem, err.Error()),
	}

	var existing *Error
	if stderrors.As(err, &existing) {
		e.ErrorText = existing.ErrorText
		e.statusCode = existing.statusCode
		e.publicText = existing.publicText
	}

	return e.addCause(publicReason, err.Error())
}
