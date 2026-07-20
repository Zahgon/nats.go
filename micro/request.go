package micro

import (
	"context"
	"errors"

	"github.com/nats-io/nats.go"
)

type (
	Handler interface {
		Handle(Request)
	}

	HandlerFunc func(Request)

	Request interface {
		Respond([]byte, ...RespondOpt) error

		RespondJSON(any, ...RespondOpt) error

		Error(code, description string, data []byte, opts ...RespondOpt) error

		Data() []byte

		Headers() Headers

		Subject() string

		Reply() string
	}

	Headers nats.Header

	RespondOpt func(*nats.Msg)

	request struct {
		msg          *nats.Msg
		respondError error
	}

	serviceError struct {
		Code        string `json:"code"`
		Description string `json:"description"`
	}
)

var (
	ErrRespond         = errors.New("NATS error when sending response")
	ErrMarshalResponse = errors.New("marshaling response")
	ErrArgRequired     = errors.New("argument required")
)

func (fn HandlerFunc) Handle(req Request) { _ = "STUB: not implemented"; return }

func ContextHandler(ctx context.Context, handler func(context.Context, Request)) Handler {
	_ = "STUB: not implemented"
	return *new(Handler)
}

func (r *request) Respond(response []byte, opts ...RespondOpt) error {
	_ = "STUB: not implemented"
	return nil
}

func (r *request) RespondJSON(response any, opts ...RespondOpt) error {
	_ = "STUB: not implemented"
	return nil
}

func (r *request) Error(code, description string, data []byte, opts ...RespondOpt) error {
	_ = "STUB: not implemented"
	return nil
}

func WithHeaders(headers Headers) RespondOpt { _ = "STUB: not implemented"; return *new(RespondOpt) }

func (r *request) Data() []byte { _ = "STUB: not implemented"; return nil }

func (r *request) Headers() Headers { _ = "STUB: not implemented"; return *new(Headers) }

func (r *request) Subject() string { _ = "STUB: not implemented"; return "" }

func (r *request) Reply() string { _ = "STUB: not implemented"; return "" }

func (h Headers) Get(key string) string { _ = "STUB: not implemented"; return "" }

func (h Headers) Values(key string) []string { _ = "STUB: not implemented"; return nil }

func (e *serviceError) Error() string { _ = "STUB: not implemented"; return "" }
