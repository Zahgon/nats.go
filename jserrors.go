package nats

import (
	"errors"
)

var (
	ErrJetStreamNotEnabled JetStreamError = &jsError{apiErr: &APIError{ErrorCode: JSErrCodeJetStreamNotEnabled, Description: "jetstream not enabled", Code: 503}}

	ErrJetStreamNotEnabledForAccount JetStreamError = &jsError{apiErr: &APIError{ErrorCode: JSErrCodeJetStreamNotEnabledForAccount, Description: "jetstream not enabled for account", Code: 503}}

	ErrStreamNotFound JetStreamError = &jsError{apiErr: &APIError{ErrorCode: JSErrCodeStreamNotFound, Description: "stream not found", Code: 404}}

	ErrStreamNameAlreadyInUse JetStreamError = &jsError{apiErr: &APIError{ErrorCode: JSErrCodeStreamNameInUse, Description: "stream name already in use", Code: 400}}

	ErrStreamSubjectTransformNotSupported JetStreamError = &jsError{message: "stream subject transformation not supported by nats-server"}

	ErrStreamSourceSubjectTransformNotSupported JetStreamError = &jsError{message: "stream subject transformation not supported by nats-server"}

	ErrStreamSourceNotSupported JetStreamError = &jsError{message: "stream sourcing is not supported by nats-server"}

	ErrStreamSourceMultipleSubjectTransformsNotSupported JetStreamError = &jsError{message: "stream sourcing with multiple subject transforms not supported by nats-server"}

	ErrConsumerNotFound JetStreamError = &jsError{apiErr: &APIError{ErrorCode: JSErrCodeConsumerNotFound, Description: "consumer not found", Code: 404}}

	ErrConsumerCreationResponseEmpty JetStreamError = &jsError{message: "consumer creation response is empty"}

	ErrMsgNotFound JetStreamError = &jsError{apiErr: &APIError{ErrorCode: JSErrCodeMessageNotFound, Description: "message not found", Code: 404}}

	ErrBadRequest JetStreamError = &jsError{apiErr: &APIError{ErrorCode: JSErrCodeBadRequest, Description: "bad request", Code: 400}}

	ErrDuplicateFilterSubjects JetStreamError = &jsError{apiErr: &APIError{ErrorCode: JSErrCodeDuplicateFilterSubjects, Description: "consumer cannot have both FilterSubject and FilterSubjects specified", Code: 500}}

	ErrOverlappingFilterSubjects JetStreamError = &jsError{apiErr: &APIError{ErrorCode: JSErrCodeOverlappingFilterSubjects, Description: "consumer subject filters cannot overlap", Code: 500}}

	ErrEmptyFilter JetStreamError = &jsError{apiErr: &APIError{ErrorCode: JSErrCodeConsumerEmptyFilter, Description: "consumer filter in FilterSubjects cannot be empty", Code: 500}}

	ErrConsumerNameAlreadyInUse JetStreamError = &jsError{message: "consumer name already in use"}

	ErrConsumerNotActive JetStreamError = &jsError{message: "consumer not active"}

	ErrInvalidJSAck JetStreamError = &jsError{message: "invalid jetstream publish response"}

	ErrStreamConfigRequired JetStreamError = &jsError{message: "stream configuration is required"}

	ErrStreamNameRequired JetStreamError = &jsError{message: "stream name is required"}

	ErrConsumerNameRequired JetStreamError = &jsError{message: "consumer name is required"}

	ErrConsumerMultipleFilterSubjectsNotSupported JetStreamError = &jsError{message: "multiple consumer filter subjects not supported by nats-server"}

	ErrConsumerConfigRequired JetStreamError = &jsError{message: "consumer configuration is required"}

	ErrPullSubscribeToPushConsumer JetStreamError = &jsError{message: "cannot pull subscribe to push based consumer"}

	ErrPullSubscribeRequired JetStreamError = &jsError{message: "must use pull subscribe to bind to pull based consumer"}

	ErrMsgAlreadyAckd JetStreamError = &jsError{message: "message was already acknowledged"}

	ErrNoStreamResponse JetStreamError = &jsError{message: "no response from stream"}

	ErrNotJSMessage JetStreamError = &jsError{message: "not a jetstream message"}

	ErrInvalidStreamName JetStreamError = &jsError{message: "invalid stream name"}

	ErrInvalidConsumerName JetStreamError = &jsError{message: "invalid consumer name"}

	ErrInvalidFilterSubject JetStreamError = &jsError{message: "invalid filter subject"}

	ErrNoMatchingStream JetStreamError = &jsError{message: "no stream matches subject"}

	ErrSubjectMismatch JetStreamError = &jsError{message: "subject does not match consumer"}

	ErrContextAndTimeout JetStreamError = &jsError{message: "context and timeout can not both be set"}

	ErrCantAckIfConsumerAckNone JetStreamError = &jsError{message: "cannot acknowledge a message for a consumer with AckNone policy"}

	ErrConsumerDeleted JetStreamError = &jsError{message: "consumer deleted"}

	ErrConsumerLeadershipChanged JetStreamError = &jsError{message: "Leadership Changed"}

	ErrConsumerInfoOnOrderedReset JetStreamError = &jsError{message: "cannot fetch consumer info; ordered consumer is being reset"}

	ErrNoHeartbeat JetStreamError = &jsError{message: "no heartbeat received"}

	ErrSubscriptionClosed JetStreamError = &jsError{message: "subscription closed"}

	ErrJetStreamPublisherClosed JetStreamError = &jsError{message: "jetstream context closed"}

	ErrInvalidDurableName = errors.New("nats: invalid durable name")

	ErrAsyncPublishTimeout JetStreamError = &jsError{message: "timeout waiting for ack"}

	ErrTooManyStalledMsgs JetStreamError = &jsError{message: "stalled with too many outstanding async published messages"}

	ErrFetchDisconnected = &jsError{message: "disconnected during fetch"}
)

type ErrorCode uint16

const (
	JSErrCodeJetStreamNotEnabledForAccount ErrorCode = 10039
	JSErrCodeJetStreamNotEnabled           ErrorCode = 10076
	JSErrCodeInsufficientResourcesErr      ErrorCode = 10023
	JSErrCodeJetStreamNotAvailable         ErrorCode = 10008

	JSErrCodeStreamNotFound  ErrorCode = 10059
	JSErrCodeStreamNameInUse ErrorCode = 10058

	JSErrCodeConsumerNotFound          ErrorCode = 10014
	JSErrCodeConsumerNameExists        ErrorCode = 10013
	JSErrCodeConsumerAlreadyExists     ErrorCode = 10105
	JSErrCodeDuplicateFilterSubjects   ErrorCode = 10136
	JSErrCodeOverlappingFilterSubjects ErrorCode = 10138
	JSErrCodeConsumerEmptyFilter       ErrorCode = 10139

	JSErrCodeMessageNotFound ErrorCode = 10037

	JSErrCodeBadRequest   ErrorCode = 10003
	JSStreamInvalidConfig ErrorCode = 10052

	JSErrCodeStreamWrongLastSequence ErrorCode = 10071
)

type APIError struct {
	Code        int       `json:"code"`
	ErrorCode   ErrorCode `json:"err_code"`
	Description string    `json:"description,omitempty"`
}

func (e *APIError) Error() string { _ = "STUB: not implemented"; return "" }

func (e *APIError) APIError() *APIError { _ = "STUB: not implemented"; return nil }

func (e *APIError) Is(err error) bool { _ = "STUB: not implemented"; return false }

type JetStreamError interface {
	APIError() *APIError
	error
}

type jsError struct {
	apiErr  *APIError
	message string
}

func (err *jsError) APIError() *APIError { _ = "STUB: not implemented"; return nil }

func (err *jsError) Error() string { _ = "STUB: not implemented"; return "" }

func (err *jsError) Unwrap() error { _ = "STUB: not implemented"; return nil }
