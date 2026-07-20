package jetstream

type (
	JetStreamError interface {
		APIError() *APIError
		error
	}

	jsError struct {
		apiErr  *APIError
		message string
	}

	APIError struct {
		Code        int       `json:"code"`
		ErrorCode   ErrorCode `json:"err_code"`
		Description string    `json:"description,omitempty"`
	}

	ErrorCode uint16
)

const (
	JSErrCodeBadRequest            ErrorCode = 10003
	JSErrCodeConsumerCreate        ErrorCode = 10012
	JSErrCodeConsumerNameExists    ErrorCode = 10013
	JSErrCodeConsumerNotFound      ErrorCode = 10014
	JSErrCodeMaximumConsumersLimit ErrorCode = 10026

	JSErrCodeMessageNotFound               ErrorCode = 10037
	JSErrCodeJetStreamNotEnabledForAccount ErrorCode = 10039

	JSErrCodeStreamNameInUse ErrorCode = 10058
	JSErrCodeStreamNotFound  ErrorCode = 10059

	JSErrCodeStreamWrongLastSequence ErrorCode = 10071
	JSErrCodeJetStreamNotEnabled     ErrorCode = 10076

	JSErrCodeConsumerAlreadyExists ErrorCode = 10105

	JSErrCodeDuplicateFilterSubjects   ErrorCode = 10136
	JSErrCodeOverlappingFilterSubjects ErrorCode = 10138
	JSErrCodeConsumerEmptyFilter       ErrorCode = 10139
	JSErrCodeConsumerExists            ErrorCode = 10148
	JSErrCodeConsumerDoesNotExist      ErrorCode = 10149

	JSErrCodeMirrorWithMsgSchedules   ErrorCode = 10186
	JSErrCodeSourceWithMsgSchedules   ErrorCode = 10187
	JSErrCodeMessageSchedulesDisabled ErrorCode = 10188
	JSErrCodeSchedulePatternInvalid   ErrorCode = 10189
	JSErrCodeScheduleTargetInvalid    ErrorCode = 10190
	JSErrCodeScheduleTTLInvalid       ErrorCode = 10191
	JSErrCodeScheduleRollupInvalid    ErrorCode = 10192
	JSErrCodeScheduleSourceInvalid    ErrorCode = 10203

	JSErrCodeConsumerInvalidReset ErrorCode = 10204
)

var (
	ErrJetStreamNotEnabled JetStreamError = &jsError{apiErr: &APIError{ErrorCode: JSErrCodeJetStreamNotEnabled, Description: "jetstream not enabled", Code: 503}}

	ErrJetStreamNotEnabledForAccount JetStreamError = &jsError{apiErr: &APIError{ErrorCode: JSErrCodeJetStreamNotEnabledForAccount, Description: "jetstream not enabled for account", Code: 503}}

	ErrStreamNotFound JetStreamError = &jsError{apiErr: &APIError{ErrorCode: JSErrCodeStreamNotFound, Description: "stream not found", Code: 404}}

	ErrStreamNameAlreadyInUse JetStreamError = &jsError{apiErr: &APIError{ErrorCode: JSErrCodeStreamNameInUse, Description: "stream name already in use", Code: 400}}

	ErrStreamSubjectTransformNotSupported JetStreamError = &jsError{message: "stream subject transformation not supported by nats-server"}

	ErrStreamSourceSubjectTransformNotSupported JetStreamError = &jsError{message: "stream subject transformation not supported by nats-server"}

	ErrStreamSourceNotSupported JetStreamError = &jsError{message: "stream sourcing is not supported by nats-server"}

	ErrStreamSourceMultipleFilterSubjectsNotSupported JetStreamError = &jsError{message: "stream sourcing with multiple subject filters not supported by nats-server"}

	ErrConsumerNotFound JetStreamError = &jsError{apiErr: &APIError{ErrorCode: JSErrCodeConsumerNotFound, Description: "consumer not found", Code: 404}}

	ErrConsumerCreationResponseEmpty JetStreamError = &jsError{message: "consumer creation response is empty"}

	ErrInvalidJetStreamResponse JetStreamError = &jsError{message: "invalid jetstream api response"}

	ErrConsumerExists JetStreamError = &jsError{apiErr: &APIError{ErrorCode: JSErrCodeConsumerExists, Description: "consumer already exists", Code: 400}}

	ErrConsumerDoesNotExist JetStreamError = &jsError{apiErr: &APIError{ErrorCode: JSErrCodeConsumerDoesNotExist, Description: "consumer does not exist", Code: 400}}

	ErrConsumerResetResponseEmpty JetStreamError = &jsError{message: "consumer reset response is empty"}

	ErrConsumerInvalidReset JetStreamError = &jsError{apiErr: &APIError{ErrorCode: JSErrCodeConsumerInvalidReset, Description: "invalid reset", Code: 400}}

	ErrMsgNotFound JetStreamError = &jsError{apiErr: &APIError{ErrorCode: JSErrCodeMessageNotFound, Description: "message not found", Code: 404}}

	ErrBadRequest JetStreamError = &jsError{apiErr: &APIError{ErrorCode: JSErrCodeBadRequest, Description: "bad request", Code: 400}}

	ErrConsumerCreate JetStreamError = &jsError{apiErr: &APIError{ErrorCode: JSErrCodeConsumerCreate, Description: "could not create consumer", Code: 500}}

	ErrMaximumConsumersLimit JetStreamError = &jsError{apiErr: &APIError{ErrorCode: JSErrCodeMaximumConsumersLimit, Description: "maximum consumers limit reached", Code: 400}}

	ErrDuplicateFilterSubjects JetStreamError = &jsError{apiErr: &APIError{ErrorCode: JSErrCodeDuplicateFilterSubjects, Description: "consumer cannot have both FilterSubject and FilterSubjects specified", Code: 500}}

	ErrOverlappingFilterSubjects JetStreamError = &jsError{apiErr: &APIError{ErrorCode: JSErrCodeOverlappingFilterSubjects, Description: "consumer subject filters cannot overlap", Code: 500}}

	ErrEmptyFilter JetStreamError = &jsError{apiErr: &APIError{ErrorCode: JSErrCodeConsumerEmptyFilter, Description: "consumer filter in FilterSubjects cannot be empty", Code: 500}}

	ErrScheduleTargetInvalid JetStreamError = &jsError{apiErr: &APIError{ErrorCode: JSErrCodeScheduleTargetInvalid, Description: "message schedules target is invalid", Code: 400}}

	ErrSchedulePatternInvalid JetStreamError = &jsError{apiErr: &APIError{ErrorCode: JSErrCodeSchedulePatternInvalid, Description: "message schedules pattern is invalid", Code: 400}}

	ErrMessageSchedulesDisabled JetStreamError = &jsError{apiErr: &APIError{ErrorCode: JSErrCodeMessageSchedulesDisabled, Description: "message schedules is disabled", Code: 400}}

	ErrScheduleSourceInvalid JetStreamError = &jsError{apiErr: &APIError{ErrorCode: JSErrCodeScheduleSourceInvalid, Description: "message schedules source is invalid", Code: 400}}

	ErrScheduleTTLInvalid JetStreamError = &jsError{apiErr: &APIError{ErrorCode: JSErrCodeScheduleTTLInvalid, Description: "message schedules invalid per-message TTL", Code: 400}}

	ErrScheduleRollupInvalid JetStreamError = &jsError{apiErr: &APIError{ErrorCode: JSErrCodeScheduleRollupInvalid, Description: "message schedules invalid rollup", Code: 400}}

	ErrMirrorWithMsgSchedules JetStreamError = &jsError{apiErr: &APIError{ErrorCode: JSErrCodeMirrorWithMsgSchedules, Description: "stream mirrors can not also schedule messages", Code: 400}}

	ErrSourceWithMsgSchedules JetStreamError = &jsError{apiErr: &APIError{ErrorCode: JSErrCodeSourceWithMsgSchedules, Description: "stream source can not also schedule messages", Code: 400}}

	ErrConsumerMultipleFilterSubjectsNotSupported JetStreamError = &jsError{message: "multiple consumer filter subjects not supported by nats-server"}

	ErrConsumerNameAlreadyInUse JetStreamError = &jsError{message: "consumer name already in use"}

	ErrNotPullConsumer JetStreamError = &jsError{message: "consumer is not a pull consumer"}

	ErrNotPushConsumer JetStreamError = &jsError{message: "consumer is not a push consumer"}

	ErrConsumerAlreadyConsuming JetStreamError = &jsError{message: "consumer is already consuming"}

	ErrInvalidJSAck JetStreamError = &jsError{message: "invalid jetstream publish response"}

	ErrStreamNameRequired JetStreamError = &jsError{message: "stream name is required"}

	ErrMsgAlreadyAckd JetStreamError = &jsError{message: "message was already acknowledged"}

	ErrNoStreamResponse JetStreamError = &jsError{message: "no response from stream"}

	ErrNotJSMessage JetStreamError = &jsError{message: "not a jetstream message"}

	ErrInvalidStreamName JetStreamError = &jsError{message: "invalid stream name"}

	ErrInvalidSubject JetStreamError = &jsError{message: "invalid subject name"}

	ErrInvalidConsumerName JetStreamError = &jsError{message: "invalid consumer name"}

	ErrNoMessages JetStreamError = &jsError{message: "no messages"}

	ErrPinIDMismatch JetStreamError = &jsError{message: "pin ID mismatch"}

	ErrMaxBytesExceeded JetStreamError = &jsError{message: "message size exceeds max bytes"}

	ErrBatchCompleted JetStreamError = &jsError{message: "batch completed"}

	ErrConsumerDeleted JetStreamError = &jsError{message: "consumer deleted"}

	ErrConsumerLeadershipChanged JetStreamError = &jsError{message: "leadership change"}

	ErrHandlerRequired JetStreamError = &jsError{message: "handler cannot be empty"}

	ErrEndOfData JetStreamError = &jsError{message: "end of data reached"}

	ErrNoHeartbeat JetStreamError = &jsError{message: "no heartbeat received"}

	ErrConsumerHasActiveSubscription JetStreamError = &jsError{message: "consumer has active subscription"}

	ErrMsgNotBound JetStreamError = &jsError{message: "message is not bound to subscription/connection"}

	ErrMsgNoReply JetStreamError = &jsError{message: "message does not have a reply"}

	ErrMsgDeleteUnsuccessful JetStreamError = &jsError{message: "message deletion unsuccessful"}

	ErrAsyncPublishReplySubjectSet JetStreamError = &jsError{message: "reply subject should be empty"}

	ErrTooManyStalledMsgs JetStreamError = &jsError{message: "stalled with too many outstanding async published messages"}

	ErrInvalidOption JetStreamError = &jsError{message: "invalid jetstream option"}

	ErrMsgIteratorClosed JetStreamError = &jsError{message: "messages iterator closed"}

	ErrConnectionClosed JetStreamError = &jsError{message: "connection closed"}

	ErrServerShutdown JetStreamError = &jsError{message: "server shutdown"}

	ErrOrderedConsumerReset JetStreamError = &jsError{message: "recreating ordered consumer"}

	ErrOrderConsumerUsedAsFetch JetStreamError = &jsError{message: "ordered consumer initialized as fetch"}

	ErrOrderConsumerUsedAsConsume JetStreamError = &jsError{message: "ordered consumer initialized as consume"}

	ErrOrderedConsumerConcurrentRequests JetStreamError = &jsError{message: "cannot run concurrent processing using ordered consumer"}

	ErrOrderedConsumerNotCreated JetStreamError = &jsError{message: "consumer instance not yet created"}

	ErrJetStreamPublisherClosed JetStreamError = &jsError{message: "jetstream context closed"}

	ErrAsyncPublishTimeout JetStreamError = &jsError{message: "timeout waiting for ack"}

	ErrKeyExists JetStreamError = &jsError{apiErr: &APIError{ErrorCode: JSErrCodeStreamWrongLastSequence, Code: 400}, message: "key exists"}

	ErrKeyValueConfigRequired JetStreamError = &jsError{message: "config required"}

	ErrInvalidBucketName JetStreamError = &jsError{message: "invalid bucket name"}

	ErrInvalidKey JetStreamError = &jsError{message: "invalid key"}

	ErrBucketExists JetStreamError = &jsError{message: "bucket name already in use"}

	ErrBucketNotFound JetStreamError = &jsError{message: "bucket not found"}

	ErrBadBucket JetStreamError = &jsError{message: "bucket not valid key-value store"}

	ErrKeyNotFound JetStreamError = &jsError{message: "key not found"}

	ErrKeyDeleted JetStreamError = &jsError{message: "key was deleted"}

	ErrHistoryTooLarge JetStreamError = &jsError{message: "history limited to a max of 64"}

	ErrNoKeysFound JetStreamError = &jsError{message: "no keys found"}

	ErrTTLOnDeleteNotSupported JetStreamError = &jsError{message: "TTL is not supported on delete"}

	ErrLimitMarkerTTLNotSupported JetStreamError = &jsError{message: "limit marker TTLs not supported by server"}

	ErrObjectConfigRequired JetStreamError = &jsError{message: "object-store config required"}

	ErrBadObjectMeta JetStreamError = &jsError{message: "object-store meta information invalid"}

	ErrObjectNotFound JetStreamError = &jsError{message: "object not found"}

	ErrInvalidStoreName JetStreamError = &jsError{message: "invalid object-store name"}

	ErrDigestMismatch JetStreamError = &jsError{message: "received a corrupt object, digests do not match"}

	ErrInvalidDigestFormat JetStreamError = &jsError{message: "object digest hash has invalid format"}

	ErrNoObjectsFound JetStreamError = &jsError{message: "no objects found"}

	ErrObjectAlreadyExists JetStreamError = &jsError{message: "an object already exists with that name"}

	ErrNameRequired JetStreamError = &jsError{message: "name is required"}

	ErrLinkNotAllowed JetStreamError = &jsError{message: "link cannot be set when putting the object in bucket"}

	ErrObjectRequired = &jsError{message: "object required"}

	ErrNoLinkToDeleted JetStreamError = &jsError{message: "not allowed to link to a deleted object"}

	ErrNoLinkToLink JetStreamError = &jsError{message: "not allowed to link to another link"}

	ErrCantGetBucket JetStreamError = &jsError{message: "invalid Get, object is a link to a bucket"}

	ErrBucketRequired JetStreamError = &jsError{message: "bucket required"}

	ErrBucketMalformed JetStreamError = &jsError{message: "bucket malformed"}

	ErrUpdateMetaDeleted JetStreamError = &jsError{message: "cannot update meta for a deleted object"}
)

func (e *APIError) Error() string { _ = "STUB: not implemented"; return "" }

func (e *APIError) APIError() *APIError { _ = "STUB: not implemented"; return nil }

func (e *APIError) Is(err error) bool { _ = "STUB: not implemented"; return false }

func (err *jsError) APIError() *APIError { _ = "STUB: not implemented"; return nil }

func (err *jsError) Error() string { _ = "STUB: not implemented"; return "" }

func (err *jsError) Unwrap() error { _ = "STUB: not implemented"; return nil }
