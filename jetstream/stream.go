package jetstream

import (
	"context"
	"time"

	"github.com/nats-io/nats.go"
)

type (
	Stream interface {
		ConsumerManager

		Info(ctx context.Context, opts ...StreamInfoOpt) (*StreamInfo, error)

		CachedInfo() *StreamInfo

		Purge(ctx context.Context, opts ...StreamPurgeOpt) error

		GetMsg(ctx context.Context, seq uint64, opts ...GetMsgOpt) (*RawStreamMsg, error)

		GetLastMsgForSubject(ctx context.Context, subject string) (*RawStreamMsg, error)

		DeleteMsg(ctx context.Context, seq uint64) error

		SecureDeleteMsg(ctx context.Context, seq uint64) error
	}

	ConsumerManager interface {
		CreateOrUpdateConsumer(ctx context.Context, cfg ConsumerConfig) (Consumer, error)

		CreateConsumer(ctx context.Context, cfg ConsumerConfig) (Consumer, error)

		UpdateConsumer(ctx context.Context, cfg ConsumerConfig) (Consumer, error)

		OrderedConsumer(ctx context.Context, cfg OrderedConsumerConfig) (Consumer, error)

		Consumer(ctx context.Context, consumer string) (Consumer, error)

		DeleteConsumer(ctx context.Context, consumer string) error

		PauseConsumer(ctx context.Context, consumer string, pauseUntil time.Time) (*ConsumerPauseResponse, error)

		ResumeConsumer(ctx context.Context, consumer string) (*ConsumerPauseResponse, error)

		ListConsumers(context.Context) ConsumerInfoLister

		ConsumerNames(context.Context) ConsumerNameLister

		UnpinConsumer(ctx context.Context, consumer string, group string) error

		ResetConsumer(ctx context.Context, consumer string) (*ConsumerResetResponse, error)

		ResetConsumerToSequence(ctx context.Context, consumer string, seq uint64) (*ConsumerResetResponse, error)

		CreateOrUpdatePushConsumer(ctx context.Context, cfg ConsumerConfig) (PushConsumer, error)

		CreatePushConsumer(ctx context.Context, cfg ConsumerConfig) (PushConsumer, error)

		UpdatePushConsumer(ctx context.Context, cfg ConsumerConfig) (PushConsumer, error)

		PushConsumer(ctx context.Context, consumer string) (PushConsumer, error)
	}

	RawStreamMsg struct {
		Subject  string
		Sequence uint64
		Header   nats.Header
		Data     []byte
		Time     time.Time
	}

	stream struct {
		name string
		info *StreamInfo
		js   *jetStream
	}

	StreamInfoOpt func(*streamInfoRequest) error

	streamInfoRequest struct {
		apiPagedRequest
		DeletedDetails bool   `json:"deleted_details,omitempty"`
		SubjectFilter  string `json:"subjects_filter,omitempty"`
	}

	consumerInfoResponse struct {
		apiResponse
		*ConsumerInfo
	}

	StreamPurgeOpt func(*StreamPurgeRequest) error

	StreamPurgeRequest struct {
		Sequence uint64 `json:"seq,omitempty"`

		Subject string `json:"filter,omitempty"`

		Keep uint64 `json:"keep,omitempty"`
	}

	streamPurgeResponse struct {
		apiResponse
		Success bool   `json:"success,omitempty"`
		Purged  uint64 `json:"purged"`
	}

	consumerDeleteResponse struct {
		apiResponse
		Success bool `json:"success,omitempty"`
	}

	consumerPauseRequest struct {
		PauseUntil *time.Time `json:"pause_until,omitempty"`
	}

	ConsumerPauseResponse struct {
		Paused bool `json:"paused"`

		PauseUntil time.Time `json:"pause_until"`

		PauseRemaining time.Duration `json:"pause_remaining,omitempty"`
	}

	consumerPauseApiResponse struct {
		apiResponse
		ConsumerPauseResponse
	}

	consumerResetRequest struct {
		Seq uint64 `json:"seq,omitempty"`
	}

	ConsumerResetResponse struct {
		*ConsumerInfo

		ResetSeq uint64 `json:"reset_seq"`
	}

	consumerResetApiResponse struct {
		apiResponse
		ConsumerResetResponse
	}

	GetMsgOpt func(*apiMsgGetRequest) error

	apiMsgGetRequest struct {
		Seq     uint64 `json:"seq,omitempty"`
		LastFor string `json:"last_by_subj,omitempty"`
		NextFor string `json:"next_by_subj,omitempty"`
	}

	apiMsgGetResponse struct {
		apiResponse
		Message *storedMsg `json:"message,omitempty"`
	}

	storedMsg struct {
		Subject  string    `json:"subject"`
		Sequence uint64    `json:"seq"`
		Header   []byte    `json:"hdrs,omitempty"`
		Data     []byte    `json:"data,omitempty"`
		Time     time.Time `json:"time"`
	}

	msgDeleteRequest struct {
		Seq     uint64 `json:"seq"`
		NoErase bool   `json:"no_erase,omitempty"`
	}

	msgDeleteResponse struct {
		apiResponse
		Success bool `json:"success,omitempty"`
	}

	ConsumerInfoLister interface {
		Info() <-chan *ConsumerInfo
		Err() error
	}

	ConsumerNameLister interface {
		Name() <-chan string
		Err() error
	}

	consumerLister struct {
		js       *jetStream
		offset   int
		pageInfo *apiPaged

		consumers chan *ConsumerInfo
		names     chan string
		err       error
	}

	consumerListResponse struct {
		apiResponse
		apiPaged
		Consumers []*ConsumerInfo `json:"consumers"`
	}

	consumerNamesResponse struct {
		apiResponse
		apiPaged
		Consumers []string `json:"consumers"`
	}

	consumerUnpinRequest struct {
		Group string `json:"group"`
	}
)

func (s *stream) CreateOrUpdateConsumer(ctx context.Context, cfg ConsumerConfig) (Consumer, error) {
	_ = "STUB: not implemented"
	return *new(Consumer), nil
}

func (s *stream) CreateConsumer(ctx context.Context, cfg ConsumerConfig) (Consumer, error) {
	_ = "STUB: not implemented"
	return *new(Consumer), nil
}

func (s *stream) UpdateConsumer(ctx context.Context, cfg ConsumerConfig) (Consumer, error) {
	_ = "STUB: not implemented"
	return *new(Consumer), nil
}

func (s *stream) CreateOrUpdatePushConsumer(ctx context.Context, cfg ConsumerConfig) (PushConsumer, error) {
	_ = "STUB: not implemented"
	return *new(PushConsumer), nil
}

func (s *stream) CreatePushConsumer(ctx context.Context, cfg ConsumerConfig) (PushConsumer, error) {
	_ = "STUB: not implemented"
	return *new(PushConsumer), nil
}

func (s *stream) UpdatePushConsumer(ctx context.Context, cfg ConsumerConfig) (PushConsumer, error) {
	_ = "STUB: not implemented"
	return *new(PushConsumer), nil
}

func (s *stream) OrderedConsumer(ctx context.Context, cfg OrderedConsumerConfig) (Consumer, error) {
	_ = "STUB: not implemented"
	return *new(Consumer), nil
}

func (s *stream) Consumer(ctx context.Context, name string) (Consumer, error) {
	_ = "STUB: not implemented"
	return *new(Consumer), nil
}

func (s *stream) PushConsumer(ctx context.Context, name string) (PushConsumer, error) {
	_ = "STUB: not implemented"
	return *new(PushConsumer), nil
}

func (s *stream) DeleteConsumer(ctx context.Context, name string) error {
	_ = "STUB: not implemented"
	return nil
}

func (s *stream) PauseConsumer(ctx context.Context, name string, pauseUntil time.Time) (*ConsumerPauseResponse, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func (s *stream) ResumeConsumer(ctx context.Context, name string) (*ConsumerPauseResponse, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func (s *stream) Info(ctx context.Context, opts ...StreamInfoOpt) (*StreamInfo, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func (s *stream) CachedInfo() *StreamInfo { _ = "STUB: not implemented"; return nil }

func (s *stream) Purge(ctx context.Context, opts ...StreamPurgeOpt) error {
	_ = "STUB: not implemented"
	return nil
}

func (s *stream) GetMsg(ctx context.Context, seq uint64, opts ...GetMsgOpt) (*RawStreamMsg, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func (s *stream) GetLastMsgForSubject(ctx context.Context, subject string) (*RawStreamMsg, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func (s *stream) getMsg(ctx context.Context, mreq *apiMsgGetRequest) (*RawStreamMsg, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func convertDirectGetMsgResponseToMsg(r *nats.Msg) (*RawStreamMsg, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func (s *stream) DeleteMsg(ctx context.Context, seq uint64) error {
	_ = "STUB: not implemented"
	return nil
}

func (s *stream) SecureDeleteMsg(ctx context.Context, seq uint64) error {
	_ = "STUB: not implemented"
	return nil
}

func (s *stream) deleteMsg(ctx context.Context, req *msgDeleteRequest) error {
	_ = "STUB: not implemented"
	return nil
}

func (s *stream) ListConsumers(ctx context.Context) ConsumerInfoLister {
	_ = "STUB: not implemented"
	return *new(ConsumerInfoLister)
}

func (s *consumerLister) Info() <-chan *ConsumerInfo { _ = "STUB: not implemented"; return nil }

func (s *consumerLister) Err() error { _ = "STUB: not implemented"; return nil }

func (s *stream) ConsumerNames(ctx context.Context) ConsumerNameLister {
	_ = "STUB: not implemented"
	return *new(ConsumerNameLister)
}

func (s *consumerLister) Name() <-chan string { _ = "STUB: not implemented"; return nil }

func (s *consumerLister) consumerInfos(ctx context.Context, stream string) ([]*ConsumerInfo, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func (s *consumerLister) consumerNames(ctx context.Context, stream string) ([]string, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func (s *stream) UnpinConsumer(ctx context.Context, consumer string, group string) error {
	_ = "STUB: not implemented"
	return nil
}

func (s *stream) ResetConsumer(ctx context.Context, consumer string) (*ConsumerResetResponse, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func (s *stream) ResetConsumerToSequence(ctx context.Context, consumer string, seq uint64) (*ConsumerResetResponse, error) {
	_ = "STUB: not implemented"
	return nil, nil
}
