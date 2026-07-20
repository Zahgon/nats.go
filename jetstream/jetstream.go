package jetstream

import (
	"context"
	"regexp"
	"time"

	"github.com/nats-io/nats.go"
)

type (
	JetStream interface {
		AccountInfo(ctx context.Context) (*AccountInfo, error)

		Conn() *nats.Conn

		Options() JetStreamOptions

		StreamConsumerManager
		StreamManager
		Publisher
		KeyValueManager
		ObjectStoreManager
	}

	Publisher interface {
		Publish(ctx context.Context, subject string, payload []byte, opts ...PublishOpt) (*PubAck, error)

		PublishMsg(ctx context.Context, msg *nats.Msg, opts ...PublishOpt) (*PubAck, error)

		PublishAsync(subject string, payload []byte, opts ...PublishOpt) (PubAckFuture, error)

		PublishMsgAsync(msg *nats.Msg, opts ...PublishOpt) (PubAckFuture, error)

		PublishAsyncPending() int

		PublishAsyncComplete() <-chan struct{}

		CleanupPublisher()
	}

	StreamManager interface {
		CreateStream(ctx context.Context, cfg StreamConfig) (Stream, error)

		UpdateStream(ctx context.Context, cfg StreamConfig) (Stream, error)

		CreateOrUpdateStream(ctx context.Context, cfg StreamConfig) (Stream, error)

		Stream(ctx context.Context, stream string) (Stream, error)

		StreamNameBySubject(ctx context.Context, subject string) (string, error)

		DeleteStream(ctx context.Context, stream string) error

		ListStreams(context.Context, ...StreamListOpt) StreamInfoLister

		StreamNames(context.Context, ...StreamListOpt) StreamNameLister
	}

	StreamConsumerManager interface {
		CreateOrUpdateConsumer(ctx context.Context, stream string, cfg ConsumerConfig) (Consumer, error)

		CreateConsumer(ctx context.Context, stream string, cfg ConsumerConfig) (Consumer, error)

		UpdateConsumer(ctx context.Context, stream string, cfg ConsumerConfig) (Consumer, error)

		OrderedConsumer(ctx context.Context, stream string, cfg OrderedConsumerConfig) (Consumer, error)

		Consumer(ctx context.Context, stream string, consumer string) (Consumer, error)

		DeleteConsumer(ctx context.Context, stream string, consumer string) error

		PauseConsumer(ctx context.Context, stream string, consumer string, pauseUntil time.Time) (*ConsumerPauseResponse, error)

		ResumeConsumer(ctx context.Context, stream string, consumer string) (*ConsumerPauseResponse, error)

		ResetConsumer(ctx context.Context, stream, consumer string) (*ConsumerResetResponse, error)

		ResetConsumerToSequence(ctx context.Context, stream, consumer string, seq uint64) (*ConsumerResetResponse, error)

		CreateOrUpdatePushConsumer(ctx context.Context, stream string, cfg ConsumerConfig) (PushConsumer, error)

		CreatePushConsumer(ctx context.Context, stream string, cfg ConsumerConfig) (PushConsumer, error)

		UpdatePushConsumer(ctx context.Context, stream string, cfg ConsumerConfig) (PushConsumer, error)

		PushConsumer(ctx context.Context, stream string, consumer string) (PushConsumer, error)
	}

	StreamListOpt func(*streamsRequest) error

	AccountInfo struct {
		Tier

		Domain string `json:"domain"`

		API APIStats `json:"api"`

		Tiers map[string]Tier `json:"tiers"`
	}

	Tier struct {
		Memory uint64 `json:"memory"`

		Store uint64 `json:"storage"`

		ReservedMemory uint64 `json:"reserved_memory"`

		ReservedStore uint64 `json:"reserved_storage"`

		Streams int `json:"streams"`

		Consumers int `json:"consumers"`

		Limits AccountLimits `json:"limits"`
	}

	APIStats struct {
		Level int `json:"level"`

		Total uint64 `json:"total"`

		Errors uint64 `json:"errors"`

		Inflight uint64 `json:"inflight,omitempty"`
	}

	AccountLimits struct {
		MaxMemory int64 `json:"max_memory"`

		MaxStore int64 `json:"max_storage"`

		MaxStreams int `json:"max_streams"`

		MaxConsumers int `json:"max_consumers"`

		MaxAckPending int `json:"max_ack_pending"`

		MemoryMaxStreamBytes int64 `json:"memory_max_stream_bytes"`

		StoreMaxStreamBytes int64 `json:"storage_max_stream_bytes"`

		MaxBytesRequired bool `json:"max_bytes_required"`
	}

	jetStream struct {
		conn *nats.Conn
		opts JetStreamOptions

		publisher *jetStreamClient
	}

	JetStreamOpt func(*JetStreamOptions) error

	JetStreamOptions struct {
		APIPrefix string

		Domain string

		DefaultTimeout time.Duration

		ClientTrace *ClientTrace

		publisherOpts asyncPublisherOpts

		apiPrefix      string
		replyPrefix    string
		replyPrefixLen int
	}

	ClientTrace struct {
		RequestSent func(subj string, payload []byte)

		ResponseReceived func(subj string, payload []byte, hdr nats.Header)
	}
	streamInfoResponse struct {
		apiResponse
		apiPaged
		*StreamInfo
	}

	accountInfoResponse struct {
		apiResponse
		AccountInfo
	}

	streamDeleteResponse struct {
		apiResponse
		Success bool `json:"success,omitempty"`
	}

	StreamInfoLister interface {
		Info() <-chan *StreamInfo
		Err() error
	}

	StreamNameLister interface {
		Name() <-chan string
		Err() error
	}

	apiPagedRequest struct {
		Offset int `json:"offset"`
	}

	streamLister struct {
		js       *jetStream
		offset   int
		pageInfo *apiPaged

		streams chan *StreamInfo
		names   chan string
		err     error
	}

	streamListResponse struct {
		apiResponse
		apiPaged
		Streams []*StreamInfo `json:"streams"`
	}

	streamNamesResponse struct {
		apiResponse
		apiPaged
		Streams []string `json:"streams"`
	}

	streamsRequest struct {
		apiPagedRequest
		Subject string `json:"subject,omitempty"`
	}
)

const defaultAPITimeout = 5 * time.Second

var subjectRegexp = regexp.MustCompile(`^[^ >]*[>]?$`)

func New(nc *nats.Conn, opts ...JetStreamOpt) (JetStream, error) {
	_ = "STUB: not implemented"
	return *new(JetStream), nil
}

const (
	defaultAsyncPubAckInflight = 4000
)

func setReplyPrefix(nc *nats.Conn, jsOpts *JetStreamOptions) { _ = "STUB: not implemented"; return }

func NewWithAPIPrefix(nc *nats.Conn, apiPrefix string, opts ...JetStreamOpt) (JetStream, error) {
	_ = "STUB: not implemented"
	return *new(JetStream), nil
}

func NewWithDomain(nc *nats.Conn, domain string, opts ...JetStreamOpt) (JetStream, error) {
	_ = "STUB: not implemented"
	return *new(JetStream), nil
}

func (js *jetStream) Conn() *nats.Conn { _ = "STUB: not implemented"; return nil }

func (js *jetStream) Options() JetStreamOptions {
	_ = "STUB: not implemented"
	return *new(JetStreamOptions)
}

func (js *jetStream) CreateStream(ctx context.Context, cfg StreamConfig) (Stream, error) {
	_ = "STUB: not implemented"
	return *new(Stream), nil
}

func (ss *StreamSource) convertDomain() error { _ = "STUB: not implemented"; return nil }

func (ss *StreamSource) copy() *StreamSource { _ = "STUB: not implemented"; return nil }

func convertStreamConfigDomains(cfg StreamConfig) (StreamConfig, error) {
	_ = "STUB: not implemented"
	return *new(StreamConfig), nil
}

func (js *jetStream) UpdateStream(ctx context.Context, cfg StreamConfig) (Stream, error) {
	_ = "STUB: not implemented"
	return *new(Stream), nil
}

func (js *jetStream) CreateOrUpdateStream(ctx context.Context, cfg StreamConfig) (Stream, error) {
	_ = "STUB: not implemented"
	return *new(Stream), nil
}

func (js *jetStream) Stream(ctx context.Context, name string) (Stream, error) {
	_ = "STUB: not implemented"
	return *new(Stream), nil
}

func (js *jetStream) DeleteStream(ctx context.Context, name string) error {
	_ = "STUB: not implemented"
	return nil
}

func (js *jetStream) CreateOrUpdateConsumer(ctx context.Context, stream string, cfg ConsumerConfig) (Consumer, error) {
	_ = "STUB: not implemented"
	return *new(Consumer), nil
}

func (js *jetStream) CreateConsumer(ctx context.Context, stream string, cfg ConsumerConfig) (Consumer, error) {
	_ = "STUB: not implemented"
	return *new(Consumer), nil
}

func (js *jetStream) UpdateConsumer(ctx context.Context, stream string, cfg ConsumerConfig) (Consumer, error) {
	_ = "STUB: not implemented"
	return *new(Consumer), nil
}

func (js *jetStream) OrderedConsumer(ctx context.Context, stream string, cfg OrderedConsumerConfig) (Consumer, error) {
	_ = "STUB: not implemented"
	return *new(Consumer), nil
}

func (js *jetStream) Consumer(ctx context.Context, stream string, name string) (Consumer, error) {
	_ = "STUB: not implemented"
	return *new(Consumer), nil
}

func (js *jetStream) DeleteConsumer(ctx context.Context, stream string, name string) error {
	_ = "STUB: not implemented"
	return nil
}

func (js *jetStream) CreateOrUpdatePushConsumer(ctx context.Context, stream string, cfg ConsumerConfig) (PushConsumer, error) {
	_ = "STUB: not implemented"
	return *new(PushConsumer), nil
}

func (js *jetStream) CreatePushConsumer(ctx context.Context, stream string, cfg ConsumerConfig) (PushConsumer, error) {
	_ = "STUB: not implemented"
	return *new(PushConsumer), nil
}

func (js *jetStream) UpdatePushConsumer(ctx context.Context, stream string, cfg ConsumerConfig) (PushConsumer, error) {
	_ = "STUB: not implemented"
	return *new(PushConsumer), nil
}

func (js *jetStream) PushConsumer(ctx context.Context, stream string, name string) (PushConsumer, error) {
	_ = "STUB: not implemented"
	return *new(PushConsumer), nil
}

func (js *jetStream) PauseConsumer(ctx context.Context, stream string, consumer string, pauseUntil time.Time) (*ConsumerPauseResponse, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func (js *jetStream) ResumeConsumer(ctx context.Context, stream string, consumer string) (*ConsumerPauseResponse, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func (js *jetStream) ResetConsumer(ctx context.Context, stream, consumer string) (*ConsumerResetResponse, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func (js *jetStream) ResetConsumerToSequence(ctx context.Context, stream, consumer string, seq uint64) (*ConsumerResetResponse, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func validateStreamName(stream string) error { _ = "STUB: not implemented"; return nil }

func validateSubject(subject string) error { _ = "STUB: not implemented"; return nil }

func (js *jetStream) AccountInfo(ctx context.Context) (*AccountInfo, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func (js *jetStream) ListStreams(ctx context.Context, opts ...StreamListOpt) StreamInfoLister {
	_ = "STUB: not implemented"
	return *new(StreamInfoLister)
}

func (s *streamLister) Info() <-chan *StreamInfo { _ = "STUB: not implemented"; return nil }

func (s *streamLister) Err() error { _ = "STUB: not implemented"; return nil }

func (js *jetStream) StreamNames(ctx context.Context, opts ...StreamListOpt) StreamNameLister {
	_ = "STUB: not implemented"
	return *new(StreamNameLister)
}

func (js *jetStream) StreamNameBySubject(ctx context.Context, subject string) (string, error) {
	_ = "STUB: not implemented"
	return "", nil
}

func (s *streamLister) Name() <-chan string { _ = "STUB: not implemented"; return nil }

func (s *streamLister) streamInfos(ctx context.Context, streamsReq streamsRequest) ([]*StreamInfo, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func (s *streamLister) streamNames(ctx context.Context, streamsReq streamsRequest) ([]string, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func (js *jetStream) wrapContextWithoutDeadline(ctx context.Context) (context.Context, context.CancelFunc) {
	_ = "STUB: not implemented"
	return *new(context.Context), *new(context.CancelFunc)
}

func (js *jetStream) CleanupPublisher() { _ = "STUB: not implemented"; return }

func (js *jetStream) cleanupReplySub() { _ = "STUB: not implemented"; return }
