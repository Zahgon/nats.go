package jetstream

import (
	"context"
	"time"
)

type (
	Consumer interface {
		Fetch(batch int, opts ...FetchOpt) (MessageBatch, error)

		FetchBytes(maxBytes int, opts ...FetchOpt) (MessageBatch, error)

		FetchNoWait(batch int) (MessageBatch, error)

		Consume(handler MessageHandler, opts ...PullConsumeOpt) (ConsumeContext, error)

		Messages(opts ...PullMessagesOpt) (MessagesContext, error)

		Next(opts ...FetchOpt) (Msg, error)

		Info(context.Context) (*ConsumerInfo, error)

		CachedInfo() *ConsumerInfo
	}

	PushConsumer interface {
		Consume(handler MessageHandler, opts ...PushConsumeOpt) (ConsumeContext, error)

		Info(context.Context) (*ConsumerInfo, error)

		CachedInfo() *ConsumerInfo
	}

	createConsumerRequest struct {
		Stream string          `json:"stream_name"`
		Config *ConsumerConfig `json:"config"`
		Action string          `json:"action"`
	}
)

func (p *pullConsumer) Info(ctx context.Context) (*ConsumerInfo, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func (p *pullConsumer) CachedInfo() *ConsumerInfo { _ = "STUB: not implemented"; return nil }

func (p *pushConsumer) Info(ctx context.Context) (*ConsumerInfo, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func (p *pushConsumer) CachedInfo() *ConsumerInfo { _ = "STUB: not implemented"; return nil }

func upsertPullConsumer(ctx context.Context, js *jetStream, stream string, cfg ConsumerConfig, action string) (Consumer, error) {
	_ = "STUB: not implemented"
	return *new(Consumer), nil
}

func upsertPushConsumer(ctx context.Context, js *jetStream, stream string, cfg ConsumerConfig, action string) (PushConsumer, error) {
	_ = "STUB: not implemented"
	return *new(PushConsumer), nil
}

func upsertConsumer(ctx context.Context, js *jetStream, stream string, cfg ConsumerConfig, action string) (*consumerInfoResponse, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

const (
	consumerActionCreate         = "create"
	consumerActionUpdate         = "update"
	consumerActionCreateOrUpdate = ""
)

func generateConsName() string { _ = "STUB: not implemented"; return "" }

func getConsumer(ctx context.Context, js *jetStream, stream, name string) (Consumer, error) {
	_ = "STUB: not implemented"
	return *new(Consumer), nil
}

func getPushConsumer(ctx context.Context, js *jetStream, stream, name string) (PushConsumer, error) {
	_ = "STUB: not implemented"
	return *new(PushConsumer), nil
}

func fetchConsumerInfo(ctx context.Context, js *jetStream, stream, name string) (*ConsumerInfo, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func deleteConsumer(ctx context.Context, js *jetStream, stream, consumer string) error {
	_ = "STUB: not implemented"
	return nil
}

func pauseConsumer(ctx context.Context, js *jetStream, stream, consumer string, pauseUntil *time.Time) (*ConsumerPauseResponse, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func resumeConsumer(ctx context.Context, js *jetStream, stream, consumer string) (*ConsumerPauseResponse, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func resetConsumer(ctx context.Context, js *jetStream, stream, consumer string, seq uint64) (*ConsumerResetResponse, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func validateConsumerName(name string) error { _ = "STUB: not implemented"; return nil }

func unpinConsumer(ctx context.Context, js *jetStream, stream, consumer, group string) error {
	_ = "STUB: not implemented"
	return nil
}
