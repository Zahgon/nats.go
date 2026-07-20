package jetstream

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/internal/syncx"
)

type (
	MessagesContext interface {
		Next(opts ...NextOpt) (Msg, error)

		Stop()

		Drain()
	}

	ConsumeContext interface {
		Stop()

		Drain()

		Closed() <-chan struct{}
	}

	MessageHandler func(msg Msg)

	PullConsumeOpt interface {
		configureConsume(*consumeOpts) error
	}

	PullMessagesOpt interface {
		configureMessages(*consumeOpts) error
	}

	pullConsumer struct {
		sync.Mutex
		js      *jetStream
		stream  string
		durable bool
		name    string
		info    *ConsumerInfo
		subs    syncx.Map[string, *pullSubscription]
		pinID   string
	}

	pullRequest struct {
		Expires       time.Duration   `json:"expires,omitempty"`
		Batch         int             `json:"batch,omitempty"`
		MaxBytes      int             `json:"max_bytes,omitempty"`
		NoWait        bool            `json:"no_wait,omitempty"`
		Heartbeat     time.Duration   `json:"idle_heartbeat,omitempty"`
		MinPending    int64           `json:"min_pending,omitempty"`
		MinAckPending int64           `json:"min_ack_pending,omitempty"`
		PinID         string          `json:"id,omitempty"`
		Group         string          `json:"group,omitempty"`
		Priority      uint8           `json:"priority,omitempty"`
		ctx           context.Context `json:"-"`
		maxWaitSet    bool            `json:"-"`
	}

	consumeOpts struct {
		Expires                 time.Duration
		MaxMessages             int
		MaxBytes                int
		LimitSize               bool
		MinPending              int64
		MinAckPending           int64
		Priority                uint8
		Group                   string
		Heartbeat               time.Duration
		ErrHandler              ConsumeErrHandler
		ReportMissingHeartbeats bool
		ThresholdMessages       int
		ThresholdBytes          int
		StopAfter               int
		stopAfterMsgsLeft       chan int
		notifyOnReconnect       bool
	}

	ConsumeErrHandlerFunc func(consumeCtx ConsumeContext, err error)

	pullSubscription struct {
		sync.Mutex
		id                string
		consumer          *pullConsumer
		subscription      *nats.Subscription
		msgs              chan *nats.Msg
		msgsClosed        atomic.Uint32
		errs              chan error
		pending           pendingMsgs
		hbMonitor         *hbMonitor
		fetchInProgress   atomic.Uint32
		closed            atomic.Uint32
		draining          atomic.Uint32
		done              chan struct{}
		connStatusChanged chan nats.Status
		fetchNext         chan *pullRequest
		consumeOpts       *consumeOpts
		delivered         int
		closedCh          chan struct{}
	}

	pendingMsgs struct {
		msgCount  int
		byteCount int
	}

	MessageBatch interface {
		Messages() <-chan Msg
		Error() error
	}

	fetchResult struct {
		sync.Mutex
		msgs chan Msg
		err  error
		done bool
		sseq uint64
	}

	FetchOpt func(*pullRequest) error

	hbMonitor struct {
		timer *time.Timer
		sync.Mutex
	}

	NextOpt interface {
		configureNext(*nextOpts)
	}

	nextOpts struct {
		timeout time.Duration
		ctx     context.Context
	}
)

const (
	DefaultMaxMessages       = 500
	DefaultExpires           = 30 * time.Second
	defaultBatchMaxBytesOnly = 1_000_000
	unset                    = -1
)

func (p *pullConsumer) Consume(handler MessageHandler, opts ...PullConsumeOpt) (ConsumeContext, error) {
	_ = "STUB: not implemented"
	return *new(ConsumeContext), nil
}

func (s *pullSubscription) resetPendingMsgs() { _ = "STUB: not implemented"; return }

func (s *pullSubscription) decrementPendingMsgs(msg *nats.Msg) { _ = "STUB: not implemented"; return }

func (s *pullSubscription) incrementDeliveredMsgs() { _ = "STUB: not implemented"; return }

func (s *pullSubscription) checkPending() { _ = "STUB: not implemented"; return }

func (p *pullConsumer) Messages(opts ...PullMessagesOpt) (MessagesContext, error) {
	_ = "STUB: not implemented"
	return *new(MessagesContext), nil
}

var (
	errConnected    = errors.New("connected")
	errDisconnected = errors.New("disconnected")
)

func (s *pullSubscription) Next(opts ...NextOpt) (Msg, error) {
	_ = "STUB: not implemented"
	return *new(Msg), nil
}

func (s *pullSubscription) handleStatusMsg(msg *nats.Msg, msgErr error) (error, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func (hb *hbMonitor) Stop() { _ = "STUB: not implemented"; return }

func (hb *hbMonitor) Reset(dur time.Duration) { _ = "STUB: not implemented"; return }

func (s *pullSubscription) Stop() { _ = "STUB: not implemented"; return }

func (s *pullSubscription) Drain() { _ = "STUB: not implemented"; return }

func (s *pullSubscription) Closed() <-chan struct{} { _ = "STUB: not implemented"; return nil }

func (p *pullConsumer) Fetch(batch int, opts ...FetchOpt) (MessageBatch, error) {
	_ = "STUB: not implemented"
	return *new(MessageBatch), nil
}

func (p *pullConsumer) FetchBytes(maxBytes int, opts ...FetchOpt) (MessageBatch, error) {
	_ = "STUB: not implemented"
	return *new(MessageBatch), nil
}

func (p *pullConsumer) FetchNoWait(batch int) (MessageBatch, error) {
	_ = "STUB: not implemented"
	return *new(MessageBatch), nil
}

func (p *pullConsumer) fetch(req *pullRequest) (MessageBatch, error) {
	_ = "STUB: not implemented"
	return *new(MessageBatch), nil
}

func (fr *fetchResult) Messages() <-chan Msg { _ = "STUB: not implemented"; return nil }

func (fr *fetchResult) Error() error { _ = "STUB: not implemented"; return nil }

func (fr *fetchResult) closed() bool { _ = "STUB: not implemented"; return false }

func (p *pullConsumer) Next(opts ...FetchOpt) (Msg, error) {
	_ = "STUB: not implemented"
	return *new(Msg), nil
}

func (s *pullSubscription) pullMessages(subject string) { _ = "STUB: not implemented"; return }

func (s *pullSubscription) closeMsgs() { _ = "STUB: not implemented"; return }

func (s *pullSubscription) scheduleHeartbeatCheck(dur time.Duration) *hbMonitor {
	_ = "STUB: not implemented"
	return nil
}

func (s *pullSubscription) cleanup() { _ = "STUB: not implemented"; return }

func (s *pullSubscription) pull(req *pullRequest, subject string) error {
	_ = "STUB: not implemented"
	return nil
}

func parseConsumeOpts(ordered bool, opts ...PullConsumeOpt) (*consumeOpts, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func parseMessagesOpts(ordered bool, opts ...PullMessagesOpt) (*consumeOpts, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func (consumeOpts *consumeOpts) setDefaults(ordered bool) error {
	_ = "STUB: not implemented"
	return nil
}

func (c *pullConsumer) getPinID() string { _ = "STUB: not implemented"; return "" }

func (c *pullConsumer) setPinID(pinID string) { _ = "STUB: not implemented"; return }
