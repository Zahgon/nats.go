package jetstream

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
)

type (
	pushConsumer struct {
		sync.Mutex
		js      *jetStream
		stream  string
		name    string
		info    *ConsumerInfo
		started atomic.Bool
	}

	pushSubscription struct {
		sync.Mutex
		id                string
		errs              chan error
		subscription      *nats.Subscription
		connStatusChanged chan nats.Status
		closedCh          chan struct{}
		done              chan struct{}
		closed            atomic.Bool
		consumeOpts       *pushConsumeOpts
		hbMonitor         *hbMonitor
		idleHeartbeat     time.Duration
	}

	pushConsumeOpts struct {
		ErrHandler ConsumeErrHandler
	}

	PushConsumeOpt interface {
		configurePushConsume(*pushConsumeOpts) error
	}
)

func (p *pushConsumer) Consume(handler MessageHandler, opts ...PushConsumeOpt) (ConsumeContext, error) {
	_ = "STUB: not implemented"
	return *new(ConsumeContext), nil
}

func (s *pushSubscription) handleStatusMsg(msg *nats.Msg, status, description string) (error, bool) {
	_ = "STUB: not implemented"
	return nil, false
}

func (s *pushSubscription) Stop() { _ = "STUB: not implemented"; return }

func (s *pushSubscription) Drain() { _ = "STUB: not implemented"; return }

func (s *pushSubscription) Closed() <-chan struct{} { _ = "STUB: not implemented"; return nil }

func (s *pushSubscription) scheduleHeartbeatCheck(dur time.Duration) *hbMonitor {
	_ = "STUB: not implemented"
	return nil
}
