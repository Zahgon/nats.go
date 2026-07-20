package jetstream

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

type (
	orderedConsumer struct {
		js                *jetStream
		cfg               *OrderedConsumerConfig
		stream            string
		currentConsumer   *pullConsumer
		currentSub        *pullSubscription
		cursor            cursor
		namePrefix        string
		serial            int
		consumerType      consumerType
		doReset           chan struct{}
		resetInProgress   atomic.Uint32
		userErrHandler    ConsumeErrHandler
		stopAfter         int
		stopAfterMsgsLeft chan int
		withStopAfter     bool
		runningFetch      *fetchResult
		subscription      *orderedSubscription
		sync.Mutex
	}

	orderedSubscription struct {
		consumer *orderedConsumer
		opts     []PullMessagesOpt
		done     chan struct{}
		closed   atomic.Uint32
	}

	cursor struct {
		streamSeq  uint64
		deliverSeq uint64
	}

	consumerType int
)

const (
	consumerTypeNotSet consumerType = iota
	consumerTypeConsume
	consumerTypeFetch
)

var (
	errOrderedSequenceMismatch = errors.New("sequence mismatch")
	errOrderedConsumerClosed   = errors.New("ordered consumer closed")
)

func (c *orderedConsumer) Consume(handler MessageHandler, opts ...PullConsumeOpt) (ConsumeContext, error) {
	_ = "STUB: not implemented"
	return *new(ConsumeContext), nil
}

func (c *orderedConsumer) errHandler(serial int) func(cc ConsumeContext, err error) {
	_ = "STUB: not implemented"
	return nil
}

func (c *orderedConsumer) Messages(opts ...PullMessagesOpt) (MessagesContext, error) {
	_ = "STUB: not implemented"
	return *new(MessagesContext), nil
}

func (s *orderedSubscription) Next(opts ...NextOpt) (Msg, error) {
	_ = "STUB: not implemented"
	return *new(Msg), nil
}

func (s *orderedSubscription) Stop() { _ = "STUB: not implemented"; return }

func (s *orderedSubscription) Drain() { _ = "STUB: not implemented"; return }

func (s *orderedSubscription) Closed() <-chan struct{} { _ = "STUB: not implemented"; return nil }

func (c *orderedConsumer) Fetch(batch int, opts ...FetchOpt) (MessageBatch, error) {
	_ = "STUB: not implemented"
	return *new(MessageBatch), nil
}

func (c *orderedConsumer) FetchBytes(maxBytes int, opts ...FetchOpt) (MessageBatch, error) {
	_ = "STUB: not implemented"
	return *new(MessageBatch), nil
}

func (c *orderedConsumer) FetchNoWait(batch int) (MessageBatch, error) {
	_ = "STUB: not implemented"
	return *new(MessageBatch), nil
}

func (c *orderedConsumer) Next(opts ...FetchOpt) (Msg, error) {
	_ = "STUB: not implemented"
	return *new(Msg), nil
}

func serialNumberFromConsumer(name string) int { _ = "STUB: not implemented"; return 0 }

func (c *orderedConsumer) reset() error { _ = "STUB: not implemented"; return nil }

func (c *orderedConsumer) getConsumerConfig() *ConsumerConfig {
	_ = "STUB: not implemented"
	return nil
}

func consumeStopAfterNotify(numMsgs int, msgsLeftAfterStop chan int) PullConsumeOpt {
	_ = "STUB: not implemented"
	return *new(PullConsumeOpt)
}

func messagesStopAfterNotify(numMsgs int, msgsLeftAfterStop chan int) PullMessagesOpt {
	_ = "STUB: not implemented"
	return *new(PullMessagesOpt)
}

func consumeReconnectNotify() PullConsumeOpt {
	_ = "STUB: not implemented"
	return *new(PullConsumeOpt)
}

func messagesReconnectNotify() PullMessagesOpt {
	_ = "STUB: not implemented"
	return *new(PullMessagesOpt)
}

func (c *orderedConsumer) Info(ctx context.Context) (*ConsumerInfo, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func (c *orderedConsumer) CachedInfo() *ConsumerInfo { _ = "STUB: not implemented"; return nil }

type backoffOpts struct {
	attempts int

	initialInterval time.Duration

	disableInitialExecution bool

	factor float64

	maxInterval time.Duration

	customBackoff []time.Duration

	cancel <-chan struct{}
}

func retryWithBackoff(f func(int) (bool, error), opts backoffOpts) error {
	_ = "STUB: not implemented"
	return nil
}
