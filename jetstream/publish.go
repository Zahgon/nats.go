package jetstream

import (
	"context"
	"math/rand"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
)

type (
	asyncPublisherOpts struct {
		aecb MsgErrHandler

		maxpa int

		ackTimeout time.Duration
	}

	PublishOpt func(*pubOpts) error

	pubOpts struct {
		id             string
		lastMsgID      string
		stream         string
		lastSeq        *uint64
		lastSubjectSeq *uint64
		lastSubject    string
		ttl            time.Duration
		schedule       string
		scheduleTarget string
		scheduleSource string
		scheduleTTL    string
		scheduleTZ     string

		retryWait     time.Duration
		retryAttempts int

		stallWait time.Duration

		pafRetry *pubAckFuture
	}

	PubAckFuture interface {
		Ok() <-chan *PubAck

		Err() <-chan error

		Msg() *nats.Msg
	}

	pubAckFuture struct {
		jsClient   *jetStreamClient
		msg        *nats.Msg
		retries    int
		maxRetries int
		retryWait  time.Duration
		ack        *PubAck
		err        error
		errCh      chan error
		doneCh     chan *PubAck
		reply      string
		timeout    *time.Timer
	}

	jetStreamClient struct {
		asyncPublishContext
		asyncPublisherOpts
	}

	MsgErrHandler func(JetStream, *nats.Msg, error)

	asyncPublishContext struct {
		sync.RWMutex
		replyPrefix string
		replySub    *nats.Subscription
		acks        map[string]*pubAckFuture
		stallCh     chan struct{}
		doneCh      chan struct{}
		rr          *rand.Rand

		connStatusCh chan (nats.Status)
	}

	pubAckResponse struct {
		apiResponse
		*PubAck
	}

	PubAck struct {
		Stream string `json:"stream"`

		Sequence uint64 `json:"seq"`

		Duplicate bool `json:"duplicate,omitempty"`

		Domain string `json:"domain,omitempty"`

		Value string `json:"val,omitempty"`
	}
)

const (
	DefaultPubRetryWait = 250 * time.Millisecond

	DefaultPubRetryAttempts = 2
)

const (
	statusHdr = "Status"

	rdigits = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
	base    = 62
)

func (js *jetStream) Publish(ctx context.Context, subj string, data []byte, opts ...PublishOpt) (*PubAck, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func (js *jetStream) PublishMsg(ctx context.Context, m *nats.Msg, opts ...PublishOpt) (*PubAck, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func (js *jetStream) PublishAsync(subj string, data []byte, opts ...PublishOpt) (PubAckFuture, error) {
	_ = "STUB: not implemented"
	return *new(PubAckFuture), nil
}

func (js *jetStream) PublishMsgAsync(m *nats.Msg, opts ...PublishOpt) (PubAckFuture, error) {
	_ = "STUB: not implemented"
	return *new(PubAckFuture), nil
}

const (
	aReplyTokensize = 6
)

func (js *jetStream) newAsyncReply() (string, error) { _ = "STUB: not implemented"; return "", nil }

func (js *jetStream) handleAsyncReply(m *nats.Msg) { _ = "STUB: not implemented"; return }

func (js *jetStream) resetPendingAcksOnReconnect() { _ = "STUB: not implemented"; return }

func (js *jetStream) registerPAF(id string, paf *pubAckFuture) (int, int) {
	_ = "STUB: not implemented"
	return 0, 0
}

func (js *jetStream) getPAF(id string) *pubAckFuture { _ = "STUB: not implemented"; return nil }

func (js *jetStream) clearPAF(id string) { _ = "STUB: not implemented"; return }

func (js *jetStream) asyncStall() <-chan struct{} { _ = "STUB: not implemented"; return nil }

func (paf *pubAckFuture) Ok() <-chan *PubAck { _ = "STUB: not implemented"; return nil }

func (paf *pubAckFuture) Err() <-chan error { _ = "STUB: not implemented"; return nil }

func (paf *pubAckFuture) Msg() *nats.Msg { _ = "STUB: not implemented"; return nil }

func (js *jetStream) PublishAsyncPending() int { _ = "STUB: not implemented"; return 0 }

func (js *jetStream) PublishAsyncComplete() <-chan struct{} { _ = "STUB: not implemented"; return nil }
