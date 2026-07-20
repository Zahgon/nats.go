package jetstream

import (
	"context"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
)

type (
	Msg interface {
		Metadata() (*MsgMetadata, error)

		Data() []byte

		Headers() nats.Header

		Subject() string

		Reply() string

		Ack() error

		DoubleAck(context.Context) error

		Nak() error

		NakWithDelay(delay time.Duration) error

		InProgress() error

		Term() error

		TermWithReason(reason string) error
	}

	MsgMetadata struct {
		Sequence SequencePair

		NumDelivered uint64

		NumPending uint64

		Timestamp time.Time

		Stream string

		Consumer string

		Domain string
	}

	SequencePair struct {
		Consumer uint64 `json:"consumer_seq"`

		Stream uint64 `json:"stream_seq"`
	}

	jetStreamMsg struct {
		msg  *nats.Msg
		ackd bool
		js   *jetStream
		sync.Mutex
	}

	ackOpts struct {
		nakDelay   time.Duration
		termReason string
	}

	ackType []byte
)

const (
	statusControlMsg    = "100"
	statusBadRequest    = "400"
	statusNoMsgs        = "404"
	statusTimeout       = "408"
	statusConflict      = "409"
	statusNoResponders  = "503"
	statusPinIdMismatch = "423"

	fcRequestDescr     = "flowcontrol request"
	idleHeartbeatDescr = "idle heartbeat"
	consumerDeleted    = "consumer deleted"
	leadershipChange   = "leadership change"
	maxBytesExceeded   = "message size exceeds maxbytes"
	batchCompleted     = "batch completed"
	serverShutdown     = "server shutdown"
)

const (
	MsgIDHeader = "Nats-Msg-Id"

	ExpectedStreamHeader = "Nats-Expected-Stream"

	ExpectedLastSeqHeader = "Nats-Expected-Last-Sequence"

	ExpectedLastSubjSeqHeader = "Nats-Expected-Last-Subject-Sequence"

	ExpectedLastSubjSeqSubjHeader = "Nats-Expected-Last-Subject-Sequence-Subject"

	ExpectedLastMsgIDHeader = "Nats-Expected-Last-Msg-Id"

	MsgTTLHeader = "Nats-TTL"

	MsgRollup = "Nats-Rollup"

	MarkerReasonHeader = "Nats-Marker-Reason"

	ScheduleHeader = "Nats-Schedule"

	ScheduleTargetHeader = "Nats-Schedule-Target"

	ScheduleSourceHeader = "Nats-Schedule-Source"

	ScheduleTTLHeader = "Nats-Schedule-TTL"

	ScheduleTimeZoneHeader = "Nats-Schedule-Time-Zone"
)

const (
	StreamHeader = "Nats-Stream"

	SequenceHeader = "Nats-Sequence"

	TimeStampHeaer = "Nats-Time-Stamp"

	SubjectHeader = "Nats-Subject"

	SchedulerHeader = "Nats-Scheduler"

	ScheduleNextHeader = "Nats-Schedule-Next"

	LastSequenceHeader = "Nats-Last-Sequence"
)

const (
	MsgRollupSubject = "sub"

	MsgRollupAll = "all"
)

var (
	ackAck      ackType = []byte("+ACK")
	ackNak      ackType = []byte("-NAK")
	ackProgress ackType = []byte("+WPI")
	ackTerm     ackType = []byte("+TERM")
)

func (m *jetStreamMsg) Metadata() (*MsgMetadata, error) { _ = "STUB: not implemented"; return nil, nil }

func (m *jetStreamMsg) Data() []byte { _ = "STUB: not implemented"; return nil }

func (m *jetStreamMsg) Headers() nats.Header { _ = "STUB: not implemented"; return *new(nats.Header) }

func (m *jetStreamMsg) Subject() string { _ = "STUB: not implemented"; return "" }

func (m *jetStreamMsg) Reply() string { _ = "STUB: not implemented"; return "" }

func (m *jetStreamMsg) Ack() error { _ = "STUB: not implemented"; return nil }

func (m *jetStreamMsg) DoubleAck(ctx context.Context) error { _ = "STUB: not implemented"; return nil }

func (m *jetStreamMsg) Nak() error { _ = "STUB: not implemented"; return nil }

func (m *jetStreamMsg) NakWithDelay(delay time.Duration) error {
	_ = "STUB: not implemented"
	return nil
}

func (m *jetStreamMsg) InProgress() error { _ = "STUB: not implemented"; return nil }

func (m *jetStreamMsg) Term() error { _ = "STUB: not implemented"; return nil }

func (m *jetStreamMsg) TermWithReason(reason string) error { _ = "STUB: not implemented"; return nil }

func (m *jetStreamMsg) ackReply(ctx context.Context, ackType ackType, sync bool, opts ackOpts) error {
	_ = "STUB: not implemented"
	return nil
}

func (m *jetStreamMsg) checkReply() error { _ = "STUB: not implemented"; return nil }

func checkMsg(msg *nats.Msg) (bool, error) { _ = "STUB: not implemented"; return false, nil }

func parsePending(msg *nats.Msg) (int, int, error) { _ = "STUB: not implemented"; return 0, 0, nil }

func (js *jetStream) toJSMsg(msg *nats.Msg) *jetStreamMsg { _ = "STUB: not implemented"; return nil }
