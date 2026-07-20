package jetstream

import (
	"context"
	"time"
)

type pullOptFunc func(*consumeOpts) error

func (fn pullOptFunc) configureConsume(opts *consumeOpts) error {
	_ = "STUB: not implemented"
	return nil
}

func (fn pullOptFunc) configureMessages(opts *consumeOpts) error {
	_ = "STUB: not implemented"
	return nil
}

func WithClientTrace(ct *ClientTrace) JetStreamOpt {
	_ = "STUB: not implemented"
	return *new(JetStreamOpt)
}

func WithPublishAsyncErrHandler(cb MsgErrHandler) JetStreamOpt {
	_ = "STUB: not implemented"
	return *new(JetStreamOpt)
}

func WithPublishAsyncMaxPending(max int) JetStreamOpt {
	_ = "STUB: not implemented"
	return *new(JetStreamOpt)
}

func WithPublishAsyncTimeout(dur time.Duration) JetStreamOpt {
	_ = "STUB: not implemented"
	return *new(JetStreamOpt)
}

func WithDefaultTimeout(timeout time.Duration) JetStreamOpt {
	_ = "STUB: not implemented"
	return *new(JetStreamOpt)
}

func WithPurgeSubject(subject string) StreamPurgeOpt {
	_ = "STUB: not implemented"
	return *new(StreamPurgeOpt)
}

func WithPurgeSequence(sequence uint64) StreamPurgeOpt {
	_ = "STUB: not implemented"
	return *new(StreamPurgeOpt)
}

func WithPurgeKeep(keep uint64) StreamPurgeOpt {
	_ = "STUB: not implemented"
	return *new(StreamPurgeOpt)
}

func WithGetMsgSubject(subject string) GetMsgOpt { _ = "STUB: not implemented"; return *new(GetMsgOpt) }

type PullMaxMessages int

func (max PullMaxMessages) configureConsume(opts *consumeOpts) error {
	_ = "STUB: not implemented"
	return nil
}

func (max PullMaxMessages) configureMessages(opts *consumeOpts) error {
	_ = "STUB: not implemented"
	return nil
}

type pullMaxMessagesWithBytesLimit struct {
	maxMessages int
	maxBytes    int
}

func PullMaxMessagesWithBytesLimit(maxMessages, byteLimit int) pullMaxMessagesWithBytesLimit {
	_ = "STUB: not implemented"
	return *new(pullMaxMessagesWithBytesLimit)
}

func (m pullMaxMessagesWithBytesLimit) configureConsume(opts *consumeOpts) error {
	_ = "STUB: not implemented"
	return nil
}

func (m pullMaxMessagesWithBytesLimit) configureMessages(opts *consumeOpts) error {
	_ = "STUB: not implemented"
	return nil
}

type PullExpiry time.Duration

func (exp PullExpiry) configureConsume(opts *consumeOpts) error {
	_ = "STUB: not implemented"
	return nil
}

func (exp PullExpiry) configureMessages(opts *consumeOpts) error {
	_ = "STUB: not implemented"
	return nil
}

type PullMaxBytes int

func (max PullMaxBytes) configureConsume(opts *consumeOpts) error {
	_ = "STUB: not implemented"
	return nil
}

func (max PullMaxBytes) configureMessages(opts *consumeOpts) error {
	_ = "STUB: not implemented"
	return nil
}

type PullThresholdMessages int

func (t PullThresholdMessages) configureConsume(opts *consumeOpts) error {
	_ = "STUB: not implemented"
	return nil
}

func (t PullThresholdMessages) configureMessages(opts *consumeOpts) error {
	_ = "STUB: not implemented"
	return nil
}

type PullThresholdBytes int

func (t PullThresholdBytes) configureConsume(opts *consumeOpts) error {
	_ = "STUB: not implemented"
	return nil
}

func (t PullThresholdBytes) configureMessages(opts *consumeOpts) error {
	_ = "STUB: not implemented"
	return nil
}

type PullMinPending int

func (min PullMinPending) configureConsume(opts *consumeOpts) error {
	_ = "STUB: not implemented"
	return nil
}

func (min PullMinPending) configureMessages(opts *consumeOpts) error {
	_ = "STUB: not implemented"
	return nil
}

type PullMinAckPending int

func (min PullMinAckPending) configureConsume(opts *consumeOpts) error {
	_ = "STUB: not implemented"
	return nil
}

func (min PullMinAckPending) configureMessages(opts *consumeOpts) error {
	_ = "STUB: not implemented"
	return nil
}

type PullPrioritized uint8

func (p PullPrioritized) configureConsume(opts *consumeOpts) error {
	_ = "STUB: not implemented"
	return nil
}

func (p PullPrioritized) configureMessages(opts *consumeOpts) error {
	_ = "STUB: not implemented"
	return nil
}

type PullPriorityGroup string

func (g PullPriorityGroup) configureConsume(opts *consumeOpts) error {
	_ = "STUB: not implemented"
	return nil
}

func (g PullPriorityGroup) configureMessages(opts *consumeOpts) error {
	_ = "STUB: not implemented"
	return nil
}

type PullHeartbeat time.Duration

func (hb PullHeartbeat) configureConsume(opts *consumeOpts) error {
	_ = "STUB: not implemented"
	return nil
}

func (hb PullHeartbeat) configureMessages(opts *consumeOpts) error {
	_ = "STUB: not implemented"
	return nil
}

type StopAfter int

func (nMsgs StopAfter) configureConsume(opts *consumeOpts) error {
	_ = "STUB: not implemented"
	return nil
}

func (nMsgs StopAfter) configureMessages(opts *consumeOpts) error {
	_ = "STUB: not implemented"
	return nil
}

type ConsumeErrHandler ConsumeErrHandlerFunc

func (c ConsumeErrHandler) configureConsume(opts *consumeOpts) error {
	_ = "STUB: not implemented"
	return nil
}

func (c ConsumeErrHandler) configurePushConsume(opts *pushConsumeOpts) error {
	_ = "STUB: not implemented"
	return nil
}

func WithMessagesErrOnMissingHeartbeat(hbErr bool) PullMessagesOpt {
	_ = "STUB: not implemented"
	return *new(PullMessagesOpt)
}

func FetchMinPending(min int64) FetchOpt { _ = "STUB: not implemented"; return *new(FetchOpt) }

func FetchMinAckPending(min int64) FetchOpt { _ = "STUB: not implemented"; return *new(FetchOpt) }

func FetchPrioritized(priority uint8) FetchOpt { _ = "STUB: not implemented"; return *new(FetchOpt) }

func FetchPriorityGroup(group string) FetchOpt { _ = "STUB: not implemented"; return *new(FetchOpt) }

func FetchMaxWait(timeout time.Duration) FetchOpt { _ = "STUB: not implemented"; return *new(FetchOpt) }

func FetchHeartbeat(hb time.Duration) FetchOpt { _ = "STUB: not implemented"; return *new(FetchOpt) }

func FetchContext(ctx context.Context) FetchOpt { _ = "STUB: not implemented"; return *new(FetchOpt) }

func WithDeletedDetails(deletedDetails bool) StreamInfoOpt {
	_ = "STUB: not implemented"
	return *new(StreamInfoOpt)
}

func WithSubjectFilter(subject string) StreamInfoOpt {
	_ = "STUB: not implemented"
	return *new(StreamInfoOpt)
}

func WithStreamListSubject(subject string) StreamListOpt {
	_ = "STUB: not implemented"
	return *new(StreamListOpt)
}

func WithMsgID(id string) PublishOpt { _ = "STUB: not implemented"; return *new(PublishOpt) }

func WithMsgTTL(dur time.Duration) PublishOpt { _ = "STUB: not implemented"; return *new(PublishOpt) }

func WithExpectStream(stream string) PublishOpt { _ = "STUB: not implemented"; return *new(PublishOpt) }

func WithExpectLastSequence(seq uint64) PublishOpt {
	_ = "STUB: not implemented"
	return *new(PublishOpt)
}

func WithExpectLastSequencePerSubject(seq uint64) PublishOpt {
	_ = "STUB: not implemented"
	return *new(PublishOpt)
}

func WithExpectLastSequenceForSubject(seq uint64, subject string) PublishOpt {
	_ = "STUB: not implemented"
	return *new(PublishOpt)
}

func WithExpectLastMsgID(id string) PublishOpt { _ = "STUB: not implemented"; return *new(PublishOpt) }

func WithRetryWait(dur time.Duration) PublishOpt {
	_ = "STUB: not implemented"
	return *new(PublishOpt)
}

func WithRetryAttempts(num int) PublishOpt { _ = "STUB: not implemented"; return *new(PublishOpt) }

func WithStallWait(ttl time.Duration) PublishOpt {
	_ = "STUB: not implemented"
	return *new(PublishOpt)
}

const (
	ScheduleYearly  = "@yearly"
	ScheduleMonthly = "@monthly"
	ScheduleWeekly  = "@weekly"
	ScheduleDaily   = "@daily"
	ScheduleHourly  = "@hourly"
)

func WithScheduleAt(t time.Time) PublishOpt { _ = "STUB: not implemented"; return *new(PublishOpt) }

func WithScheduleEvery(d time.Duration) PublishOpt {
	_ = "STUB: not implemented"
	return *new(PublishOpt)
}

func WithScheduleCron(expr string) PublishOpt { _ = "STUB: not implemented"; return *new(PublishOpt) }

func WithScheduleTarget(subject string) PublishOpt {
	_ = "STUB: not implemented"
	return *new(PublishOpt)
}

func WithScheduleSource(subject string) PublishOpt {
	_ = "STUB: not implemented"
	return *new(PublishOpt)
}

func WithScheduleTTL(d time.Duration) PublishOpt {
	_ = "STUB: not implemented"
	return *new(PublishOpt)
}

func WithScheduleTTLNever() PublishOpt { _ = "STUB: not implemented"; return *new(PublishOpt) }

func WithScheduleTimeZone(zone string) PublishOpt {
	_ = "STUB: not implemented"
	return *new(PublishOpt)
}

type nextOptFunc func(*nextOpts)

func (fn nextOptFunc) configureNext(opts *nextOpts) { _ = "STUB: not implemented"; return }

func NextMaxWait(timeout time.Duration) NextOpt { _ = "STUB: not implemented"; return *new(NextOpt) }

func NextContext(ctx context.Context) NextOpt { _ = "STUB: not implemented"; return *new(NextOpt) }
