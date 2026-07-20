package nats

import (
	"context"
	"errors"
	"math/rand"
	"sync"
	"time"
)

type JetStream interface {
	Publish(subj string, data []byte, opts ...PubOpt) (*PubAck, error)

	PublishMsg(m *Msg, opts ...PubOpt) (*PubAck, error)

	PublishAsync(subj string, data []byte, opts ...PubOpt) (PubAckFuture, error)

	PublishMsgAsync(m *Msg, opts ...PubOpt) (PubAckFuture, error)

	PublishAsyncPending() int

	PublishAsyncComplete() <-chan struct{}

	CleanupPublisher()

	Subscribe(subj string, cb MsgHandler, opts ...SubOpt) (*Subscription, error)

	SubscribeSync(subj string, opts ...SubOpt) (*Subscription, error)

	ChanSubscribe(subj string, ch chan *Msg, opts ...SubOpt) (*Subscription, error)

	ChanQueueSubscribe(subj, queue string, ch chan *Msg, opts ...SubOpt) (*Subscription, error)

	QueueSubscribe(subj, queue string, cb MsgHandler, opts ...SubOpt) (*Subscription, error)

	QueueSubscribeSync(subj, queue string, opts ...SubOpt) (*Subscription, error)

	PullSubscribe(subj, durable string, opts ...SubOpt) (*Subscription, error)
}

type JetStreamContext interface {
	JetStream
	JetStreamManager
	KeyValueManager
	ObjectStoreManager
}

const (
	defaultAPIPrefix = "$JS.API."

	jsDomainT = "$JS.%s.API."

	jsExtDomainT = "$JS.%s.API"

	apiAccountInfo = "INFO"

	apiConsumerCreateT = "CONSUMER.CREATE.%s.%s"

	apiConsumerCreateWithFilterSubjectT = "CONSUMER.CREATE.%s.%s.%s"

	apiLegacyConsumerCreateT = "CONSUMER.CREATE.%s"

	apiDurableCreateT = "CONSUMER.DURABLE.CREATE.%s.%s"

	apiConsumerInfoT = "CONSUMER.INFO.%s.%s"

	apiRequestNextT = "CONSUMER.MSG.NEXT.%s.%s"

	apiConsumerDeleteT = "CONSUMER.DELETE.%s.%s"

	apiConsumerListT = "CONSUMER.LIST.%s"

	apiConsumerNamesT = "CONSUMER.NAMES.%s"

	apiStreams = "STREAM.NAMES"

	apiStreamCreateT = "STREAM.CREATE.%s"

	apiStreamInfoT = "STREAM.INFO.%s"

	apiStreamUpdateT = "STREAM.UPDATE.%s"

	apiStreamDeleteT = "STREAM.DELETE.%s"

	apiStreamPurgeT = "STREAM.PURGE.%s"

	apiStreamListT = "STREAM.LIST"

	apiMsgGetT = "STREAM.MSG.GET.%s"

	apiDirectMsgGetT = "DIRECT.GET.%s"

	apiDirectMsgGetLastBySubjectT = "DIRECT.GET.%s.%s"

	apiMsgDeleteT = "STREAM.MSG.DELETE.%s"

	orderedHeartbeatsInterval = 5 * time.Second

	hbcThresh = 2

	chanSubFCCheckInterval = 250 * time.Millisecond

	DefaultPubRetryWait = 250 * time.Millisecond

	DefaultPubRetryAttempts = 2

	defaultAsyncPubAckInflight = 4000
)

const (
	jsCtrlHB = 1
	jsCtrlFC = 2
)

type js struct {
	nc   *Conn
	opts *jsOpts

	mu             sync.RWMutex
	rpre           string
	rsub           *Subscription
	pafs           map[string]*pubAckFuture
	stc            chan struct{}
	dch            chan struct{}
	rr             *rand.Rand
	connStatusCh   chan (Status)
	replyPrefix    string
	replyPrefixLen int
}

type jsOpts struct {
	ctx context.Context

	pre string

	wait time.Duration

	aecb MsgErrHandler

	maxpa int

	ackTimeout time.Duration

	domain string

	ctrace      ClientTrace
	shouldTrace bool

	purgeOpts *StreamPurgeRequest

	streamInfoOpts *StreamInfoRequest

	streamListSubject string

	directGet bool

	directNextFor string

	featureFlags featureFlags
}

const (
	defaultRequestWait = 5 * time.Second
)

func (nc *Conn) JetStream(opts ...JSOpt) (JetStreamContext, error) {
	_ = "STUB: not implemented"
	return *new(JetStreamContext), nil
}

type JSOpt interface {
	configureJSContext(opts *jsOpts) error
}

type jsOptFn func(opts *jsOpts) error

func (opt jsOptFn) configureJSContext(opts *jsOpts) error { _ = "STUB: not implemented"; return nil }

type featureFlags struct {
	useDurableConsumerCreate bool
}

func UseLegacyDurableConsumers() JSOpt { _ = "STUB: not implemented"; return *new(JSOpt) }

type ClientTrace struct {
	RequestSent      func(subj string, payload []byte)
	ResponseReceived func(subj string, payload []byte, hdr Header)
}

func (ct ClientTrace) configureJSContext(js *jsOpts) error { _ = "STUB: not implemented"; return nil }

func Domain(domain string) JSOpt { _ = "STUB: not implemented"; return *new(JSOpt) }

func (s *StreamPurgeRequest) configureJSContext(js *jsOpts) error {
	_ = "STUB: not implemented"
	return nil
}

func (s *StreamInfoRequest) configureJSContext(js *jsOpts) error {
	_ = "STUB: not implemented"
	return nil
}

func APIPrefix(pre string) JSOpt { _ = "STUB: not implemented"; return *new(JSOpt) }

func DirectGet() JSOpt { _ = "STUB: not implemented"; return *new(JSOpt) }

func DirectGetNext(subject string) JSOpt { _ = "STUB: not implemented"; return *new(JSOpt) }

func StreamListFilter(subject string) JSOpt { _ = "STUB: not implemented"; return *new(JSOpt) }

func (js *js) apiSubj(subj string) string { _ = "STUB: not implemented"; return "" }

func apiSubjWithPrefix(pre, subj string) string { _ = "STUB: not implemented"; return "" }

func (o *jsOpts) apiSubj(subj string) string { _ = "STUB: not implemented"; return "" }

type PubOpt interface {
	configurePublish(opts *pubOpts) error
}

type pubOptFn func(opts *pubOpts) error

func (opt pubOptFn) configurePublish(opts *pubOpts) error { _ = "STUB: not implemented"; return nil }

type pubOpts struct {
	ctx    context.Context
	ttl    time.Duration
	id     string
	lid    string
	str    string
	seq    *uint64
	lss    *uint64
	msgTTL time.Duration

	rwait time.Duration
	rnum  int

	stallWait time.Duration

	pafRetry *pubAckFuture
}

type pubAckResponse struct {
	apiResponse
	*PubAck
}

type PubAck struct {
	Stream    string `json:"stream"`
	Sequence  uint64 `json:"seq"`
	Duplicate bool   `json:"duplicate,omitempty"`
	Domain    string `json:"domain,omitempty"`
}

const (
	MsgIdHdr               = "Nats-Msg-Id"
	ExpectedStreamHdr      = "Nats-Expected-Stream"
	ExpectedLastSeqHdr     = "Nats-Expected-Last-Sequence"
	ExpectedLastSubjSeqHdr = "Nats-Expected-Last-Subject-Sequence"
	ExpectedLastMsgIdHdr   = "Nats-Expected-Last-Msg-Id"
	MsgRollup              = "Nats-Rollup"
	MsgTTLHdr              = "Nats-TTL"
)

const (
	JSStream       = "Nats-Stream"
	JSSequence     = "Nats-Sequence"
	JSTimeStamp    = "Nats-Time-Stamp"
	JSSubject      = "Nats-Subject"
	JSLastSequence = "Nats-Last-Sequence"
)

const MsgSize = "Nats-Msg-Size"

const (
	MsgRollupSubject = "sub"
	MsgRollupAll     = "all"
)

func (js *js) PublishMsg(m *Msg, opts ...PubOpt) (*PubAck, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func (js *js) Publish(subj string, data []byte, opts ...PubOpt) (*PubAck, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

type PubAckFuture interface {
	Ok() <-chan *PubAck

	Err() <-chan error

	Msg() *Msg
}

type pubAckFuture struct {
	js         *js
	msg        *Msg
	pa         *PubAck
	st         time.Time
	err        error
	errCh      chan error
	doneCh     chan *PubAck
	retries    int
	maxRetries int
	retryWait  time.Duration
	reply      string
	timeout    *time.Timer
}

func (paf *pubAckFuture) Ok() <-chan *PubAck { _ = "STUB: not implemented"; return nil }

func (paf *pubAckFuture) Err() <-chan error { _ = "STUB: not implemented"; return nil }

func (paf *pubAckFuture) Msg() *Msg { _ = "STUB: not implemented"; return nil }

const aReplyTokensize = 6

func (js *js) newAsyncReply() string { _ = "STUB: not implemented"; return "" }

func (js *js) resetPendingAcksOnReconnect() { _ = "STUB: not implemented"; return }

func (js *js) CleanupPublisher() { _ = "STUB: not implemented"; return }

func (js *js) cleanupReplySub() { _ = "STUB: not implemented"; return }

func (js *js) registerPAF(id string, paf *pubAckFuture) (int, int) {
	_ = "STUB: not implemented"
	return 0, 0
}

func (js *js) getPAF(id string) *pubAckFuture { _ = "STUB: not implemented"; return nil }

func (js *js) clearPAF(id string) { _ = "STUB: not implemented"; return }

func (js *js) PublishAsyncPending() int { _ = "STUB: not implemented"; return 0 }

func (js *js) asyncStall() <-chan struct{} { _ = "STUB: not implemented"; return nil }

func (js *js) handleAsyncReply(m *Msg) { _ = "STUB: not implemented"; return }

type MsgErrHandler func(JetStream, *Msg, error)

func PublishAsyncErrHandler(cb MsgErrHandler) JSOpt { _ = "STUB: not implemented"; return *new(JSOpt) }

func PublishAsyncMaxPending(max int) JSOpt { _ = "STUB: not implemented"; return *new(JSOpt) }

func PublishAsyncTimeout(dur time.Duration) JSOpt { _ = "STUB: not implemented"; return *new(JSOpt) }

func (js *js) PublishAsync(subj string, data []byte, opts ...PubOpt) (PubAckFuture, error) {
	_ = "STUB: not implemented"
	return *new(PubAckFuture), nil
}

const defaultStallWait = 200 * time.Millisecond

func (js *js) PublishMsgAsync(m *Msg, opts ...PubOpt) (PubAckFuture, error) {
	_ = "STUB: not implemented"
	return *new(PubAckFuture), nil
}

func (js *js) PublishAsyncComplete() <-chan struct{} { _ = "STUB: not implemented"; return nil }

func MsgId(id string) PubOpt { _ = "STUB: not implemented"; return *new(PubOpt) }

func ExpectStream(stream string) PubOpt { _ = "STUB: not implemented"; return *new(PubOpt) }

func ExpectLastSequence(seq uint64) PubOpt { _ = "STUB: not implemented"; return *new(PubOpt) }

func ExpectLastSequencePerSubject(seq uint64) PubOpt {
	_ = "STUB: not implemented"
	return *new(PubOpt)
}

func ExpectLastMsgId(id string) PubOpt { _ = "STUB: not implemented"; return *new(PubOpt) }

func RetryWait(dur time.Duration) PubOpt { _ = "STUB: not implemented"; return *new(PubOpt) }

func RetryAttempts(num int) PubOpt { _ = "STUB: not implemented"; return *new(PubOpt) }

func StallWait(ttl time.Duration) PubOpt { _ = "STUB: not implemented"; return *new(PubOpt) }

func MsgTTL(dur time.Duration) PubOpt { _ = "STUB: not implemented"; return *new(PubOpt) }

type ackOpts struct {
	ttl      time.Duration
	ctx      context.Context
	nakDelay time.Duration
}

type AckOpt interface {
	configureAck(opts *ackOpts) error
}

type MaxWait time.Duration

func (ttl MaxWait) configureJSContext(js *jsOpts) error { _ = "STUB: not implemented"; return nil }

func (ttl MaxWait) configurePull(opts *pullOpts) error { _ = "STUB: not implemented"; return nil }

type AckWait time.Duration

func (ttl AckWait) configurePublish(opts *pubOpts) error { _ = "STUB: not implemented"; return nil }

func (ttl AckWait) configureSubscribe(opts *subOpts) error { _ = "STUB: not implemented"; return nil }

func (ttl AckWait) configureAck(opts *ackOpts) error { _ = "STUB: not implemented"; return nil }

type ContextOpt struct {
	context.Context
}

func (ctx ContextOpt) configureJSContext(opts *jsOpts) error { _ = "STUB: not implemented"; return nil }

func (ctx ContextOpt) configurePublish(opts *pubOpts) error { _ = "STUB: not implemented"; return nil }

func (ctx ContextOpt) configureSubscribe(opts *subOpts) error {
	_ = "STUB: not implemented"
	return nil
}

func (ctx ContextOpt) configurePull(opts *pullOpts) error { _ = "STUB: not implemented"; return nil }

func (ctx ContextOpt) configureAck(opts *ackOpts) error { _ = "STUB: not implemented"; return nil }

func Context(ctx context.Context) ContextOpt { _ = "STUB: not implemented"; return *new(ContextOpt) }

type nakDelay time.Duration

func (d nakDelay) configureAck(opts *ackOpts) error { _ = "STUB: not implemented"; return nil }

type ConsumerConfig struct {
	Durable         string          `json:"durable_name,omitempty"`
	Name            string          `json:"name,omitempty"`
	Description     string          `json:"description,omitempty"`
	DeliverPolicy   DeliverPolicy   `json:"deliver_policy"`
	OptStartSeq     uint64          `json:"opt_start_seq,omitempty"`
	OptStartTime    *time.Time      `json:"opt_start_time,omitempty"`
	AckPolicy       AckPolicy       `json:"ack_policy"`
	AckWait         time.Duration   `json:"ack_wait,omitempty"`
	MaxDeliver      int             `json:"max_deliver,omitempty"`
	BackOff         []time.Duration `json:"backoff,omitempty"`
	FilterSubject   string          `json:"filter_subject,omitempty"`
	FilterSubjects  []string        `json:"filter_subjects,omitempty"`
	ReplayPolicy    ReplayPolicy    `json:"replay_policy"`
	RateLimit       uint64          `json:"rate_limit_bps,omitempty"`
	SampleFrequency string          `json:"sample_freq,omitempty"`
	MaxWaiting      int             `json:"max_waiting,omitempty"`
	MaxAckPending   int             `json:"max_ack_pending,omitempty"`
	FlowControl     bool            `json:"flow_control,omitempty"`
	Heartbeat       time.Duration   `json:"idle_heartbeat,omitempty"`
	HeadersOnly     bool            `json:"headers_only,omitempty"`

	MaxRequestBatch    int           `json:"max_batch,omitempty"`
	MaxRequestExpires  time.Duration `json:"max_expires,omitempty"`
	MaxRequestMaxBytes int           `json:"max_bytes,omitempty"`

	DeliverSubject string `json:"deliver_subject,omitempty"`
	DeliverGroup   string `json:"deliver_group,omitempty"`

	InactiveThreshold time.Duration `json:"inactive_threshold,omitempty"`

	Replicas int `json:"num_replicas"`

	MemoryStorage bool `json:"mem_storage,omitempty"`

	Metadata map[string]string `json:"metadata,omitempty"`
}

type ConsumerInfo struct {
	Stream         string         `json:"stream_name"`
	Name           string         `json:"name"`
	Created        time.Time      `json:"created"`
	Config         ConsumerConfig `json:"config"`
	Delivered      SequenceInfo   `json:"delivered"`
	AckFloor       SequenceInfo   `json:"ack_floor"`
	NumAckPending  int            `json:"num_ack_pending"`
	NumRedelivered int            `json:"num_redelivered"`
	NumWaiting     int            `json:"num_waiting"`
	NumPending     uint64         `json:"num_pending"`
	Cluster        *ClusterInfo   `json:"cluster,omitempty"`
	PushBound      bool           `json:"push_bound,omitempty"`
}

type SequenceInfo struct {
	Consumer uint64     `json:"consumer_seq"`
	Stream   uint64     `json:"stream_seq"`
	Last     *time.Time `json:"last_active,omitempty"`
}

type SequencePair struct {
	Consumer uint64 `json:"consumer_seq"`
	Stream   uint64 `json:"stream_seq"`
}

type nextRequest struct {
	Expires   time.Duration `json:"expires,omitempty"`
	Batch     int           `json:"batch,omitempty"`
	NoWait    bool          `json:"no_wait,omitempty"`
	MaxBytes  int           `json:"max_bytes,omitempty"`
	Heartbeat time.Duration `json:"idle_heartbeat,omitempty"`
}

type jsSub struct {
	js *js

	nms string

	psubj    string
	consumer string
	stream   string
	deliver  string
	pull     bool
	dc       bool
	ackNone  bool

	pending uint64

	ordered bool
	dseq    uint64
	sseq    uint64
	ccreq   *createConsumerRequest

	hbc    *time.Timer
	hbi    time.Duration
	active bool
	cmeta  string
	fcr    string
	fcd    uint64
	fciseq uint64
	csfct  *time.Timer

	ctx context.Context

	cancel func()
}

func (sub *Subscription) deleteConsumer() error { _ = "STUB: not implemented"; return nil }

type SubOpt interface {
	configureSubscribe(opts *subOpts) error
}

type subOptFn func(opts *subOpts) error

func (opt subOptFn) configureSubscribe(opts *subOpts) error { _ = "STUB: not implemented"; return nil }

func (js *js) Subscribe(subj string, cb MsgHandler, opts ...SubOpt) (*Subscription, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func (js *js) SubscribeSync(subj string, opts ...SubOpt) (*Subscription, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func (js *js) QueueSubscribe(subj, queue string, cb MsgHandler, opts ...SubOpt) (*Subscription, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func (js *js) QueueSubscribeSync(subj, queue string, opts ...SubOpt) (*Subscription, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func (js *js) ChanSubscribe(subj string, ch chan *Msg, opts ...SubOpt) (*Subscription, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func (js *js) ChanQueueSubscribe(subj, queue string, ch chan *Msg, opts ...SubOpt) (*Subscription, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func (js *js) PullSubscribe(subj, durable string, opts ...SubOpt) (*Subscription, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func processConsInfo(info *ConsumerInfo, userCfg *ConsumerConfig, isPullMode bool, subj, queue string) (string, error) {
	_ = "STUB: not implemented"
	return "", nil
}

func checkConfig(s, u *ConsumerConfig) error { _ = "STUB: not implemented"; return nil }

func (js *js) subscribe(subj, queue string, cb MsgHandler, ch chan *Msg, isSync, isPullMode bool, opts []SubOpt) (*Subscription, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func (sub *Subscription) InitialConsumerPending() (uint64, error) {
	_ = "STUB: not implemented"
	return 0, nil
}

func (sub *Subscription) chanSubcheckForFlowControlResponse() { _ = "STUB: not implemented"; return }

type ErrConsumerSequenceMismatch struct {
	StreamResumeSequence uint64

	ConsumerSequence uint64

	LastConsumerSequence uint64
}

func (ecs *ErrConsumerSequenceMismatch) Error() string { _ = "STUB: not implemented"; return "" }

func isJSControlMessage(msg *Msg) (bool, int) { _ = "STUB: not implemented"; return false, 0 }

func (sub *Subscription) trackSequences(reply string) { _ = "STUB: not implemented"; return }

func (sub *Subscription) checkOrderedMsgs(m *Msg) bool { _ = "STUB: not implemented"; return false }

func (sub *Subscription) applyNewSID() (osid int64) { _ = "STUB: not implemented"; return 0 }

func (sub *Subscription) resetOrderedConsumer(sseq uint64) { _ = "STUB: not implemented"; return }

func (sub *Subscription) getJSDelivered() uint64 { _ = "STUB: not implemented"; return 0 }

func (sub *Subscription) checkForFlowControlResponse() string { _ = "STUB: not implemented"; return "" }

func (sub *Subscription) scheduleFlowControlResponse(reply string) {
	_ = "STUB: not implemented"
	return
}

func (sub *Subscription) activityCheck() { _ = "STUB: not implemented"; return }

func (sub *Subscription) scheduleHeartbeatCheck() { _ = "STUB: not implemented"; return }

func (nc *Conn) handleConsumerSequenceMismatch(sub *Subscription, err error) {
	_ = "STUB: not implemented"
	return
}

func (nc *Conn) checkForSequenceMismatch(msg *Msg, s *Subscription, jsi *jsSub) {
	_ = "STUB: not implemented"
	return
}

type streamRequest struct {
	Subject string `json:"subject,omitempty"`
}

type streamNamesResponse struct {
	apiResponse
	apiPaged
	Streams []string `json:"streams"`
}

type subOpts struct {
	stream, consumer string

	cfg *ConsumerConfig

	bound bool

	mack bool

	ordered bool
	ctx     context.Context

	skipCInfo bool
}

func SkipConsumerLookup() SubOpt { _ = "STUB: not implemented"; return *new(SubOpt) }

func OrderedConsumer() SubOpt { _ = "STUB: not implemented"; return *new(SubOpt) }

func ManualAck() SubOpt { _ = "STUB: not implemented"; return *new(SubOpt) }

func Description(description string) SubOpt { _ = "STUB: not implemented"; return *new(SubOpt) }

func Durable(consumer string) SubOpt { _ = "STUB: not implemented"; return *new(SubOpt) }

func DeliverAll() SubOpt { _ = "STUB: not implemented"; return *new(SubOpt) }

func DeliverLast() SubOpt { _ = "STUB: not implemented"; return *new(SubOpt) }

func DeliverLastPerSubject() SubOpt { _ = "STUB: not implemented"; return *new(SubOpt) }

func DeliverNew() SubOpt { _ = "STUB: not implemented"; return *new(SubOpt) }

func StartSequence(seq uint64) SubOpt { _ = "STUB: not implemented"; return *new(SubOpt) }

func StartTime(startTime time.Time) SubOpt { _ = "STUB: not implemented"; return *new(SubOpt) }

func AckNone() SubOpt { _ = "STUB: not implemented"; return *new(SubOpt) }

func AckAll() SubOpt { _ = "STUB: not implemented"; return *new(SubOpt) }

func AckExplicit() SubOpt { _ = "STUB: not implemented"; return *new(SubOpt) }

func MaxDeliver(n int) SubOpt { _ = "STUB: not implemented"; return *new(SubOpt) }

func MaxAckPending(n int) SubOpt { _ = "STUB: not implemented"; return *new(SubOpt) }

func ReplayOriginal() SubOpt { _ = "STUB: not implemented"; return *new(SubOpt) }

func ReplayInstant() SubOpt { _ = "STUB: not implemented"; return *new(SubOpt) }

func RateLimit(n uint64) SubOpt { _ = "STUB: not implemented"; return *new(SubOpt) }

func BackOff(backOff []time.Duration) SubOpt { _ = "STUB: not implemented"; return *new(SubOpt) }

func BindStream(stream string) SubOpt { _ = "STUB: not implemented"; return *new(SubOpt) }

func Bind(stream, consumer string) SubOpt { _ = "STUB: not implemented"; return *new(SubOpt) }

func EnableFlowControl() SubOpt { _ = "STUB: not implemented"; return *new(SubOpt) }

func IdleHeartbeat(duration time.Duration) SubOpt { _ = "STUB: not implemented"; return *new(SubOpt) }

func DeliverSubject(subject string) SubOpt { _ = "STUB: not implemented"; return *new(SubOpt) }

func HeadersOnly() SubOpt { _ = "STUB: not implemented"; return *new(SubOpt) }

func MaxRequestBatch(max int) SubOpt { _ = "STUB: not implemented"; return *new(SubOpt) }

func MaxRequestExpires(max time.Duration) SubOpt { _ = "STUB: not implemented"; return *new(SubOpt) }

func MaxRequestMaxBytes(bytes int) SubOpt { _ = "STUB: not implemented"; return *new(SubOpt) }

func InactiveThreshold(threshold time.Duration) SubOpt {
	_ = "STUB: not implemented"
	return *new(SubOpt)
}

func ConsumerReplicas(replicas int) SubOpt { _ = "STUB: not implemented"; return *new(SubOpt) }

func ConsumerMemoryStorage() SubOpt { _ = "STUB: not implemented"; return *new(SubOpt) }

func ConsumerName(name string) SubOpt { _ = "STUB: not implemented"; return *new(SubOpt) }

func ConsumerFilterSubjects(subjects ...string) SubOpt {
	_ = "STUB: not implemented"
	return *new(SubOpt)
}

func (sub *Subscription) ConsumerInfo() (*ConsumerInfo, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

type pullOpts struct {
	maxBytes int
	ttl      time.Duration
	ctx      context.Context
	hb       time.Duration
}

type PullOpt interface {
	configurePull(opts *pullOpts) error
}

func PullMaxWaiting(n int) SubOpt { _ = "STUB: not implemented"; return *new(SubOpt) }

type PullHeartbeat time.Duration

func (h PullHeartbeat) configurePull(opts *pullOpts) error { _ = "STUB: not implemented"; return nil }

type PullMaxBytes int

func (n PullMaxBytes) configurePull(opts *pullOpts) error { _ = "STUB: not implemented"; return nil }

var (
	errNoMessages = errors.New("nats: no messages")

	errRequestsPending = errors.New("nats: requests pending")
)

func checkMsg(msg *Msg, checkSts, isNoWait bool) (usrMsg bool, err error) {
	_ = "STUB: not implemented"
	return false, nil
}

func (sub *Subscription) Fetch(batch int, opts ...PullOpt) ([]*Msg, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func newFetchInbox(subj string) (string, string) { _ = "STUB: not implemented"; return "", "" }

func subjectMatchesReqID(subject, reqID string) bool { _ = "STUB: not implemented"; return false }

type MessageBatch interface {
	Messages() <-chan *Msg

	Error() error

	Done() <-chan struct{}
}

type messageBatch struct {
	sync.Mutex
	msgs chan *Msg
	err  error
	done chan struct{}
}

func (mb *messageBatch) Messages() <-chan *Msg { _ = "STUB: not implemented"; return nil }

func (mb *messageBatch) Error() error { _ = "STUB: not implemented"; return nil }

func (mb *messageBatch) Done() <-chan struct{} { _ = "STUB: not implemented"; return nil }

func (sub *Subscription) FetchBatch(batch int, opts ...PullOpt) (MessageBatch, error) {
	_ = "STUB: not implemented"
	return *new(MessageBatch), nil
}

func (o *pullOpts) checkCtxErr(err error) error { _ = "STUB: not implemented"; return nil }

func (js *js) getConsumerInfo(stream, consumer string) (*ConsumerInfo, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func (js *js) getConsumerInfoContext(ctx context.Context, stream, consumer string, o *jsOpts) (*ConsumerInfo, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func (js *js) apiRequestWithContext(ctx context.Context, subj string, data []byte) (*Msg, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func (m *Msg) checkReply() error { _ = "STUB: not implemented"; return nil }

func (m *Msg) ackReply(ackType []byte, sync bool, opts ...AckOpt) error {
	_ = "STUB: not implemented"
	return nil
}

func (m *Msg) Ack(opts ...AckOpt) error { _ = "STUB: not implemented"; return nil }

func (m *Msg) AckSync(opts ...AckOpt) error { _ = "STUB: not implemented"; return nil }

func (m *Msg) Nak(opts ...AckOpt) error { _ = "STUB: not implemented"; return nil }

func (m *Msg) NakWithDelay(delay time.Duration, opts ...AckOpt) error {
	_ = "STUB: not implemented"
	return nil
}

func (m *Msg) Term(opts ...AckOpt) error { _ = "STUB: not implemented"; return nil }

func (m *Msg) InProgress(opts ...AckOpt) error { _ = "STUB: not implemented"; return nil }

type MsgMetadata struct {
	Sequence     SequencePair
	NumDelivered uint64
	NumPending   uint64
	Timestamp    time.Time
	Stream       string
	Consumer     string
	Domain       string
}

func (m *Msg) Metadata() (*MsgMetadata, error) { _ = "STUB: not implemented"; return nil, nil }

type AckPolicy int

const (
	AckNonePolicy AckPolicy = iota

	AckAllPolicy

	AckExplicitPolicy

	AckFlowControlPolicy

	ackPolicyNotSet = 99
)

func jsonString(s string) string { _ = "STUB: not implemented"; return "" }

func (p *AckPolicy) UnmarshalJSON(data []byte) error { _ = "STUB: not implemented"; return nil }

func (p AckPolicy) MarshalJSON() ([]byte, error) { _ = "STUB: not implemented"; return nil, nil }

func (p AckPolicy) String() string { _ = "STUB: not implemented"; return "" }

type ReplayPolicy int

const (
	ReplayInstantPolicy ReplayPolicy = iota

	ReplayOriginalPolicy

	replayPolicyNotSet = 99
)

func (p *ReplayPolicy) UnmarshalJSON(data []byte) error { _ = "STUB: not implemented"; return nil }

func (p ReplayPolicy) MarshalJSON() ([]byte, error) { _ = "STUB: not implemented"; return nil, nil }

var (
	ackAck      = []byte("+ACK")
	ackNak      = []byte("-NAK")
	ackProgress = []byte("+WPI")
	ackTerm     = []byte("+TERM")
)

type DeliverPolicy int

const (
	DeliverAllPolicy DeliverPolicy = iota

	DeliverLastPolicy

	DeliverNewPolicy

	DeliverByStartSequencePolicy

	DeliverByStartTimePolicy

	DeliverLastPerSubjectPolicy

	deliverPolicyNotSet = 99
)

func (p *DeliverPolicy) UnmarshalJSON(data []byte) error { _ = "STUB: not implemented"; return nil }

func (p DeliverPolicy) MarshalJSON() ([]byte, error) { _ = "STUB: not implemented"; return nil, nil }

type RetentionPolicy int

const (
	LimitsPolicy RetentionPolicy = iota

	InterestPolicy

	WorkQueuePolicy
)

type DiscardPolicy int

const (
	DiscardOld DiscardPolicy = iota

	DiscardNew
)

const (
	limitsPolicyString    = "limits"
	interestPolicyString  = "interest"
	workQueuePolicyString = "workqueue"
)

func (rp RetentionPolicy) String() string { _ = "STUB: not implemented"; return "" }

func (rp RetentionPolicy) MarshalJSON() ([]byte, error) { _ = "STUB: not implemented"; return nil, nil }

func (rp *RetentionPolicy) UnmarshalJSON(data []byte) error { _ = "STUB: not implemented"; return nil }

func (dp DiscardPolicy) String() string { _ = "STUB: not implemented"; return "" }

func (dp DiscardPolicy) MarshalJSON() ([]byte, error) { _ = "STUB: not implemented"; return nil, nil }

func (dp *DiscardPolicy) UnmarshalJSON(data []byte) error { _ = "STUB: not implemented"; return nil }

type StorageType int

const (
	FileStorage StorageType = iota

	MemoryStorage
)

const (
	memoryStorageString = "memory"
	fileStorageString   = "file"
)

func (st StorageType) String() string { _ = "STUB: not implemented"; return "" }

func (st StorageType) MarshalJSON() ([]byte, error) { _ = "STUB: not implemented"; return nil, nil }

func (st *StorageType) UnmarshalJSON(data []byte) error { _ = "STUB: not implemented"; return nil }

type StoreCompression uint8

const (
	NoCompression StoreCompression = iota
	S2Compression
)

func (alg StoreCompression) String() string { _ = "STUB: not implemented"; return "" }

func (alg StoreCompression) MarshalJSON() ([]byte, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func (alg *StoreCompression) UnmarshalJSON(b []byte) error { _ = "STUB: not implemented"; return nil }

const nameHashLen = 8

func getHash(name string) string { _ = "STUB: not implemented"; return "" }
