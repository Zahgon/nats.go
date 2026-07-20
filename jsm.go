package nats

import (
	"context"
	"time"
)

type JetStreamManager interface {
	AddStream(cfg *StreamConfig, opts ...JSOpt) (*StreamInfo, error)

	UpdateStream(cfg *StreamConfig, opts ...JSOpt) (*StreamInfo, error)

	DeleteStream(name string, opts ...JSOpt) error

	StreamInfo(stream string, opts ...JSOpt) (*StreamInfo, error)

	PurgeStream(name string, opts ...JSOpt) error

	StreamsInfo(opts ...JSOpt) <-chan *StreamInfo

	Streams(opts ...JSOpt) <-chan *StreamInfo

	StreamNames(opts ...JSOpt) <-chan string

	GetMsg(name string, seq uint64, opts ...JSOpt) (*RawStreamMsg, error)

	GetLastMsg(name, subject string, opts ...JSOpt) (*RawStreamMsg, error)

	DeleteMsg(name string, seq uint64, opts ...JSOpt) error

	SecureDeleteMsg(name string, seq uint64, opts ...JSOpt) error

	AddConsumer(stream string, cfg *ConsumerConfig, opts ...JSOpt) (*ConsumerInfo, error)

	UpdateConsumer(stream string, cfg *ConsumerConfig, opts ...JSOpt) (*ConsumerInfo, error)

	DeleteConsumer(stream, consumer string, opts ...JSOpt) error

	ConsumerInfo(stream, name string, opts ...JSOpt) (*ConsumerInfo, error)

	ConsumersInfo(stream string, opts ...JSOpt) <-chan *ConsumerInfo

	Consumers(stream string, opts ...JSOpt) <-chan *ConsumerInfo

	ConsumerNames(stream string, opts ...JSOpt) <-chan string

	AccountInfo(opts ...JSOpt) (*AccountInfo, error)

	StreamNameBySubject(string, ...JSOpt) (string, error)
}

type StreamConfig struct {
	Name string `json:"name"`

	Description string `json:"description,omitempty"`

	Subjects []string `json:"subjects,omitempty"`

	Retention RetentionPolicy `json:"retention"`

	MaxConsumers int `json:"max_consumers"`

	MaxMsgs int64 `json:"max_msgs"`

	MaxBytes int64 `json:"max_bytes"`

	Discard DiscardPolicy `json:"discard"`

	DiscardNewPerSubject bool `json:"discard_new_per_subject,omitempty"`

	MaxAge time.Duration `json:"max_age"`

	MaxMsgsPerSubject int64 `json:"max_msgs_per_subject"`

	MaxMsgSize int32 `json:"max_msg_size,omitempty"`

	Storage StorageType `json:"storage"`

	Replicas int `json:"num_replicas"`

	NoAck bool `json:"no_ack,omitempty"`

	Duplicates time.Duration `json:"duplicate_window,omitempty"`

	Placement *Placement `json:"placement,omitempty"`

	Mirror *StreamSource `json:"mirror,omitempty"`

	Sources []*StreamSource `json:"sources,omitempty"`

	Sealed bool `json:"sealed,omitempty"`

	DenyDelete bool `json:"deny_delete,omitempty"`

	DenyPurge bool `json:"deny_purge,omitempty"`

	AllowRollup bool `json:"allow_rollup_hdrs,omitempty"`

	Compression StoreCompression `json:"compression"`

	FirstSeq uint64 `json:"first_seq,omitempty"`

	SubjectTransform *SubjectTransformConfig `json:"subject_transform,omitempty"`

	RePublish *RePublish `json:"republish,omitempty"`

	AllowDirect bool `json:"allow_direct"`

	MirrorDirect bool `json:"mirror_direct"`

	ConsumerLimits StreamConsumerLimits `json:"consumer_limits,omitempty"`

	Metadata map[string]string `json:"metadata,omitempty"`

	Template string `json:"template_owner,omitempty"`

	AllowMsgTTL bool `json:"allow_msg_ttl"`

	SubjectDeleteMarkerTTL time.Duration `json:"subject_delete_marker_ttl,omitempty"`
}

type SubjectTransformConfig struct {
	Source      string `json:"src,omitempty"`
	Destination string `json:"dest"`
}

type RePublish struct {
	Source      string `json:"src,omitempty"`
	Destination string `json:"dest"`
	HeadersOnly bool   `json:"headers_only,omitempty"`
}

type Placement struct {
	Cluster string   `json:"cluster"`
	Tags    []string `json:"tags,omitempty"`
}

type StreamSource struct {
	Name              string                   `json:"name"`
	OptStartSeq       uint64                   `json:"opt_start_seq,omitempty"`
	OptStartTime      *time.Time               `json:"opt_start_time,omitempty"`
	FilterSubject     string                   `json:"filter_subject,omitempty"`
	SubjectTransforms []SubjectTransformConfig `json:"subject_transforms,omitempty"`
	External          *ExternalStream          `json:"external,omitempty"`
	Domain            string                   `json:"-"`
}

type ExternalStream struct {
	APIPrefix     string `json:"api"`
	DeliverPrefix string `json:"deliver,omitempty"`
}

type StreamConsumerLimits struct {
	InactiveThreshold time.Duration `json:"inactive_threshold,omitempty"`
	MaxAckPending     int           `json:"max_ack_pending,omitempty"`
}

func (ss *StreamSource) copy() *StreamSource { _ = "STUB: not implemented"; return nil }

func (ss *StreamSource) convertDomain() error { _ = "STUB: not implemented"; return nil }

type apiResponse struct {
	Type  string    `json:"type"`
	Error *APIError `json:"error,omitempty"`
}

type apiPaged struct {
	Total  int `json:"total"`
	Offset int `json:"offset"`
	Limit  int `json:"limit"`
}

type apiPagedRequest struct {
	Offset int `json:"offset,omitempty"`
}

type AccountInfo struct {
	Tier
	Domain string          `json:"domain"`
	API    APIStats        `json:"api"`
	Tiers  map[string]Tier `json:"tiers"`
}

type Tier struct {
	Memory         uint64        `json:"memory"`
	Store          uint64        `json:"storage"`
	ReservedMemory uint64        `json:"reserved_memory"`
	ReservedStore  uint64        `json:"reserved_storage"`
	Streams        int           `json:"streams"`
	Consumers      int           `json:"consumers"`
	Limits         AccountLimits `json:"limits"`
}

type APIStats struct {
	Level    int    `json:"level"`
	Total    uint64 `json:"total"`
	Errors   uint64 `json:"errors"`
	Inflight uint64 `json:"inflight,omitempty"`
}

type AccountLimits struct {
	MaxMemory            int64 `json:"max_memory"`
	MaxStore             int64 `json:"max_storage"`
	MaxStreams           int   `json:"max_streams"`
	MaxConsumers         int   `json:"max_consumers"`
	MaxAckPending        int   `json:"max_ack_pending"`
	MemoryMaxStreamBytes int64 `json:"memory_max_stream_bytes"`
	StoreMaxStreamBytes  int64 `json:"storage_max_stream_bytes"`
	MaxBytesRequired     bool  `json:"max_bytes_required"`
}

type accountInfoResponse struct {
	apiResponse
	AccountInfo
}

func (js *js) AccountInfo(opts ...JSOpt) (*AccountInfo, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

type createConsumerRequest struct {
	Stream string          `json:"stream_name"`
	Config *ConsumerConfig `json:"config"`
}

type consumerResponse struct {
	apiResponse
	*ConsumerInfo
}

func (js *js) AddConsumer(stream string, cfg *ConsumerConfig, opts ...JSOpt) (*ConsumerInfo, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func (js *js) UpdateConsumer(stream string, cfg *ConsumerConfig, opts ...JSOpt) (*ConsumerInfo, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func (js *js) upsertConsumer(stream, consumerName string, cfg *ConsumerConfig, opts ...JSOpt) (*ConsumerInfo, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

type consumerDeleteResponse struct {
	apiResponse
	Success bool `json:"success,omitempty"`
}

func checkStreamName(stream string) error { _ = "STUB: not implemented"; return nil }

func checkConsumerName(consumer string) error { _ = "STUB: not implemented"; return nil }

func (js *js) DeleteConsumer(stream, consumer string, opts ...JSOpt) error {
	_ = "STUB: not implemented"
	return nil
}

func (js *js) ConsumerInfo(stream, consumer string, opts ...JSOpt) (*ConsumerInfo, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

type consumerLister struct {
	stream string
	js     *js

	err      error
	offset   int
	page     []*ConsumerInfo
	pageInfo *apiPaged
}

type consumersRequest struct {
	apiPagedRequest
}

type consumerListResponse struct {
	apiResponse
	apiPaged
	Consumers []*ConsumerInfo `json:"consumers"`
}

func (c *consumerLister) Next() bool { _ = "STUB: not implemented"; return false }

func (c *consumerLister) Page() []*ConsumerInfo { _ = "STUB: not implemented"; return nil }

func (c *consumerLister) Err() error { _ = "STUB: not implemented"; return nil }

func (jsc *js) Consumers(stream string, opts ...JSOpt) <-chan *ConsumerInfo {
	_ = "STUB: not implemented"
	return nil
}

func (jsc *js) ConsumersInfo(stream string, opts ...JSOpt) <-chan *ConsumerInfo {
	_ = "STUB: not implemented"
	return nil
}

type consumerNamesLister struct {
	stream string
	js     *js

	err      error
	offset   int
	page     []string
	pageInfo *apiPaged
}

type consumerNamesListResponse struct {
	apiResponse
	apiPaged
	Consumers []string `json:"consumers"`
}

func (c *consumerNamesLister) Next() bool { _ = "STUB: not implemented"; return false }

func (c *consumerNamesLister) Page() []string { _ = "STUB: not implemented"; return nil }

func (c *consumerNamesLister) Err() error { _ = "STUB: not implemented"; return nil }

func (jsc *js) ConsumerNames(stream string, opts ...JSOpt) <-chan string {
	_ = "STUB: not implemented"
	return nil
}

type streamCreateResponse struct {
	apiResponse
	*StreamInfo
}

func (js *js) AddStream(cfg *StreamConfig, opts ...JSOpt) (*StreamInfo, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

type (
	StreamInfoRequest struct {
		apiPagedRequest

		DeletedDetails bool `json:"deleted_details,omitempty"`

		SubjectsFilter string `json:"subjects_filter,omitempty"`
	}
	streamInfoResponse = struct {
		apiResponse
		apiPaged
		*StreamInfo
	}
)

func (js *js) StreamInfo(stream string, opts ...JSOpt) (*StreamInfo, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

type StreamInfo struct {
	Config     StreamConfig        `json:"config"`
	Created    time.Time           `json:"created"`
	State      StreamState         `json:"state"`
	Cluster    *ClusterInfo        `json:"cluster,omitempty"`
	Mirror     *StreamSourceInfo   `json:"mirror,omitempty"`
	Sources    []*StreamSourceInfo `json:"sources,omitempty"`
	Alternates []*StreamAlternate  `json:"alternates,omitempty"`
}

type StreamAlternate struct {
	Name    string `json:"name"`
	Domain  string `json:"domain,omitempty"`
	Cluster string `json:"cluster"`
}

type StreamSourceInfo struct {
	Name              string                   `json:"name"`
	Lag               uint64                   `json:"lag"`
	Active            time.Duration            `json:"active"`
	External          *ExternalStream          `json:"external"`
	Error             *APIError                `json:"error"`
	FilterSubject     string                   `json:"filter_subject,omitempty"`
	SubjectTransforms []SubjectTransformConfig `json:"subject_transforms,omitempty"`
}

type StreamState struct {
	Msgs        uint64            `json:"messages"`
	Bytes       uint64            `json:"bytes"`
	FirstSeq    uint64            `json:"first_seq"`
	FirstTime   time.Time         `json:"first_ts"`
	LastSeq     uint64            `json:"last_seq"`
	LastTime    time.Time         `json:"last_ts"`
	Consumers   int               `json:"consumer_count"`
	Deleted     []uint64          `json:"deleted"`
	NumDeleted  int               `json:"num_deleted"`
	NumSubjects uint64            `json:"num_subjects"`
	Subjects    map[string]uint64 `json:"subjects"`
}

type ClusterInfo struct {
	Name        string      `json:"name,omitempty"`
	RaftGroup   string      `json:"raft_group,omitempty"`
	Leader      string      `json:"leader,omitempty"`
	LeaderSince *time.Time  `json:"leader_since,omitempty"`
	SystemAcc   bool        `json:"system_account,omitempty"`
	TrafficAcc  string      `json:"traffic_account,omitempty"`
	Replicas    []*PeerInfo `json:"replicas,omitempty"`
}

type PeerInfo struct {
	Name    string        `json:"name"`
	Current bool          `json:"current"`
	Offline bool          `json:"offline,omitempty"`
	Active  time.Duration `json:"active"`
	Lag     uint64        `json:"lag,omitempty"`
}

func (js *js) UpdateStream(cfg *StreamConfig, opts ...JSOpt) (*StreamInfo, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

type streamDeleteResponse struct {
	apiResponse
	Success bool `json:"success,omitempty"`
}

func (js *js) DeleteStream(name string, opts ...JSOpt) error { _ = "STUB: not implemented"; return nil }

type apiMsgGetRequest struct {
	Seq     uint64 `json:"seq,omitempty"`
	LastFor string `json:"last_by_subj,omitempty"`
	NextFor string `json:"next_by_subj,omitempty"`
}

type RawStreamMsg struct {
	Subject  string
	Sequence uint64
	Header   Header
	Data     []byte
	Time     time.Time
}

type storedMsg struct {
	Subject  string    `json:"subject"`
	Sequence uint64    `json:"seq"`
	Header   []byte    `json:"hdrs,omitempty"`
	Data     []byte    `json:"data,omitempty"`
	Time     time.Time `json:"time"`
}

type apiMsgGetResponse struct {
	apiResponse
	Message *storedMsg `json:"message,omitempty"`
}

func (js *js) GetLastMsg(name, subject string, opts ...JSOpt) (*RawStreamMsg, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func (js *js) GetMsg(name string, seq uint64, opts ...JSOpt) (*RawStreamMsg, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func (js *js) getMsg(name string, mreq *apiMsgGetRequest, opts ...JSOpt) (*RawStreamMsg, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func convertDirectGetMsgResponseToMsg(name string, r *Msg) (*RawStreamMsg, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

type msgDeleteRequest struct {
	Seq     uint64 `json:"seq"`
	NoErase bool   `json:"no_erase,omitempty"`
}

type msgDeleteResponse struct {
	apiResponse
	Success bool `json:"success,omitempty"`
}

func (js *js) DeleteMsg(name string, seq uint64, opts ...JSOpt) error {
	_ = "STUB: not implemented"
	return nil
}

func (js *js) SecureDeleteMsg(name string, seq uint64, opts ...JSOpt) error {
	_ = "STUB: not implemented"
	return nil
}

func (js *js) deleteMsg(o *jsOpts, stream string, req *msgDeleteRequest) error {
	_ = "STUB: not implemented"
	return nil
}

type StreamPurgeRequest struct {
	Sequence uint64 `json:"seq,omitempty"`

	Subject string `json:"filter,omitempty"`

	Keep uint64 `json:"keep,omitempty"`
}

type streamPurgeResponse struct {
	apiResponse
	Success bool   `json:"success,omitempty"`
	Purged  uint64 `json:"purged"`
}

func (js *js) PurgeStream(stream string, opts ...JSOpt) error {
	_ = "STUB: not implemented"
	return nil
}

func (js *js) purgeStream(stream string, req *StreamPurgeRequest, opts ...JSOpt) error {
	_ = "STUB: not implemented"
	return nil
}

type streamLister struct {
	js   *js
	page []*StreamInfo
	err  error

	offset   int
	pageInfo *apiPaged
}

type streamListResponse struct {
	apiResponse
	apiPaged
	Streams []*StreamInfo `json:"streams"`
}

type streamNamesRequest struct {
	apiPagedRequest

	Subject string `json:"subject,omitempty"`
}

func (s *streamLister) Next() bool { _ = "STUB: not implemented"; return false }

func (s *streamLister) Page() []*StreamInfo { _ = "STUB: not implemented"; return nil }

func (s *streamLister) Err() error { _ = "STUB: not implemented"; return nil }

func (jsc *js) Streams(opts ...JSOpt) <-chan *StreamInfo { _ = "STUB: not implemented"; return nil }

func (jsc *js) StreamsInfo(opts ...JSOpt) <-chan *StreamInfo { _ = "STUB: not implemented"; return nil }

type streamNamesLister struct {
	js *js

	err      error
	offset   int
	page     []string
	pageInfo *apiPaged
}

func (l *streamNamesLister) Next() bool { _ = "STUB: not implemented"; return false }

func (l *streamNamesLister) Page() []string { _ = "STUB: not implemented"; return nil }

func (l *streamNamesLister) Err() error { _ = "STUB: not implemented"; return nil }

func (jsc *js) StreamNames(opts ...JSOpt) <-chan string { _ = "STUB: not implemented"; return nil }

func (jsc *js) StreamNameBySubject(subj string, opts ...JSOpt) (string, error) {
	_ = "STUB: not implemented"
	return "", nil
}

func getJSContextOpts(defs *jsOpts, opts ...JSOpt) (*jsOpts, context.CancelFunc, error) {
	_ = "STUB: not implemented"
	return nil, *new(context.CancelFunc), nil
}
