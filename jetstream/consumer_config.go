package jetstream

import (
	"time"
)

type (
	ConsumerInfo struct {
		Stream string `json:"stream_name"`

		Name string `json:"name"`

		Created time.Time `json:"created"`

		Config ConsumerConfig `json:"config"`

		Delivered SequenceInfo `json:"delivered"`

		AckFloor SequenceInfo `json:"ack_floor"`

		NumAckPending int `json:"num_ack_pending"`

		NumRedelivered int `json:"num_redelivered"`

		NumWaiting int `json:"num_waiting"`

		NumPending uint64 `json:"num_pending"`

		Cluster *ClusterInfo `json:"cluster,omitempty"`

		PushBound bool `json:"push_bound,omitempty"`

		TimeStamp time.Time `json:"ts"`

		PriorityGroups []PriorityGroupState `json:"priority_groups,omitempty"`

		Paused bool `json:"paused,omitempty"`

		PauseRemaining time.Duration `json:"pause_remaining,omitempty"`
	}

	PriorityGroupState struct {
		Group string `json:"group"`

		PinnedClientID string `json:"pinned_client_id,omitempty"`

		PinnedTS time.Time `json:"pinned_ts,omitempty"`
	}

	ConsumerConfig struct {
		Name string `json:"name,omitempty"`

		Durable string `json:"durable_name,omitempty"`

		Description string `json:"description,omitempty"`

		DeliverPolicy DeliverPolicy `json:"deliver_policy"`

		OptStartSeq uint64 `json:"opt_start_seq,omitempty"`

		OptStartTime *time.Time `json:"opt_start_time,omitempty"`

		AckPolicy AckPolicy `json:"ack_policy"`

		AckWait time.Duration `json:"ack_wait,omitempty"`

		MaxDeliver int `json:"max_deliver,omitempty"`

		BackOff []time.Duration `json:"backoff,omitempty"`

		FilterSubject string `json:"filter_subject,omitempty"`

		ReplayPolicy ReplayPolicy `json:"replay_policy"`

		RateLimit uint64 `json:"rate_limit_bps,omitempty"`

		SampleFrequency string `json:"sample_freq,omitempty"`

		MaxWaiting int `json:"max_waiting,omitempty"`

		MaxAckPending int `json:"max_ack_pending,omitempty"`

		HeadersOnly bool `json:"headers_only,omitempty"`

		MaxRequestBatch int `json:"max_batch,omitempty"`

		MaxRequestExpires time.Duration `json:"max_expires,omitempty"`

		MaxRequestMaxBytes int `json:"max_bytes,omitempty"`

		InactiveThreshold time.Duration `json:"inactive_threshold,omitempty"`

		Replicas int `json:"num_replicas"`

		MemoryStorage bool `json:"mem_storage,omitempty"`

		FilterSubjects []string `json:"filter_subjects,omitempty"`

		Metadata map[string]string `json:"metadata,omitempty"`

		PauseUntil *time.Time `json:"pause_until,omitempty"`

		PriorityPolicy PriorityPolicy `json:"priority_policy,omitempty"`

		PinnedTTL time.Duration `json:"priority_timeout,omitempty"`

		PriorityGroups []string `json:"priority_groups,omitempty"`

		DeliverSubject string `json:"deliver_subject,omitempty"`

		DeliverGroup string `json:"deliver_group,omitempty"`

		FlowControl bool `json:"flow_control,omitempty"`

		IdleHeartbeat time.Duration `json:"idle_heartbeat,omitempty"`
	}

	OrderedConsumerConfig struct {
		FilterSubjects []string `json:"filter_subjects,omitempty"`

		DeliverPolicy DeliverPolicy `json:"deliver_policy"`

		OptStartSeq uint64 `json:"opt_start_seq,omitempty"`

		OptStartTime *time.Time `json:"opt_start_time,omitempty"`

		ReplayPolicy ReplayPolicy `json:"replay_policy"`

		InactiveThreshold time.Duration `json:"inactive_threshold,omitempty"`

		HeadersOnly bool `json:"headers_only,omitempty"`

		MaxResetAttempts int

		Metadata map[string]string `json:"metadata,omitempty"`

		NamePrefix string `json:"-"`
	}

	DeliverPolicy int

	AckPolicy int

	ReplayPolicy int

	SequenceInfo struct {
		Consumer uint64     `json:"consumer_seq"`
		Stream   uint64     `json:"stream_seq"`
		Last     *time.Time `json:"last_active,omitempty"`
	}

	PriorityPolicy int
)

const (
	PriorityPolicyNone PriorityPolicy = iota

	PriorityPolicyPinned

	PriorityPolicyOverflow

	PriorityPolicyPrioritized
)

func (p *PriorityPolicy) UnmarshalJSON(data []byte) error { _ = "STUB: not implemented"; return nil }

func (p PriorityPolicy) MarshalJSON() ([]byte, error) { _ = "STUB: not implemented"; return nil, nil }

const (
	DeliverAllPolicy DeliverPolicy = iota

	DeliverLastPolicy

	DeliverNewPolicy

	DeliverByStartSequencePolicy

	DeliverByStartTimePolicy

	DeliverLastPerSubjectPolicy
)

func (p *DeliverPolicy) UnmarshalJSON(data []byte) error { _ = "STUB: not implemented"; return nil }

func (p DeliverPolicy) MarshalJSON() ([]byte, error) { _ = "STUB: not implemented"; return nil, nil }

func (p DeliverPolicy) String() string { _ = "STUB: not implemented"; return "" }

const (
	AckExplicitPolicy AckPolicy = iota

	AckAllPolicy

	AckNonePolicy

	AckFlowControlPolicy
)

func (p *AckPolicy) UnmarshalJSON(data []byte) error { _ = "STUB: not implemented"; return nil }

func (p AckPolicy) MarshalJSON() ([]byte, error) { _ = "STUB: not implemented"; return nil, nil }

func (p AckPolicy) String() string { _ = "STUB: not implemented"; return "" }

const (
	ReplayInstantPolicy ReplayPolicy = iota

	ReplayOriginalPolicy
)

func (p *ReplayPolicy) UnmarshalJSON(data []byte) error { _ = "STUB: not implemented"; return nil }

func (p ReplayPolicy) MarshalJSON() ([]byte, error) { _ = "STUB: not implemented"; return nil, nil }

func (p ReplayPolicy) String() string { _ = "STUB: not implemented"; return "" }
