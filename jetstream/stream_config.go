package jetstream

import (
	"time"
)

type (
	StreamInfo struct {
		Config StreamConfig `json:"config"`

		Created time.Time `json:"created"`

		State StreamState `json:"state"`

		Cluster *ClusterInfo `json:"cluster,omitempty"`

		Mirror *StreamSourceInfo `json:"mirror,omitempty"`

		Sources []*StreamSourceInfo `json:"sources,omitempty"`

		TimeStamp time.Time `json:"ts"`
	}

	StreamConfig struct {
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

		AllowMsgTTL bool `json:"allow_msg_ttl,omitempty"`

		SubjectDeleteMarkerTTL time.Duration `json:"subject_delete_marker_ttl,omitempty"`

		AllowMsgCounter bool `json:"allow_msg_counter,omitempty"`

		AllowAtomicPublish bool `json:"allow_atomic,omitempty"`

		AllowMsgSchedules bool `json:"allow_msg_schedules,omitempty"`

		PersistMode PersistModeType `json:"persist_mode,omitempty"`

		AllowBatchPublish bool `json:"allow_batched,omitempty"`
	}

	StreamSourceInfo struct {
		Name string `json:"name"`

		Lag uint64 `json:"lag"`

		Active time.Duration `json:"active"`

		FilterSubject string `json:"filter_subject,omitempty"`

		SubjectTransforms []SubjectTransformConfig `json:"subject_transforms,omitempty"`
	}

	StreamState struct {
		Msgs uint64 `json:"messages"`

		Bytes uint64 `json:"bytes"`

		FirstSeq uint64 `json:"first_seq"`

		FirstTime time.Time `json:"first_ts"`

		LastSeq uint64 `json:"last_seq"`

		LastTime time.Time `json:"last_ts"`

		Consumers int `json:"consumer_count"`

		Deleted []uint64 `json:"deleted"`

		NumDeleted int `json:"num_deleted"`

		NumSubjects uint64 `json:"num_subjects"`

		Subjects map[string]uint64 `json:"subjects"`
	}

	ClusterInfo struct {
		Name string `json:"name,omitempty"`

		RaftGroup string `json:"raft_group,omitempty"`

		Leader string `json:"leader,omitempty"`

		LeaderSince *time.Time `json:"leader_since,omitempty"`

		SystemAcc bool `json:"system_account,omitempty"`

		TrafficAcc string `json:"traffic_account,omitempty"`

		Replicas []*PeerInfo `json:"replicas,omitempty"`
	}

	PeerInfo struct {
		Name string `json:"name"`

		Current bool `json:"current"`

		Offline bool `json:"offline,omitempty"`

		Active time.Duration `json:"active"`

		Lag uint64 `json:"lag,omitempty"`
	}

	SubjectTransformConfig struct {
		Source string `json:"src"`

		Destination string `json:"dest"`
	}

	RePublish struct {
		Source string `json:"src,omitempty"`

		Destination string `json:"dest"`

		HeadersOnly bool `json:"headers_only,omitempty"`
	}

	Placement struct {
		Cluster string `json:"cluster"`

		Tags []string `json:"tags,omitempty"`
	}

	StreamSource struct {
		Name string `json:"name"`

		OptStartSeq uint64 `json:"opt_start_seq,omitempty"`

		OptStartTime *time.Time `json:"opt_start_time,omitempty"`

		FilterSubject string `json:"filter_subject,omitempty"`

		SubjectTransforms []SubjectTransformConfig `json:"subject_transforms,omitempty"`

		External *ExternalStream `json:"external,omitempty"`

		Consumer *StreamConsumerSource `json:"consumer,omitempty"`

		Domain string `json:"-"`
	}

	StreamConsumerSource struct {
		Name string `json:"name,omitempty"`

		DeliverSubject string `json:"deliver_subject,omitempty"`
	}

	ExternalStream struct {
		APIPrefix string `json:"api"`

		DeliverPrefix string `json:"deliver"`
	}

	StreamConsumerLimits struct {
		InactiveThreshold time.Duration `json:"inactive_threshold,omitempty"`

		MaxAckPending int `json:"max_ack_pending,omitempty"`
	}

	DiscardPolicy int

	RetentionPolicy int

	StorageType int

	StoreCompression uint8

	PersistModeType int
)

const (
	LimitsPolicy RetentionPolicy = iota

	InterestPolicy

	WorkQueuePolicy
)

const (
	DiscardOld DiscardPolicy = iota

	DiscardNew
)

const (
	limitsPolicyString    = "limits"
	interestPolicyString  = "interest"
	workQueuePolicyString = "workqueue"
)

const (
	DefaultPersistMode = PersistModeType(iota)

	AsyncPersistMode
)

func (rp RetentionPolicy) String() string { _ = "STUB: not implemented"; return "" }

func (rp RetentionPolicy) MarshalJSON() ([]byte, error) { _ = "STUB: not implemented"; return nil, nil }

func (rp *RetentionPolicy) UnmarshalJSON(data []byte) error { _ = "STUB: not implemented"; return nil }

func (dp DiscardPolicy) String() string { _ = "STUB: not implemented"; return "" }

func (dp DiscardPolicy) MarshalJSON() ([]byte, error) { _ = "STUB: not implemented"; return nil, nil }

func (dp *DiscardPolicy) UnmarshalJSON(data []byte) error { _ = "STUB: not implemented"; return nil }

func (pm PersistModeType) String() string { _ = "STUB: not implemented"; return "" }

func (pm PersistModeType) MarshalJSON() ([]byte, error) { _ = "STUB: not implemented"; return nil, nil }

func (pm *PersistModeType) UnmarshalJSON(data []byte) error { _ = "STUB: not implemented"; return nil }

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

func jsonString(s string) string { _ = "STUB: not implemented"; return "" }

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
