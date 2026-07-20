package jetstream

import (
	"context"
	"regexp"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
)

type (
	KeyValueManager interface {
		KeyValue(ctx context.Context, bucket string) (KeyValue, error)

		CreateKeyValue(ctx context.Context, cfg KeyValueConfig) (KeyValue, error)

		UpdateKeyValue(ctx context.Context, cfg KeyValueConfig) (KeyValue, error)

		CreateOrUpdateKeyValue(ctx context.Context, cfg KeyValueConfig) (KeyValue, error)

		DeleteKeyValue(ctx context.Context, bucket string) error

		KeyValueStoreNames(ctx context.Context) KeyValueNamesLister

		KeyValueStores(ctx context.Context) KeyValueLister
	}

	KeyValue interface {
		Get(ctx context.Context, key string) (KeyValueEntry, error)

		GetRevision(ctx context.Context, key string, revision uint64) (KeyValueEntry, error)

		Put(ctx context.Context, key string, value []byte) (uint64, error)

		PutString(ctx context.Context, key string, value string) (uint64, error)

		Create(ctx context.Context, key string, value []byte, opts ...KVCreateOpt) (uint64, error)

		Update(ctx context.Context, key string, value []byte, revision uint64) (uint64, error)

		Delete(ctx context.Context, key string, opts ...KVDeleteOpt) error

		Purge(ctx context.Context, key string, opts ...KVDeleteOpt) error

		Watch(ctx context.Context, keys string, opts ...WatchOpt) (KeyWatcher, error)

		WatchAll(ctx context.Context, opts ...WatchOpt) (KeyWatcher, error)

		WatchFiltered(ctx context.Context, keys []string, opts ...WatchOpt) (KeyWatcher, error)

		Keys(ctx context.Context, opts ...WatchOpt) ([]string, error)

		ListKeys(ctx context.Context, opts ...WatchOpt) (KeyLister, error)

		ListKeysFiltered(ctx context.Context, filters ...string) (KeyLister, error)

		History(ctx context.Context, key string, opts ...WatchOpt) ([]KeyValueEntry, error)

		Bucket() string

		PurgeDeletes(ctx context.Context, opts ...KVPurgeOpt) error

		Status(ctx context.Context) (KeyValueStatus, error)
	}

	KeyValueConfig struct {
		Bucket string `json:"bucket"`

		Description string `json:"description,omitempty"`

		MaxValueSize int32 `json:"max_value_size,omitempty"`

		History uint8 `json:"history,omitempty"`

		TTL time.Duration `json:"ttl,omitempty"`

		MaxBytes int64 `json:"max_bytes,omitempty"`

		Storage StorageType `json:"storage,omitempty"`

		Replicas int `json:"num_replicas,omitempty"`

		Placement *Placement `json:"placement,omitempty"`

		RePublish *RePublish `json:"republish,omitempty"`

		Mirror *StreamSource `json:"mirror,omitempty"`

		Sources []*StreamSource `json:"sources,omitempty"`

		Compression bool `json:"compression,omitempty"`

		LimitMarkerTTL time.Duration `json:"limit_marker_ttl,omitempty"`

		Metadata map[string]string `json:"metadata,omitempty"`
	}

	KeyLister interface {
		Keys() <-chan string
		Stop() error
	}

	KeyValueLister interface {
		Status() <-chan KeyValueStatus
		Error() error
	}

	KeyValueNamesLister interface {
		Name() <-chan string
		Error() error
	}

	KeyValueStatus interface {
		Bucket() string

		Values() uint64

		History() int64

		TTL() time.Duration

		BackingStore() string

		Bytes() uint64

		IsCompressed() bool

		LimitMarkerTTL() time.Duration

		Metadata() map[string]string

		Config() KeyValueConfig
	}

	KeyWatcher interface {
		Updates() <-chan KeyValueEntry
		Stop() error
	}

	KeyValueEntry interface {
		Bucket() string

		Key() string

		Value() []byte

		Revision() uint64

		Created() time.Time

		Delta() uint64

		Operation() KeyValueOp
	}
)

type (
	WatchOpt interface {
		configureWatcher(opts *watchOpts) error
	}

	watchOpts struct {
		ignoreDeletes bool

		includeHistory bool

		updatesOnly bool

		metaOnly bool

		resumeFromRevision uint64
	}

	KVDeleteOpt interface {
		configureDelete(opts *deleteOpts) error
	}

	deleteOpts struct {
		purge bool

		revision uint64

		ttl time.Duration
	}

	KVCreateOpt interface {
		configureCreate(opts *createOpts) error
	}

	createOpts struct {
		ttl time.Duration
	}

	KVPurgeOpt interface {
		configurePurge(opts *purgeOpts) error
	}

	purgeOpts struct {
		dmthr time.Duration
	}
)

type kvs struct {
	name       string
	streamName string
	pre        string
	putPre     string
	pushJS     nats.JetStreamContext
	js         *jetStream
	stream     Stream

	useJSPfx bool

	useDirect bool
}

type KeyValueOp uint8

const (
	KeyValuePut KeyValueOp = iota

	KeyValueDelete

	KeyValuePurge
)

func (op KeyValueOp) String() string { _ = "STUB: not implemented"; return "" }

const (
	kvBucketNamePre         = "KV_"
	kvBucketNameTmpl        = "KV_%s"
	kvSubjectsTmpl          = "$KV.%s.>"
	kvSubjectsPreTmpl       = "$KV.%s."
	kvSubjectsPreDomainTmpl = "%s.$KV.%s."
)

const (
	KeyValueMaxHistory = 64
	AllKeys            = ">"
	kvLatestRevision   = 0
	kvop               = "KV-Operation"
	kvdel              = "DEL"
	kvpurge            = "PURGE"
)

var (
	validBucketRe    = regexp.MustCompile(`^[a-zA-Z0-9_-]+$`)
	validKeyRe       = regexp.MustCompile(`^[-/_=\.a-zA-Z0-9]+$`)
	validSearchKeyRe = regexp.MustCompile(`^[-/_=\.a-zA-Z0-9*]*[>]?$`)
)

func (js *jetStream) KeyValue(ctx context.Context, bucket string) (KeyValue, error) {
	_ = "STUB: not implemented"
	return *new(KeyValue), nil
}

func (js *jetStream) CreateKeyValue(ctx context.Context, cfg KeyValueConfig) (KeyValue, error) {
	_ = "STUB: not implemented"
	return *new(KeyValue), nil
}

func (js *jetStream) UpdateKeyValue(ctx context.Context, cfg KeyValueConfig) (KeyValue, error) {
	_ = "STUB: not implemented"
	return *new(KeyValue), nil
}

func (js *jetStream) CreateOrUpdateKeyValue(ctx context.Context, cfg KeyValueConfig) (KeyValue, error) {
	_ = "STUB: not implemented"
	return *new(KeyValue), nil
}

func (js *jetStream) prepareKeyValueConfig(ctx context.Context, cfg KeyValueConfig) (StreamConfig, error) {
	_ = "STUB: not implemented"
	return *new(StreamConfig), nil
}

func (js *jetStream) DeleteKeyValue(ctx context.Context, bucket string) error {
	_ = "STUB: not implemented"
	return nil
}

func (js *jetStream) KeyValueStoreNames(ctx context.Context) KeyValueNamesLister {
	_ = "STUB: not implemented"
	return *new(KeyValueNamesLister)
}

func (js *jetStream) KeyValueStores(ctx context.Context) KeyValueLister {
	_ = "STUB: not implemented"
	return *new(KeyValueLister)
}

type KeyValueBucketStatus struct {
	info   *StreamInfo
	bucket string
}

func (s *KeyValueBucketStatus) Bucket() string { _ = "STUB: not implemented"; return "" }

func (s *KeyValueBucketStatus) Values() uint64 { _ = "STUB: not implemented"; return 0 }

func (s *KeyValueBucketStatus) History() int64 { _ = "STUB: not implemented"; return 0 }

func (s *KeyValueBucketStatus) TTL() time.Duration {
	_ = "STUB: not implemented"
	return *new(time.Duration)
}

func (s *KeyValueBucketStatus) BackingStore() string { _ = "STUB: not implemented"; return "" }

func (s *KeyValueBucketStatus) StreamInfo() *StreamInfo { _ = "STUB: not implemented"; return nil }

func (s *KeyValueBucketStatus) Bytes() uint64 { _ = "STUB: not implemented"; return 0 }

func (s *KeyValueBucketStatus) IsCompressed() bool { _ = "STUB: not implemented"; return false }

func (s *KeyValueBucketStatus) LimitMarkerTTL() time.Duration {
	_ = "STUB: not implemented"
	return *new(time.Duration)
}

func (s *KeyValueBucketStatus) Config() KeyValueConfig {
	_ = "STUB: not implemented"
	return *new(KeyValueConfig)
}

func (s *KeyValueBucketStatus) Metadata() map[string]string { _ = "STUB: not implemented"; return nil }

type kvLister struct {
	kvs     chan KeyValueStatus
	kvNames chan string
	err     error
}

func (kl *kvLister) Status() <-chan KeyValueStatus { _ = "STUB: not implemented"; return nil }

func (kl *kvLister) Name() <-chan string { _ = "STUB: not implemented"; return nil }

func (kl *kvLister) Error() error { _ = "STUB: not implemented"; return nil }

func (js *jetStream) legacyJetStream() (nats.JetStreamContext, error) {
	_ = "STUB: not implemented"
	return *new(nats.JetStreamContext), nil
}

func bucketValid(bucket string) bool { _ = "STUB: not implemented"; return false }

func keyValid(key string) bool { _ = "STUB: not implemented"; return false }

func searchKeyValid(key string) bool { _ = "STUB: not implemented"; return false }

func (kv *kvs) get(ctx context.Context, key string, revision uint64) (KeyValueEntry, error) {
	_ = "STUB: not implemented"
	return *new(KeyValueEntry), nil
}

type kve struct {
	bucket   string
	key      string
	value    []byte
	revision uint64
	delta    uint64
	created  time.Time
	op       KeyValueOp
}

func (e *kve) Bucket() string        { _ = "STUB: not implemented"; return "" }
func (e *kve) Key() string           { _ = "STUB: not implemented"; return "" }
func (e *kve) Value() []byte         { _ = "STUB: not implemented"; return nil }
func (e *kve) Revision() uint64      { _ = "STUB: not implemented"; return 0 }
func (e *kve) Created() time.Time    { _ = "STUB: not implemented"; return *new(time.Time) }
func (e *kve) Delta() uint64         { _ = "STUB: not implemented"; return 0 }
func (e *kve) Operation() KeyValueOp { _ = "STUB: not implemented"; return *new(KeyValueOp) }

func (kv *kvs) Get(ctx context.Context, key string) (KeyValueEntry, error) {
	_ = "STUB: not implemented"
	return *new(KeyValueEntry), nil
}

func (kv *kvs) GetRevision(ctx context.Context, key string, revision uint64) (KeyValueEntry, error) {
	_ = "STUB: not implemented"
	return *new(KeyValueEntry), nil
}

func (kv *kvs) Put(ctx context.Context, key string, value []byte) (uint64, error) {
	_ = "STUB: not implemented"
	return 0, nil
}

func (kv *kvs) PutString(ctx context.Context, key string, value string) (uint64, error) {
	_ = "STUB: not implemented"
	return 0, nil
}

func (kv *kvs) Create(ctx context.Context, key string, value []byte, opts ...KVCreateOpt) (revision uint64, err error) {
	_ = "STUB: not implemented"
	return 0, nil
}

func (kv *kvs) Update(ctx context.Context, key string, value []byte, revision uint64) (uint64, error) {
	_ = "STUB: not implemented"
	return 0, nil
}

func (kv *kvs) updateRevision(ctx context.Context, key string, value []byte, revision uint64, ttl time.Duration) (uint64, error) {
	_ = "STUB: not implemented"
	return 0, nil
}

func (kv *kvs) Delete(ctx context.Context, key string, opts ...KVDeleteOpt) error {
	_ = "STUB: not implemented"
	return nil
}

func (kv *kvs) Purge(ctx context.Context, key string, opts ...KVDeleteOpt) error {
	_ = "STUB: not implemented"
	return nil
}

func purge() KVDeleteOpt { _ = "STUB: not implemented"; return *new(KVDeleteOpt) }

type watcher struct {
	mu          sync.Mutex
	updates     chan KeyValueEntry
	sub         *nats.Subscription
	initDone    bool
	initPending uint64
	received    uint64
}

func (w *watcher) Updates() <-chan KeyValueEntry { _ = "STUB: not implemented"; return nil }

func (w *watcher) Stop() error { _ = "STUB: not implemented"; return nil }

func (kv *kvs) WatchFiltered(ctx context.Context, keys []string, opts ...WatchOpt) (KeyWatcher, error) {
	_ = "STUB: not implemented"
	return *new(KeyWatcher), nil
}

func (kv *kvs) Watch(ctx context.Context, keys string, opts ...WatchOpt) (KeyWatcher, error) {
	_ = "STUB: not implemented"
	return *new(KeyWatcher), nil
}

func (kv *kvs) WatchAll(ctx context.Context, opts ...WatchOpt) (KeyWatcher, error) {
	_ = "STUB: not implemented"
	return *new(KeyWatcher), nil
}

func (kv *kvs) Keys(ctx context.Context, opts ...WatchOpt) ([]string, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

type keyLister struct {
	watcher KeyWatcher
	keys    chan string
}

func (kv *kvs) ListKeys(ctx context.Context, opts ...WatchOpt) (KeyLister, error) {
	_ = "STUB: not implemented"
	return *new(KeyLister), nil
}

func (kv *kvs) ListKeysFiltered(ctx context.Context, filters ...string) (KeyLister, error) {
	_ = "STUB: not implemented"
	return *new(KeyLister), nil
}

func (kl *keyLister) Keys() <-chan string { _ = "STUB: not implemented"; return nil }

func (kl *keyLister) Stop() error { _ = "STUB: not implemented"; return nil }

func (kv *kvs) History(ctx context.Context, key string, opts ...WatchOpt) ([]KeyValueEntry, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func (kv *kvs) Bucket() string { _ = "STUB: not implemented"; return "" }

const kvDefaultPurgeDeletesMarkerThreshold = 30 * time.Minute

func (kv *kvs) PurgeDeletes(ctx context.Context, opts ...KVPurgeOpt) error {
	_ = "STUB: not implemented"
	return nil
}

func (kv *kvs) Status(ctx context.Context) (KeyValueStatus, error) {
	_ = "STUB: not implemented"
	return *new(KeyValueStatus), nil
}

func mapStreamToKVS(js *jetStream, pushJS nats.JetStreamContext, stream Stream) *kvs {
	_ = "STUB: not implemented"
	return nil
}
