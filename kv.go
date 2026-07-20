package nats

import (
	"context"
	"errors"
	"regexp"
	"sync"
	"time"
)

type KeyValueManager interface {
	KeyValue(bucket string) (KeyValue, error)

	CreateKeyValue(cfg *KeyValueConfig) (KeyValue, error)

	DeleteKeyValue(bucket string) error

	KeyValueStoreNames() <-chan string

	KeyValueStores() <-chan KeyValueStatus
}

type KeyValue interface {
	Get(key string) (entry KeyValueEntry, err error)

	GetRevision(key string, revision uint64) (entry KeyValueEntry, err error)

	Put(key string, value []byte) (revision uint64, err error)

	PutString(key string, value string) (revision uint64, err error)

	Create(key string, value []byte) (revision uint64, err error)

	Update(key string, value []byte, last uint64) (revision uint64, err error)

	Delete(key string, opts ...DeleteOpt) error

	Purge(key string, opts ...DeleteOpt) error

	Watch(keys string, opts ...WatchOpt) (KeyWatcher, error)

	WatchAll(opts ...WatchOpt) (KeyWatcher, error)

	WatchFiltered(keys []string, opts ...WatchOpt) (KeyWatcher, error)

	Keys(opts ...WatchOpt) ([]string, error)

	ListKeys(opts ...WatchOpt) (KeyLister, error)

	History(key string, opts ...WatchOpt) ([]KeyValueEntry, error)

	Bucket() string

	PurgeDeletes(opts ...PurgeOpt) error

	Status() (KeyValueStatus, error)
}

type KeyValueStatus interface {
	Bucket() string

	Values() uint64

	History() int64

	TTL() time.Duration

	BackingStore() string

	Bytes() uint64

	IsCompressed() bool

	Config() KeyValueConfig
}

type KeyWatcher interface {
	Context() context.Context

	Updates() <-chan KeyValueEntry

	Stop() error

	Error() <-chan error
}

type KeyLister interface {
	Keys() <-chan string
	Stop() error

	Error() <-chan error
}

type WatchOpt interface {
	configureWatcher(opts *watchOpts) error
}

func (ctx ContextOpt) configureWatcher(opts *watchOpts) error {
	_ = "STUB: not implemented"
	return nil
}

type watchOpts struct {
	ctx context.Context

	ignoreDeletes bool

	includeHistory bool

	updatesOnly bool

	metaOnly bool
}

type watchOptFn func(opts *watchOpts) error

func (opt watchOptFn) configureWatcher(opts *watchOpts) error {
	_ = "STUB: not implemented"
	return nil
}

func IncludeHistory() WatchOpt { _ = "STUB: not implemented"; return *new(WatchOpt) }

func UpdatesOnly() WatchOpt { _ = "STUB: not implemented"; return *new(WatchOpt) }

func IgnoreDeletes() WatchOpt { _ = "STUB: not implemented"; return *new(WatchOpt) }

func MetaOnly() WatchOpt { _ = "STUB: not implemented"; return *new(WatchOpt) }

type PurgeOpt interface {
	configurePurge(opts *purgeOpts) error
}

type purgeOpts struct {
	dmthr time.Duration
	ctx   context.Context
}

type DeleteMarkersOlderThan time.Duration

func (ttl DeleteMarkersOlderThan) configurePurge(opts *purgeOpts) error {
	_ = "STUB: not implemented"
	return nil
}

func (ctx ContextOpt) configurePurge(opts *purgeOpts) error { _ = "STUB: not implemented"; return nil }

type DeleteOpt interface {
	configureDelete(opts *deleteOpts) error
}

type deleteOpts struct {
	purge bool

	revision uint64
}

type deleteOptFn func(opts *deleteOpts) error

func (opt deleteOptFn) configureDelete(opts *deleteOpts) error {
	_ = "STUB: not implemented"
	return nil
}

func LastRevision(revision uint64) DeleteOpt { _ = "STUB: not implemented"; return *new(DeleteOpt) }

func purge() DeleteOpt { _ = "STUB: not implemented"; return *new(DeleteOpt) }

type KeyValueConfig struct {
	Bucket       string          `json:"bucket"`
	Description  string          `json:"description,omitempty"`
	MaxValueSize int32           `json:"max_value_size,omitempty"`
	History      uint8           `json:"history,omitempty"`
	TTL          time.Duration   `json:"ttl,omitempty"`
	MaxBytes     int64           `json:"max_bytes,omitempty"`
	Storage      StorageType     `json:"storage,omitempty"`
	Replicas     int             `json:"num_replicas,omitempty"`
	Placement    *Placement      `json:"placement,omitempty"`
	RePublish    *RePublish      `json:"republish,omitempty"`
	Mirror       *StreamSource   `json:"mirror,omitempty"`
	Sources      []*StreamSource `json:"sources,omitempty"`

	Compression bool `json:"compression,omitempty"`
}

const (
	KeyValueMaxHistory = 64
	AllKeys            = ">"
	kvLatestRevision   = 0
	kvop               = "KV-Operation"
	kvdel              = "DEL"
	kvpurge            = "PURGE"
)

type KeyValueOp uint8

const (
	KeyValuePut KeyValueOp = iota
	KeyValueDelete
	KeyValuePurge
)

func (op KeyValueOp) String() string { _ = "STUB: not implemented"; return "" }

type KeyValueEntry interface {
	Bucket() string

	Key() string

	Value() []byte

	Revision() uint64

	Created() time.Time

	Delta() uint64

	Operation() KeyValueOp
}

var (
	ErrKeyValueConfigRequired = errors.New("nats: config required")
	ErrInvalidBucketName      = errors.New("nats: invalid bucket name")
	ErrInvalidKey             = errors.New("nats: invalid key")
	ErrBucketNotFound         = errors.New("nats: bucket not found")
	ErrBadBucket              = errors.New("nats: bucket not valid key-value store")
	ErrKeyNotFound            = errors.New("nats: key not found")
	ErrKeyDeleted             = errors.New("nats: key was deleted")
	ErrHistoryToLarge         = errors.New("nats: history limited to a max of 64")
	ErrNoKeysFound            = errors.New("nats: no keys found")
	ErrKeyWatcherTimeout      = errors.New("nats: key watcher timed out waiting for initial keys")
)

var (
	ErrKeyExists JetStreamError = &jsError{apiErr: &APIError{ErrorCode: JSErrCodeStreamWrongLastSequence, Code: 400}, message: "key exists"}
)

const (
	kvBucketNamePre         = "KV_"
	kvBucketNameTmpl        = "KV_%s"
	kvSubjectsTmpl          = "$KV.%s.>"
	kvSubjectsPreTmpl       = "$KV.%s."
	kvSubjectsPreDomainTmpl = "%s.$KV.%s."
)

var (
	validBucketRe    = regexp.MustCompile(`^[a-zA-Z0-9_-]+$`)
	validKeyRe       = regexp.MustCompile(`^[-/_=\.a-zA-Z0-9]+$`)
	validSearchKeyRe = regexp.MustCompile(`^[-/_=\.a-zA-Z0-9*]*[>]?$`)
)

func (js *js) KeyValue(bucket string) (KeyValue, error) {
	_ = "STUB: not implemented"
	return *new(KeyValue), nil
}

func (js *js) CreateKeyValue(cfg *KeyValueConfig) (KeyValue, error) {
	_ = "STUB: not implemented"
	return *new(KeyValue), nil
}

func (js *js) DeleteKeyValue(bucket string) error { _ = "STUB: not implemented"; return nil }

type kvs struct {
	name   string
	stream string
	pre    string
	putPre string
	js     *js

	useJSPfx bool

	useDirect bool
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

func bucketValid(bucket string) bool { _ = "STUB: not implemented"; return false }

func keyValid(key string) bool { _ = "STUB: not implemented"; return false }

func searchKeyValid(key string) bool { _ = "STUB: not implemented"; return false }

func (kv *kvs) Get(key string) (KeyValueEntry, error) {
	_ = "STUB: not implemented"
	return *new(KeyValueEntry), nil
}

func (kv *kvs) GetRevision(key string, revision uint64) (KeyValueEntry, error) {
	_ = "STUB: not implemented"
	return *new(KeyValueEntry), nil
}

func (kv *kvs) get(key string, revision uint64) (KeyValueEntry, error) {
	_ = "STUB: not implemented"
	return *new(KeyValueEntry), nil
}

func (kv *kvs) Put(key string, value []byte) (revision uint64, err error) {
	_ = "STUB: not implemented"
	return 0, nil
}

func (kv *kvs) PutString(key string, value string) (revision uint64, err error) {
	_ = "STUB: not implemented"
	return 0, nil
}

func (kv *kvs) Create(key string, value []byte) (revision uint64, err error) {
	_ = "STUB: not implemented"
	return 0, nil
}

func (kv *kvs) Update(key string, value []byte, revision uint64) (uint64, error) {
	_ = "STUB: not implemented"
	return 0, nil
}

func (kv *kvs) Delete(key string, opts ...DeleteOpt) error { _ = "STUB: not implemented"; return nil }

func (kv *kvs) Purge(key string, opts ...DeleteOpt) error { _ = "STUB: not implemented"; return nil }

const kvDefaultPurgeDeletesMarkerThreshold = 30 * time.Minute

func (kv *kvs) PurgeDeletes(opts ...PurgeOpt) error { _ = "STUB: not implemented"; return nil }

func (kv *kvs) Keys(opts ...WatchOpt) ([]string, error) { _ = "STUB: not implemented"; return nil, nil }

type keyLister struct {
	watcher KeyWatcher
	keys    chan string
}

func (kv *kvs) ListKeys(opts ...WatchOpt) (KeyLister, error) {
	_ = "STUB: not implemented"
	return *new(KeyLister), nil
}

func (kl *keyLister) Keys() <-chan string { _ = "STUB: not implemented"; return nil }

func (kl *keyLister) Stop() error { _ = "STUB: not implemented"; return nil }

func (kl *keyLister) Error() <-chan error { _ = "STUB: not implemented"; return nil }

func (kv *kvs) History(key string, opts ...WatchOpt) ([]KeyValueEntry, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

type watcher struct {
	mu            sync.Mutex
	updates       chan KeyValueEntry
	sub           *Subscription
	initDone      bool
	initPending   uint64
	received      uint64
	ctx           context.Context
	initDoneTimer *time.Timer
	errCh         chan error
}

func (w *watcher) Context() context.Context {
	_ = "STUB: not implemented"
	return *new(context.Context)
}

func (w *watcher) Updates() <-chan KeyValueEntry { _ = "STUB: not implemented"; return nil }

func (w *watcher) Stop() error { _ = "STUB: not implemented"; return nil }

func (w *watcher) Error() <-chan error { _ = "STUB: not implemented"; return nil }

func (kv *kvs) WatchAll(opts ...WatchOpt) (KeyWatcher, error) {
	_ = "STUB: not implemented"
	return *new(KeyWatcher), nil
}

func (kv *kvs) WatchFiltered(keys []string, opts ...WatchOpt) (KeyWatcher, error) {
	_ = "STUB: not implemented"
	return *new(KeyWatcher), nil
}

func (kv *kvs) Watch(keys string, opts ...WatchOpt) (KeyWatcher, error) {
	_ = "STUB: not implemented"
	return *new(KeyWatcher), nil
}

func (kv *kvs) Bucket() string { _ = "STUB: not implemented"; return "" }

type KeyValueBucketStatus struct {
	nfo    *StreamInfo
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

func (s *KeyValueBucketStatus) Config() KeyValueConfig {
	_ = "STUB: not implemented"
	return *new(KeyValueConfig)
}

func (kv *kvs) Status() (KeyValueStatus, error) {
	_ = "STUB: not implemented"
	return *new(KeyValueStatus), nil
}

func (js *js) KeyValueStoreNames() <-chan string { _ = "STUB: not implemented"; return nil }

func (js *js) KeyValueStores() <-chan KeyValueStatus { _ = "STUB: not implemented"; return nil }

func mapStreamToKVS(js *js, info *StreamInfo) *kvs { _ = "STUB: not implemented"; return nil }
