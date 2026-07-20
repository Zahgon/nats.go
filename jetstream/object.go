package jetstream

import (
	"context"
	"hash"
	"io"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
)

type (
	ObjectStoreManager interface {
		ObjectStore(ctx context.Context, bucket string) (ObjectStore, error)

		CreateObjectStore(ctx context.Context, cfg ObjectStoreConfig) (ObjectStore, error)

		UpdateObjectStore(ctx context.Context, cfg ObjectStoreConfig) (ObjectStore, error)

		CreateOrUpdateObjectStore(ctx context.Context, cfg ObjectStoreConfig) (ObjectStore, error)

		DeleteObjectStore(ctx context.Context, bucket string) error

		ObjectStoreNames(ctx context.Context) ObjectStoreNamesLister

		ObjectStores(ctx context.Context) ObjectStoresLister
	}

	ObjectStore interface {
		Put(ctx context.Context, obj ObjectMeta, reader io.Reader) (*ObjectInfo, error)

		PutBytes(ctx context.Context, name string, data []byte) (*ObjectInfo, error)

		PutString(ctx context.Context, name string, data string) (*ObjectInfo, error)

		PutFile(ctx context.Context, file string) (*ObjectInfo, error)

		Get(ctx context.Context, name string, opts ...GetObjectOpt) (ObjectResult, error)

		GetBytes(ctx context.Context, name string, opts ...GetObjectOpt) ([]byte, error)

		GetString(ctx context.Context, name string, opts ...GetObjectOpt) (string, error)

		GetFile(ctx context.Context, name, file string, opts ...GetObjectOpt) error

		GetInfo(ctx context.Context, name string, opts ...GetObjectInfoOpt) (*ObjectInfo, error)

		UpdateMeta(ctx context.Context, name string, meta ObjectMeta) error

		Delete(ctx context.Context, name string) error

		AddLink(ctx context.Context, name string, obj *ObjectInfo) (*ObjectInfo, error)

		AddBucketLink(ctx context.Context, name string, bucket ObjectStore) (*ObjectInfo, error)

		Seal(ctx context.Context) error

		Watch(ctx context.Context, opts ...WatchOpt) (ObjectWatcher, error)

		List(ctx context.Context, opts ...ListObjectsOpt) ([]*ObjectInfo, error)

		Status(ctx context.Context) (ObjectStoreStatus, error)
	}

	ObjectWatcher interface {
		Updates() <-chan *ObjectInfo
		Stop() error
	}

	ObjectStoreConfig struct {
		Bucket string `json:"bucket"`

		Description string `json:"description,omitempty"`

		TTL time.Duration `json:"max_age,omitempty"`

		MaxBytes int64 `json:"max_bytes,omitempty"`

		Storage StorageType `json:"storage,omitempty"`

		Replicas int `json:"num_replicas,omitempty"`

		Placement *Placement `json:"placement,omitempty"`

		Compression bool `json:"compression,omitempty"`

		Metadata map[string]string `json:"metadata,omitempty"`
	}

	ObjectStoresLister interface {
		Status() <-chan ObjectStoreStatus
		Error() error
	}

	ObjectStoreNamesLister interface {
		Name() <-chan string
		Error() error
	}

	ObjectStoreStatus interface {
		Bucket() string

		Description() string

		TTL() time.Duration

		Storage() StorageType

		Replicas() int

		Sealed() bool

		Size() uint64

		BackingStore() string

		Metadata() map[string]string

		IsCompressed() bool
	}

	ObjectMetaOptions struct {
		Link *ObjectLink `json:"link,omitempty"`

		ChunkSize uint32 `json:"max_chunk_size,omitempty"`
	}

	ObjectMeta struct {
		Name string `json:"name"`

		Description string `json:"description,omitempty"`

		Headers nats.Header `json:"headers,omitempty"`

		Metadata map[string]string `json:"metadata,omitempty"`

		Opts *ObjectMetaOptions `json:"options,omitempty"`
	}

	ObjectInfo struct {
		ObjectMeta

		Bucket string `json:"bucket"`

		NUID string `json:"nuid"`

		Size uint64 `json:"size"`

		ModTime time.Time `json:"mtime"`

		Chunks uint32 `json:"chunks"`

		Digest string `json:"digest,omitempty"`

		Deleted bool `json:"deleted,omitempty"`
	}

	ObjectLink struct {
		Bucket string `json:"bucket"`

		Name string `json:"name,omitempty"`
	}

	ObjectResult interface {
		io.ReadCloser
		Info() (*ObjectInfo, error)
		Error() error
	}

	GetObjectOpt func(opts *getObjectOpts) error

	GetObjectInfoOpt func(opts *getObjectInfoOpts) error

	ListObjectsOpt func(opts *listObjectOpts) error

	getObjectOpts struct {
		showDeleted bool
	}

	getObjectInfoOpts struct {
		showDeleted bool
	}

	listObjectOpts struct {
		showDeleted bool
	}

	obs struct {
		name       string
		streamName string
		stream     Stream
		pushJS     nats.JetStreamContext
		js         *jetStream
	}

	objResult struct {
		sync.Mutex
		info   *ObjectInfo
		r      io.ReadCloser
		err    error
		ctx    context.Context
		cancel context.CancelFunc
		digest hash.Hash
	}
)

const (
	objNameTmpl         = "OBJ_%s"
	objAllChunksPreTmpl = "$O.%s.C.>"
	objAllMetaPreTmpl   = "$O.%s.M.>"
	objChunksPreTmpl    = "$O.%s.C.%s"
	objMetaPreTmpl      = "$O.%s.M.%s"
	objNoPending        = "0"
	objDefaultChunkSize = uint32(128 * 1024)
	objDigestType       = "SHA-256="
	objDigestTmpl       = objDigestType + "%s"
)

func (js *jetStream) CreateObjectStore(ctx context.Context, cfg ObjectStoreConfig) (ObjectStore, error) {
	_ = "STUB: not implemented"
	return *new(ObjectStore), nil
}

func (js *jetStream) UpdateObjectStore(ctx context.Context, cfg ObjectStoreConfig) (ObjectStore, error) {
	_ = "STUB: not implemented"
	return *new(ObjectStore), nil
}

func (js *jetStream) CreateOrUpdateObjectStore(ctx context.Context, cfg ObjectStoreConfig) (ObjectStore, error) {
	_ = "STUB: not implemented"
	return *new(ObjectStore), nil
}

func (js *jetStream) prepareObjectStoreConfig(cfg ObjectStoreConfig) (StreamConfig, error) {
	_ = "STUB: not implemented"
	return *new(StreamConfig), nil
}

func (js *jetStream) ObjectStore(ctx context.Context, bucket string) (ObjectStore, error) {
	_ = "STUB: not implemented"
	return *new(ObjectStore), nil
}

func (js *jetStream) DeleteObjectStore(ctx context.Context, bucket string) error {
	_ = "STUB: not implemented"
	return nil
}

func encodeName(name string) string { _ = "STUB: not implemented"; return "" }

func (obs *obs) Put(ctx context.Context, meta ObjectMeta, r io.Reader) (*ObjectInfo, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func GetObjectDigestValue(data hash.Hash) string { _ = "STUB: not implemented"; return "" }

func DecodeObjectDigest(data string) ([]byte, error) { _ = "STUB: not implemented"; return nil, nil }

func (info *ObjectInfo) isLink() bool { _ = "STUB: not implemented"; return false }

func (obs *obs) Get(ctx context.Context, name string, opts ...GetObjectOpt) (ObjectResult, error) {
	_ = "STUB: not implemented"
	return *new(ObjectResult), nil
}

func (obs *obs) Delete(ctx context.Context, name string) error {
	_ = "STUB: not implemented"
	return nil
}

func publishMeta(ctx context.Context, info *ObjectInfo, js *jetStream) error {
	_ = "STUB: not implemented"
	return nil
}

func (obs *obs) AddLink(ctx context.Context, name string, obj *ObjectInfo) (*ObjectInfo, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func (ob *obs) AddBucketLink(ctx context.Context, name string, bucket ObjectStore) (*ObjectInfo, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func (obs *obs) PutBytes(ctx context.Context, name string, data []byte) (*ObjectInfo, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func (obs *obs) GetBytes(ctx context.Context, name string, opts ...GetObjectOpt) ([]byte, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func (obs *obs) PutString(ctx context.Context, name string, data string) (*ObjectInfo, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func (obs *obs) GetString(ctx context.Context, name string, opts ...GetObjectOpt) (string, error) {
	_ = "STUB: not implemented"
	return "", nil
}

func (obs *obs) PutFile(ctx context.Context, file string) (*ObjectInfo, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func (obs *obs) GetFile(ctx context.Context, name, file string, opts ...GetObjectOpt) error {
	_ = "STUB: not implemented"
	return nil
}

func (obs *obs) GetInfo(ctx context.Context, name string, opts ...GetObjectInfoOpt) (*ObjectInfo, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func (obs *obs) UpdateMeta(ctx context.Context, name string, meta ObjectMeta) error {
	_ = "STUB: not implemented"
	return nil
}

func (obs *obs) Seal(ctx context.Context) error { _ = "STUB: not implemented"; return nil }

type objWatcher struct {
	updates chan *ObjectInfo
	sub     *nats.Subscription
}

func (w *objWatcher) Updates() <-chan *ObjectInfo { _ = "STUB: not implemented"; return nil }

func (w *objWatcher) Stop() error { _ = "STUB: not implemented"; return nil }

func (obs *obs) Watch(ctx context.Context, opts ...WatchOpt) (ObjectWatcher, error) {
	_ = "STUB: not implemented"
	return *new(ObjectWatcher), nil
}

func (obs *obs) List(ctx context.Context, opts ...ListObjectsOpt) ([]*ObjectInfo, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

type ObjectBucketStatus struct {
	nfo    *StreamInfo
	bucket string
}

func (s *ObjectBucketStatus) Bucket() string { _ = "STUB: not implemented"; return "" }

func (s *ObjectBucketStatus) Description() string { _ = "STUB: not implemented"; return "" }

func (s *ObjectBucketStatus) TTL() time.Duration {
	_ = "STUB: not implemented"
	return *new(time.Duration)
}

func (s *ObjectBucketStatus) Storage() StorageType {
	_ = "STUB: not implemented"
	return *new(StorageType)
}

func (s *ObjectBucketStatus) Replicas() int { _ = "STUB: not implemented"; return 0 }

func (s *ObjectBucketStatus) Sealed() bool { _ = "STUB: not implemented"; return false }

func (s *ObjectBucketStatus) Size() uint64 { _ = "STUB: not implemented"; return 0 }

func (s *ObjectBucketStatus) BackingStore() string { _ = "STUB: not implemented"; return "" }

func (s *ObjectBucketStatus) Metadata() map[string]string { _ = "STUB: not implemented"; return nil }

func (s *ObjectBucketStatus) StreamInfo() *StreamInfo { _ = "STUB: not implemented"; return nil }

func (s *ObjectBucketStatus) IsCompressed() bool { _ = "STUB: not implemented"; return false }

func (obs *obs) Status(ctx context.Context) (ObjectStoreStatus, error) {
	_ = "STUB: not implemented"
	return *new(ObjectStoreStatus), nil
}

func (o *objResult) Read(p []byte) (n int, err error) { _ = "STUB: not implemented"; return 0, nil }

func (o *objResult) Close() error { _ = "STUB: not implemented"; return nil }

func (o *objResult) setErr(err error) { _ = "STUB: not implemented"; return }

func (o *objResult) Info() (*ObjectInfo, error) { _ = "STUB: not implemented"; return nil, nil }

func (o *objResult) Error() error { _ = "STUB: not implemented"; return nil }

func (js *jetStream) ObjectStoreNames(ctx context.Context) ObjectStoreNamesLister {
	_ = "STUB: not implemented"
	return *new(ObjectStoreNamesLister)
}

func (js *jetStream) ObjectStores(ctx context.Context) ObjectStoresLister {
	_ = "STUB: not implemented"
	return *new(ObjectStoresLister)
}

type obsLister struct {
	obs      chan ObjectStoreStatus
	obsNames chan string
	err      error
}

func (ol *obsLister) Status() <-chan ObjectStoreStatus { _ = "STUB: not implemented"; return nil }

func (ol *obsLister) Name() <-chan string { _ = "STUB: not implemented"; return nil }

func (ol *obsLister) Error() error { _ = "STUB: not implemented"; return nil }

func mapStreamToObjectStore(js *jetStream, pushJS nats.JetStreamContext, bucket string, stream Stream) *obs {
	_ = "STUB: not implemented"
	return nil
}
