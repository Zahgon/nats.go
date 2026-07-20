package nats

import (
	"context"
	"errors"
	"hash"
	"io"
	"sync"
	"time"
)

type ObjectStoreManager interface {
	ObjectStore(bucket string) (ObjectStore, error)

	CreateObjectStore(cfg *ObjectStoreConfig) (ObjectStore, error)

	DeleteObjectStore(bucket string) error

	ObjectStoreNames(opts ...ObjectOpt) <-chan string

	ObjectStores(opts ...ObjectOpt) <-chan ObjectStoreStatus
}

type ObjectStore interface {
	Put(obj *ObjectMeta, reader io.Reader, opts ...ObjectOpt) (*ObjectInfo, error)

	Get(name string, opts ...GetObjectOpt) (ObjectResult, error)

	PutBytes(name string, data []byte, opts ...ObjectOpt) (*ObjectInfo, error)

	GetBytes(name string, opts ...GetObjectOpt) ([]byte, error)

	PutString(name string, data string, opts ...ObjectOpt) (*ObjectInfo, error)

	GetString(name string, opts ...GetObjectOpt) (string, error)

	PutFile(file string, opts ...ObjectOpt) (*ObjectInfo, error)

	GetFile(name, file string, opts ...GetObjectOpt) error

	GetInfo(name string, opts ...GetObjectInfoOpt) (*ObjectInfo, error)

	UpdateMeta(name string, meta *ObjectMeta) error

	Delete(name string) error

	AddLink(name string, obj *ObjectInfo) (*ObjectInfo, error)

	AddBucketLink(name string, bucket ObjectStore) (*ObjectInfo, error)

	Seal() error

	Watch(opts ...WatchOpt) (ObjectWatcher, error)

	List(opts ...ListObjectsOpt) ([]*ObjectInfo, error)

	Status() (ObjectStoreStatus, error)
}

type ObjectOpt interface {
	configureObject(opts *objOpts) error
}

type objOpts struct {
	ctx context.Context
}

func (ctx ContextOpt) configureObject(opts *objOpts) error { _ = "STUB: not implemented"; return nil }

type ObjectWatcher interface {
	Updates() <-chan *ObjectInfo

	Stop() error
}

var (
	ErrObjectConfigRequired = errors.New("nats: object-store config required")
	ErrBadObjectMeta        = errors.New("nats: object-store meta information invalid")
	ErrObjectNotFound       = errors.New("nats: object not found")
	ErrInvalidStoreName     = errors.New("nats: invalid object-store name")
	ErrDigestMismatch       = errors.New("nats: received a corrupt object, digests do not match")
	ErrInvalidDigestFormat  = errors.New("nats: object digest hash has invalid format")
	ErrNoObjectsFound       = errors.New("nats: no objects found")
	ErrObjectAlreadyExists  = errors.New("nats: an object already exists with that name")
	ErrNameRequired         = errors.New("nats: name is required")
	ErrNeeds262             = errors.New("nats: object-store requires at least server version 2.6.2")
	ErrLinkNotAllowed       = errors.New("nats: link cannot be set when putting the object in bucket")
	ErrObjectRequired       = errors.New("nats: object required")
	ErrNoLinkToDeleted      = errors.New("nats: not allowed to link to a deleted object")
	ErrNoLinkToLink         = errors.New("nats: not allowed to link to another link")
	ErrCantGetBucket        = errors.New("nats: invalid Get, object is a link to a bucket")
	ErrBucketRequired       = errors.New("nats: bucket required")
	ErrBucketMalformed      = errors.New("nats: bucket malformed")
	ErrUpdateMetaDeleted    = errors.New("nats: cannot update meta for a deleted object")
)

type ObjectStoreConfig struct {
	Bucket      string        `json:"bucket"`
	Description string        `json:"description,omitempty"`
	TTL         time.Duration `json:"max_age,omitempty"`
	MaxBytes    int64         `json:"max_bytes,omitempty"`
	Storage     StorageType   `json:"storage,omitempty"`
	Replicas    int           `json:"num_replicas,omitempty"`
	Placement   *Placement    `json:"placement,omitempty"`

	Metadata map[string]string `json:"metadata,omitempty"`

	Compression bool `json:"compression,omitempty"`
}

type ObjectStoreStatus interface {
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

type ObjectMetaOptions struct {
	Link      *ObjectLink `json:"link,omitempty"`
	ChunkSize uint32      `json:"max_chunk_size,omitempty"`
}

type ObjectMeta struct {
	Name        string            `json:"name"`
	Description string            `json:"description,omitempty"`
	Headers     Header            `json:"headers,omitempty"`
	Metadata    map[string]string `json:"metadata,omitempty"`

	Opts *ObjectMetaOptions `json:"options,omitempty"`
}

type ObjectInfo struct {
	ObjectMeta
	Bucket  string    `json:"bucket"`
	NUID    string    `json:"nuid"`
	Size    uint64    `json:"size"`
	ModTime time.Time `json:"mtime"`
	Chunks  uint32    `json:"chunks"`
	Digest  string    `json:"digest,omitempty"`
	Deleted bool      `json:"deleted,omitempty"`
}

type ObjectLink struct {
	Bucket string `json:"bucket"`

	Name string `json:"name,omitempty"`
}

type ObjectResult interface {
	io.ReadCloser
	Info() (*ObjectInfo, error)
	Error() error
}

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

type obs struct {
	name   string
	stream string
	js     *js
}

func (js *js) CreateObjectStore(cfg *ObjectStoreConfig) (ObjectStore, error) {
	_ = "STUB: not implemented"
	return *new(ObjectStore), nil
}

func (js *js) ObjectStore(bucket string) (ObjectStore, error) {
	_ = "STUB: not implemented"
	return *new(ObjectStore), nil
}

func (js *js) DeleteObjectStore(bucket string) error { _ = "STUB: not implemented"; return nil }

func encodeName(name string) string { _ = "STUB: not implemented"; return "" }

func (obs *obs) Put(meta *ObjectMeta, r io.Reader, opts ...ObjectOpt) (*ObjectInfo, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func GetObjectDigestValue(data hash.Hash) string { _ = "STUB: not implemented"; return "" }

func DecodeObjectDigest(data string) ([]byte, error) { _ = "STUB: not implemented"; return nil, nil }

type objResult struct {
	sync.Mutex
	info        *ObjectInfo
	r           io.ReadCloser
	err         error
	ctx         context.Context
	digest      hash.Hash
	readTimeout time.Duration
}

func (info *ObjectInfo) isLink() bool { _ = "STUB: not implemented"; return false }

type GetObjectOpt interface {
	configureGetObject(opts *getObjectOpts) error
}
type getObjectOpts struct {
	ctx context.Context

	showDeleted bool
}

type getObjectFn func(opts *getObjectOpts) error

func (opt getObjectFn) configureGetObject(opts *getObjectOpts) error {
	_ = "STUB: not implemented"
	return nil
}

func GetObjectShowDeleted() GetObjectOpt { _ = "STUB: not implemented"; return *new(GetObjectOpt) }

func (ctx ContextOpt) configureGetObject(opts *getObjectOpts) error {
	_ = "STUB: not implemented"
	return nil
}

func (obs *obs) Get(name string, opts ...GetObjectOpt) (ObjectResult, error) {
	_ = "STUB: not implemented"
	return *new(ObjectResult), nil
}

func (obs *obs) Delete(name string) error { _ = "STUB: not implemented"; return nil }

func publishMeta(info *ObjectInfo, js JetStreamContext) error {
	_ = "STUB: not implemented"
	return nil
}

func (obs *obs) AddLink(name string, obj *ObjectInfo) (*ObjectInfo, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func (ob *obs) AddBucketLink(name string, bucket ObjectStore) (*ObjectInfo, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func (obs *obs) PutBytes(name string, data []byte, opts ...ObjectOpt) (*ObjectInfo, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func (obs *obs) GetBytes(name string, opts ...GetObjectOpt) ([]byte, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func (obs *obs) PutString(name string, data string, opts ...ObjectOpt) (*ObjectInfo, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func (obs *obs) GetString(name string, opts ...GetObjectOpt) (string, error) {
	_ = "STUB: not implemented"
	return "", nil
}

func (obs *obs) PutFile(file string, opts ...ObjectOpt) (*ObjectInfo, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func (obs *obs) GetFile(name, file string, opts ...GetObjectOpt) error {
	_ = "STUB: not implemented"
	return nil
}

type GetObjectInfoOpt interface {
	configureGetInfo(opts *getObjectInfoOpts) error
}
type getObjectInfoOpts struct {
	ctx context.Context

	showDeleted bool
}

type getObjectInfoFn func(opts *getObjectInfoOpts) error

func (opt getObjectInfoFn) configureGetInfo(opts *getObjectInfoOpts) error {
	_ = "STUB: not implemented"
	return nil
}

func GetObjectInfoShowDeleted() GetObjectInfoOpt {
	_ = "STUB: not implemented"
	return *new(GetObjectInfoOpt)
}

func (ctx ContextOpt) configureGetInfo(opts *getObjectInfoOpts) error {
	_ = "STUB: not implemented"
	return nil
}

func (obs *obs) GetInfo(name string, opts ...GetObjectInfoOpt) (*ObjectInfo, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func (obs *obs) UpdateMeta(name string, meta *ObjectMeta) error {
	_ = "STUB: not implemented"
	return nil
}

func (obs *obs) Seal() error { _ = "STUB: not implemented"; return nil }

type objWatcher struct {
	updates chan *ObjectInfo
	sub     *Subscription
}

func (w *objWatcher) Updates() <-chan *ObjectInfo { _ = "STUB: not implemented"; return nil }

func (w *objWatcher) Stop() error { _ = "STUB: not implemented"; return nil }

func (obs *obs) Watch(opts ...WatchOpt) (ObjectWatcher, error) {
	_ = "STUB: not implemented"
	return *new(ObjectWatcher), nil
}

type ListObjectsOpt interface {
	configureListObjects(opts *listObjectOpts) error
}
type listObjectOpts struct {
	ctx context.Context

	showDeleted bool
}

type listObjectsFn func(opts *listObjectOpts) error

func (opt listObjectsFn) configureListObjects(opts *listObjectOpts) error {
	_ = "STUB: not implemented"
	return nil
}

func ListObjectsShowDeleted() ListObjectsOpt {
	_ = "STUB: not implemented"
	return *new(ListObjectsOpt)
}

func (ctx ContextOpt) configureListObjects(opts *listObjectOpts) error {
	_ = "STUB: not implemented"
	return nil
}

func (obs *obs) List(opts ...ListObjectsOpt) ([]*ObjectInfo, error) {
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

func (obs *obs) Status() (ObjectStoreStatus, error) {
	_ = "STUB: not implemented"
	return *new(ObjectStoreStatus), nil
}

func (o *objResult) Read(p []byte) (n int, err error) { _ = "STUB: not implemented"; return 0, nil }

func (o *objResult) Close() error { _ = "STUB: not implemented"; return nil }

func (o *objResult) setErr(err error) { _ = "STUB: not implemented"; return }

func (o *objResult) Info() (*ObjectInfo, error) { _ = "STUB: not implemented"; return nil, nil }

func (o *objResult) Error() error { _ = "STUB: not implemented"; return nil }

func (js *js) ObjectStoreNames(opts ...ObjectOpt) <-chan string {
	_ = "STUB: not implemented"
	return nil
}

func (js *js) ObjectStores(opts ...ObjectOpt) <-chan ObjectStoreStatus {
	_ = "STUB: not implemented"
	return nil
}
