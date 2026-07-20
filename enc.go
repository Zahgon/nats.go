package nats

import (
	"reflect"
	"sync"
	"time"

	"github.com/nats-io/nats.go/encoders/builtin"
)

//lint:file-ignore SA1019 Ignore deprecation warnings for EncodedConn

type Encoder interface {
	Encode(subject string, v any) ([]byte, error)
	Decode(subject string, data []byte, vPtr any) error
}

var encMap map[string]Encoder
var encLock sync.Mutex

const (
	JSON_ENCODER    = "json"
	GOB_ENCODER     = "gob"
	DEFAULT_ENCODER = "default"
)

func init() {
	encMap = make(map[string]Encoder)

	RegisterEncoder(JSON_ENCODER, &builtin.JsonEncoder{})
	RegisterEncoder(GOB_ENCODER, &builtin.GobEncoder{})
	RegisterEncoder(DEFAULT_ENCODER, &builtin.DefaultEncoder{})
}

type EncodedConn struct {
	Conn *Conn
	Enc  Encoder
}

func NewEncodedConn(c *Conn, encType string) (*EncodedConn, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func RegisterEncoder(encType string, enc Encoder) { _ = "STUB: not implemented"; return }

func EncoderForType(encType string) Encoder { _ = "STUB: not implemented"; return *new(Encoder) }

func (c *EncodedConn) Publish(subject string, v any) error { _ = "STUB: not implemented"; return nil }

func (c *EncodedConn) PublishRequest(subject, reply string, v any) error {
	_ = "STUB: not implemented"
	return nil
}

func (c *EncodedConn) Request(subject string, v any, vPtr any, timeout time.Duration) error {
	_ = "STUB: not implemented"
	return nil
}

type Handler any

func argInfo(cb Handler) (reflect.Type, int) {
	_ = "STUB: not implemented"
	return *new(reflect.Type), 0
}

var emptyMsgType = reflect.TypeOf(&Msg{})

func (c *EncodedConn) Subscribe(subject string, cb Handler) (*Subscription, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func (c *EncodedConn) QueueSubscribe(subject, queue string, cb Handler) (*Subscription, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func (c *EncodedConn) subscribe(subject, queue string, cb Handler) (*Subscription, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func (c *EncodedConn) FlushTimeout(timeout time.Duration) (err error) {
	_ = "STUB: not implemented"
	return nil
}

func (c *EncodedConn) Flush() error { _ = "STUB: not implemented"; return nil }

func (c *EncodedConn) Close() { _ = "STUB: not implemented"; return }

func (c *EncodedConn) Drain() error { _ = "STUB: not implemented"; return nil }

func (c *EncodedConn) LastError() error { _ = "STUB: not implemented"; return nil }
