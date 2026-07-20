package protobuf

import (
	"errors"

	"github.com/nats-io/nats.go"
)

//lint:file-ignore SA1019 Ignore deprecation warnings for EncodedConn

const (
	PROTOBUF_ENCODER = "protobuf"
)

func init() {

	nats.RegisterEncoder(PROTOBUF_ENCODER, &ProtobufEncoder{})
}

type ProtobufEncoder struct {
}

var (
	ErrInvalidProtoMsgEncode = errors.New("nats: Invalid protobuf proto.Message object passed to encode")
	ErrInvalidProtoMsgDecode = errors.New("nats: Invalid protobuf proto.Message object passed to decode")
)

func (pb *ProtobufEncoder) Encode(subject string, v any) ([]byte, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func (pb *ProtobufEncoder) Decode(subject string, data []byte, vPtr any) error {
	_ = "STUB: not implemented"
	return nil
}
