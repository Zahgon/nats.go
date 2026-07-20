package nats

type msgArg struct {
	subject []byte
	reply   []byte
	sid     int64
	hdr     int
	size    int
}

const MAX_CONTROL_LINE_SIZE = 4096

type parseState struct {
	state     int
	as        int
	drop      int
	hdr       int
	ma        msgArg
	argBuf    []byte
	msgBuf    []byte
	msgCopied bool
	scratch   [MAX_CONTROL_LINE_SIZE]byte
}

const (
	OP_START = iota
	OP_PLUS
	OP_PLUS_O
	OP_PLUS_OK
	OP_MINUS
	OP_MINUS_E
	OP_MINUS_ER
	OP_MINUS_ERR
	OP_MINUS_ERR_SPC
	MINUS_ERR_ARG
	OP_M
	OP_MS
	OP_MSG
	OP_MSG_SPC
	MSG_ARG
	MSG_PAYLOAD
	MSG_END
	OP_H
	OP_P
	OP_PI
	OP_PIN
	OP_PING
	OP_PO
	OP_PON
	OP_PONG
	OP_I
	OP_IN
	OP_INF
	OP_INFO
	OP_INFO_SPC
	INFO_ARG
)

func (nc *Conn) parse(buf []byte) error { _ = "STUB: not implemented"; return nil }

func (nc *Conn) cloneMsgArg() { _ = "STUB: not implemented"; return }

const argsLenMax = 4

func (nc *Conn) processMsgArgs(arg []byte) error { _ = "STUB: not implemented"; return nil }

func (nc *Conn) processHeaderMsgArgs(arg []byte) error { _ = "STUB: not implemented"; return nil }

const (
	ascii_0 = 48
	ascii_9 = 57
)

func parseInt64(d []byte) (n int64) { _ = "STUB: not implemented"; return 0 }
