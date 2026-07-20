package nats

import (
	"io"
	"net/http"
	"net/url"

	"github.com/klauspost/compress/flate"
)

type wsOpCode int

const (
	wsTextMessage   = wsOpCode(1)
	wsBinaryMessage = wsOpCode(2)
	wsCloseMessage  = wsOpCode(8)
	wsPingMessage   = wsOpCode(9)
	wsPongMessage   = wsOpCode(10)

	wsFinalBit = 1 << 7
	wsRsv1Bit  = 1 << 6
	wsRsv2Bit  = 1 << 5
	wsRsv3Bit  = 1 << 4

	wsMaskBit = 1 << 7

	wsContinuationFrame     = 0
	wsMaxFrameHeaderSize    = 14
	wsMaxControlPayloadSize = 125
	wsCloseSatusSize        = 2

	wsMaxMsgPayloadMultiple = 8

	wsMaxMsgPayloadLimit = 64 * 1024 * 1024

	wsCloseStatusNormalClosure      = 1000
	wsCloseStatusNoStatusReceived   = 1005
	wsCloseStatusAbnormalClosure    = 1006
	wsCloseStatusInvalidPayloadData = 1007

	wsScheme    = "ws"
	wsSchemeTLS = "wss"

	wsPMCExtension      = "permessage-deflate"
	wsPMCSrvNoCtx       = "server_no_context_takeover"
	wsPMCCliNoCtx       = "client_no_context_takeover"
	wsPMCReqHeaderValue = wsPMCExtension + "; " + wsPMCSrvNoCtx + "; " + wsPMCCliNoCtx
)

var wsGUID = []byte("258EAFA5-E914-47DA-95CA-C5AB0DC85B11")

var compressFinalBlock = []byte{0x00, 0x00, 0xff, 0xff, 0x01, 0x00, 0x00, 0xff, 0xff}

type websocketReader struct {
	r        io.Reader
	pending  [][]byte
	compress bool
	ib       []byte
	ff       bool
	fc       bool
	nl       bool
	dc       *wsDecompressor
	nc       *Conn
	closeErr error
}

type wsDecompressor struct {
	flate io.ReadCloser
	bufs  [][]byte
	off   int
}

type websocketWriter struct {
	w          io.Writer
	compress   bool
	compressor *flate.Writer
	ctrlFrames [][]byte
	cm         []byte
	cmDone     bool
	noMoreSend bool
}

func (d *wsDecompressor) Read(dst []byte) (int, error) { _ = "STUB: not implemented"; return 0, nil }

func (d *wsDecompressor) nextBuf() []byte { _ = "STUB: not implemented"; return nil }

func (d *wsDecompressor) ReadByte() (byte, error) { _ = "STUB: not implemented"; return 0, nil }

func (d *wsDecompressor) addBuf(b []byte) { _ = "STUB: not implemented"; return }

func (d *wsDecompressor) decompress() ([]byte, error) { _ = "STUB: not implemented"; return nil, nil }

func wsNewReader(r io.Reader) *websocketReader { _ = "STUB: not implemented"; return nil }

func (r *websocketReader) maxFrameSize() uint64 { _ = "STUB: not implemented"; return 0 }

func (r *websocketReader) doneWithConnect() { _ = "STUB: not implemented"; return }

func (r *websocketReader) Read(p []byte) (int, error) { _ = "STUB: not implemented"; return 0, nil }

func (r *websocketReader) addCBuf(b []byte) { _ = "STUB: not implemented"; return }

func (r *websocketReader) drainPending(p []byte) int { _ = "STUB: not implemented"; return 0 }

func wsGet(r io.Reader, buf []byte, pos, needed int) ([]byte, int, error) {
	_ = "STUB: not implemented"
	return nil, 0, nil
}

func (r *websocketReader) handleControlFrame(frameType wsOpCode, buf []byte, pos, rem int) (int, error) {
	_ = "STUB: not implemented"
	return 0, nil
}

func (w *websocketWriter) Write(p []byte) (int, error) { _ = "STUB: not implemented"; return 0, nil }

func (w *websocketWriter) writeCtrlFrames() (int, error) { _ = "STUB: not implemented"; return 0, nil }

func (w *websocketWriter) writeCloseMsg() (int, error) { _ = "STUB: not implemented"; return 0, nil }

func wsMaskBuf(key, buf []byte) { _ = "STUB: not implemented"; return }

func wsCreateFrameHeader(compressed bool, frameType wsOpCode, l int) ([]byte, []byte) {
	_ = "STUB: not implemented"
	return nil, nil
}

func wsFillFrameHeader(fh []byte, compressed bool, frameType wsOpCode, l int) (int, []byte) {
	_ = "STUB: not implemented"
	return 0, nil
}

func (nc *Conn) wsInitHandshake(u *url.URL) error { _ = "STUB: not implemented"; return nil }

func (nc *Conn) wsClose() { _ = "STUB: not implemented"; return }

func (nc *Conn) wsEnqueueCloseMsg(needsLock bool, status int, payload string) {
	_ = "STUB: not implemented"
	return
}

func (nc *Conn) wsEnqueueCloseMsgLocked(status int, payload string) {
	_ = "STUB: not implemented"
	return
}

func (nc *Conn) wsEnqueueControlMsg(needsLock bool, frameType wsOpCode, payload []byte) {
	_ = "STUB: not implemented"
	return
}

func (nc *Conn) wsUpdateConnectionHeaders(req *http.Request) error {
	_ = "STUB: not implemented"
	return nil
}

func wsPMCExtensionSupport(header http.Header) (bool, bool) {
	_ = "STUB: not implemented"
	return false, false
}

func wsMakeChallengeKey() (string, error) { _ = "STUB: not implemented"; return "", nil }

func wsAcceptKey(key string) string { _ = "STUB: not implemented"; return "" }

func wsIsControlFrame(frameType wsOpCode) bool { _ = "STUB: not implemented"; return false }

func isWebsocketScheme(u *url.URL) bool { _ = "STUB: not implemented"; return false }
