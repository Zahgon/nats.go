package nats

import (
	"context"
)

func (nc *Conn) RequestMsgWithContext(ctx context.Context, msg *Msg) (*Msg, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func (nc *Conn) RequestWithContext(ctx context.Context, subj string, data []byte) (*Msg, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func (nc *Conn) requestWithContext(ctx context.Context, subj string, hdr, data []byte) (*Msg, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func (nc *Conn) oldRequestWithContext(ctx context.Context, subj string, hdr, data []byte) (*Msg, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func (s *Subscription) nextMsgWithContext(ctx context.Context, pullSubInternal, waitIfNoMsg bool) (*Msg, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func (s *Subscription) NextMsgWithContext(ctx context.Context) (*Msg, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func (nc *Conn) FlushWithContext(ctx context.Context) error { _ = "STUB: not implemented"; return nil }

func (c *EncodedConn) RequestWithContext(ctx context.Context, subject string, v any, vPtr any) error {
	_ = "STUB: not implemented"
	return nil
}
