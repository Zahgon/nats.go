package nats

import (
	"reflect"
)

func (c *EncodedConn) BindSendChan(subject string, channel any) error {
	_ = "STUB: not implemented"
	return nil
}

func chPublish(c *EncodedConn, chVal reflect.Value, subject string) {
	_ = "STUB: not implemented"
	return
}

func (c *EncodedConn) BindRecvChan(subject string, channel any) (*Subscription, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func (c *EncodedConn) BindRecvQueueChan(subject, queue string, channel any) (*Subscription, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func (c *EncodedConn) bindRecvChan(subject, queue string, channel any) (*Subscription, error) {
	_ = "STUB: not implemented"
	return nil, nil
}
