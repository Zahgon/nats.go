//go:build internal_testing

package nats

func (nc *Conn) AddMsgFilter(subject string, filter msgFilter) { _ = "STUB: not implemented"; return }

func (nc *Conn) RemoveMsgFilter(subject string) { _ = "STUB: not implemented"; return }

func IsJSControlMessage(msg *Msg) (bool, int) { _ = "STUB: not implemented"; return false, 0 }

func (nc *Conn) CloseTCPConn() { _ = "STUB: not implemented"; return }
