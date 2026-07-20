//go:build go1.23

package nats

import (
	"iter"
	"time"
)

func (sub *Subscription) Msgs() iter.Seq2[*Msg, error] { _ = "STUB: not implemented"; return nil }

func (sub *Subscription) MsgsTimeout(timeout time.Duration) iter.Seq2[*Msg, error] {
	_ = "STUB: not implemented"
	return nil
}
