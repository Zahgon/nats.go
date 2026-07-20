package nats

import (
	"sync"
	"time"
)

var globalTimerPool timerPool

type timerPool struct {
	p sync.Pool
}

func (tp *timerPool) Get(d time.Duration) *time.Timer { _ = "STUB: not implemented"; return nil }

func (tp *timerPool) Put(t *time.Timer) { _ = "STUB: not implemented"; return }
