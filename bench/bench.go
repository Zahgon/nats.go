package bench

import (
	"time"

	"github.com/nats-io/nats.go"
)

type Sample struct {
	JobMsgCnt int
	MsgCnt    uint64
	MsgBytes  uint64
	IOBytes   uint64
	Start     time.Time
	End       time.Time
}

type SampleGroup struct {
	Sample
	Samples []*Sample
}

type Benchmark struct {
	Sample
	Name       string
	RunID      string
	Pubs       *SampleGroup
	Subs       *SampleGroup
	subChannel chan *Sample
	pubChannel chan *Sample
}

func NewBenchmark(name string, subCnt, pubCnt int) *Benchmark {
	_ = "STUB: not implemented"
	return nil
}

func (bm *Benchmark) Close() { _ = "STUB: not implemented"; return }

func (bm *Benchmark) AddSubSample(s *Sample) { _ = "STUB: not implemented"; return }

func (bm *Benchmark) AddPubSample(s *Sample) { _ = "STUB: not implemented"; return }

func (bm *Benchmark) CSV() string { _ = "STUB: not implemented"; return "" }

func NewSample(jobCount int, msgSize int, start, end time.Time, nc *nats.Conn) *Sample {
	_ = "STUB: not implemented"
	return nil
}

func (s *Sample) Throughput() float64 { _ = "STUB: not implemented"; return 0 }

func (s *Sample) Rate() int64 { _ = "STUB: not implemented"; return 0 }

func (s *Sample) String() string { _ = "STUB: not implemented"; return "" }

func (s *Sample) Duration() time.Duration { _ = "STUB: not implemented"; return *new(time.Duration) }

func (s *Sample) Seconds() float64 { _ = "STUB: not implemented"; return 0 }

func NewSampleGroup() *SampleGroup { _ = "STUB: not implemented"; return nil }

func (sg *SampleGroup) Statistics() string { _ = "STUB: not implemented"; return "" }

func (sg *SampleGroup) MinRate() int64 { _ = "STUB: not implemented"; return 0 }

func (sg *SampleGroup) MaxRate() int64 { _ = "STUB: not implemented"; return 0 }

func (sg *SampleGroup) AvgRate() int64 { _ = "STUB: not implemented"; return 0 }

func (sg *SampleGroup) StdDev() float64 { _ = "STUB: not implemented"; return 0 }

func (sg *SampleGroup) AddSample(e *Sample) { _ = "STUB: not implemented"; return }

func (sg *SampleGroup) HasSamples() bool { _ = "STUB: not implemented"; return false }

func (bm *Benchmark) Report() string { _ = "STUB: not implemented"; return "" }

func commaFormat(n int64) string { _ = "STUB: not implemented"; return "" }

func HumanBytes(bytes float64, si bool) string { _ = "STUB: not implemented"; return "" }

func MsgsPerClient(numMsgs, numClients int) []int { _ = "STUB: not implemented"; return nil }
