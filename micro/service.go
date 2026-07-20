package micro

import (
	"encoding/json"
	"errors"
	"regexp"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
)

type (
	Service interface {
		AddEndpoint(string, Handler, ...EndpointOpt) error

		AddGroup(string, ...GroupOpt) Group

		Info() Info

		Stats() Stats

		Reset()

		Stop() error

		Stopped() bool
	}

	Group interface {
		AddGroup(string, ...GroupOpt) Group

		AddEndpoint(string, Handler, ...EndpointOpt) error
	}

	EndpointOpt func(*endpointOpts) error
	GroupOpt    func(*groupOpts)

	endpointOpts struct {
		subject    string
		metadata   map[string]string
		queueGroup string
		qgDisabled bool
		msgLimit   int
		bytesLimit int
	}

	groupOpts struct {
		queueGroup string
		qgDisabled bool
	}

	ErrHandler func(Service, *NATSError)

	DoneHandler func(Service)

	StatsHandler func(*Endpoint) any

	ServiceIdentity struct {
		Name     string            `json:"name"`
		ID       string            `json:"id"`
		Version  string            `json:"version"`
		Metadata map[string]string `json:"metadata"`
	}

	Stats struct {
		ServiceIdentity
		Type      string           `json:"type"`
		Started   time.Time        `json:"started"`
		Endpoints []*EndpointStats `json:"endpoints"`
	}

	EndpointStats struct {
		Name                  string          `json:"name"`
		Subject               string          `json:"subject"`
		QueueGroup            string          `json:"queue_group"`
		NumRequests           int             `json:"num_requests"`
		NumErrors             int             `json:"num_errors"`
		LastError             string          `json:"last_error"`
		ProcessingTime        time.Duration   `json:"processing_time"`
		AverageProcessingTime time.Duration   `json:"average_processing_time"`
		Data                  json.RawMessage `json:"data,omitempty"`
	}

	Ping struct {
		ServiceIdentity
		Type string `json:"type"`
	}

	Info struct {
		ServiceIdentity
		Type        string         `json:"type"`
		Description string         `json:"description"`
		Endpoints   []EndpointInfo `json:"endpoints"`
	}

	EndpointInfo struct {
		Name       string            `json:"name"`
		Subject    string            `json:"subject"`
		QueueGroup string            `json:"queue_group"`
		Metadata   map[string]string `json:"metadata"`
	}

	Endpoint struct {
		EndpointConfig
		Name string

		service *service

		stats        EndpointStats
		subscription *nats.Subscription
	}

	group struct {
		service            *service
		prefix             string
		queueGroup         string
		queueGroupDisabled bool
	}

	Verb int64

	Config struct {
		Name string `json:"name"`

		Endpoint *EndpointConfig `json:"endpoint"`

		Version string `json:"version"`

		Description string `json:"description"`

		Metadata map[string]string `json:"metadata,omitempty"`

		QueueGroup string `json:"queue_group"`

		QueueGroupDisabled bool `json:"queue_group_disabled"`

		StatsHandler StatsHandler

		DoneHandler DoneHandler

		ErrorHandler ErrHandler
	}

	EndpointConfig struct {
		Subject string

		Handler Handler

		Metadata map[string]string `json:"metadata,omitempty"`

		QueueGroup string `json:"queue_group"`

		QueueGroupDisabled bool `json:"queue_group_disabled"`
	}

	NATSError struct {
		Subject     string
		Description string
		err         error
	}

	service struct {
		Config

		m            sync.Mutex
		id           string
		endpoints    []*Endpoint
		verbSubs     map[string]*nats.Subscription
		started      time.Time
		nc           *nats.Conn
		natsHandlers handlers
		stopped      bool

		asyncDispatcher asyncCallbacksHandler
	}

	handlers struct {
		closed   nats.ConnHandler
		asyncErr nats.ErrHandler
	}

	asyncCallbacksHandler struct {
		cbQueue chan func()
		closed  bool
	}
)

const (
	DefaultQueueGroup = "q"

	APIPrefix = "$SRV"
)

const (
	ErrorHeader     = "Nats-Service-Error"
	ErrorCodeHeader = "Nats-Service-Error-Code"
)

const (
	PingVerb Verb = iota
	StatsVerb
	InfoVerb
)

const (
	InfoResponseType  = "io.nats.micro.v1.info_response"
	PingResponseType  = "io.nats.micro.v1.ping_response"
	StatsResponseType = "io.nats.micro.v1.stats_response"
)

var (
	semVerRegexp  = regexp.MustCompile(`^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)(?:-((?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*)(?:\.(?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*))*))?(?:\+([0-9a-zA-Z-]+(?:\.[0-9a-zA-Z-]+)*))?$`)
	nameRegexp    = regexp.MustCompile(`^[A-Za-z0-9\-_]+$`)
	subjectRegexp = regexp.MustCompile(`^[^ >]*[>]?$`)
)

var (
	ErrConfigValidation = errors.New("validation")

	ErrVerbNotSupported = errors.New("unsupported verb")

	ErrServiceNameRequired = errors.New("service name is required to generate ID control subject")
)

func (s Verb) String() string { _ = "STUB: not implemented"; return "" }

func AddService(nc *nats.Conn, config Config) (Service, error) {
	_ = "STUB: not implemented"
	return *new(Service), nil
}

func (s *service) AddEndpoint(name string, handler Handler, opts ...EndpointOpt) error {
	_ = "STUB: not implemented"
	return nil
}

func addEndpoint(s *service, name, subject string, handler Handler, metadata map[string]string, queueGroup string, noQueue bool, msgLimit, bytesLimit int) error {
	_ = "STUB: not implemented"
	return nil
}

func (s *service) AddGroup(name string, opts ...GroupOpt) Group {
	_ = "STUB: not implemented"
	return *new(Group)
}

func (ac *asyncCallbacksHandler) run() { _ = "STUB: not implemented"; return }

func (ac *asyncCallbacksHandler) push(f func()) { _ = "STUB: not implemented"; return }

func (ac *asyncCallbacksHandler) close() { _ = "STUB: not implemented"; return }

func (c *Config) valid() error { _ = "STUB: not implemented"; return nil }

func (s *service) wrapConnectionEventCallbacks() { _ = "STUB: not implemented"; return }

func unwrapConnectionEventCallbacks(nc *nats.Conn, handlers handlers) {
	_ = "STUB: not implemented"
	return
}

func (s *service) matchSubscriptionSubject(subj string) (*Endpoint, bool) {
	_ = "STUB: not implemented"
	return nil, false
}

func matchEndpointSubject(endpointSubject, literalSubject string) bool {
	_ = "STUB: not implemented"
	return false
}

func (svc *service) addVerbHandlers(nc *nats.Conn, verb Verb, handler HandlerFunc) error {
	_ = "STUB: not implemented"
	return nil
}

func (s *service) addInternalHandler(nc *nats.Conn, verb Verb, kind, id, name string, handler HandlerFunc) error {
	_ = "STUB: not implemented"
	return nil
}

func (s *service) reqHandler(endpoint *Endpoint, req *request) { _ = "STUB: not implemented"; return }

func (s *service) Stop() error { _ = "STUB: not implemented"; return nil }

func (s *service) serviceIdentity() ServiceIdentity {
	_ = "STUB: not implemented"
	return *new(ServiceIdentity)
}

func (s *service) Info() Info { _ = "STUB: not implemented"; return *new(Info) }

func (s *service) Stats() Stats { _ = "STUB: not implemented"; return *new(Stats) }

func (s *service) Reset() { _ = "STUB: not implemented"; return }

func (s *service) Stopped() bool { _ = "STUB: not implemented"; return false }

func (e *NATSError) Error() string { _ = "STUB: not implemented"; return "" }

func (e *NATSError) Unwrap() error { _ = "STUB: not implemented"; return nil }

func (e *NATSError) Is(target error) bool { _ = "STUB: not implemented"; return false }

func (g *group) AddEndpoint(name string, handler Handler, opts ...EndpointOpt) error {
	_ = "STUB: not implemented"
	return nil
}

func resolveQueueGroup(customQG, parentQG string, disabled, parentDisabled bool) (string, bool) {
	_ = "STUB: not implemented"
	return "", false
}

func (g *group) AddGroup(name string, opts ...GroupOpt) Group {
	_ = "STUB: not implemented"
	return *new(Group)
}

func (e *Endpoint) stop() error { _ = "STUB: not implemented"; return nil }

func (e *Endpoint) reset() { _ = "STUB: not implemented"; return }

func ControlSubject(verb Verb, name, id string) (string, error) {
	_ = "STUB: not implemented"
	return "", nil
}

func WithEndpointSubject(subject string) EndpointOpt {
	_ = "STUB: not implemented"
	return *new(EndpointOpt)
}

func WithEndpointMetadata(metadata map[string]string) EndpointOpt {
	_ = "STUB: not implemented"
	return *new(EndpointOpt)
}

func WithEndpointMetadataKey(key, value string) EndpointOpt {
	_ = "STUB: not implemented"
	return *new(EndpointOpt)
}

func WithEndpointQueueGroup(queueGroup string) EndpointOpt {
	_ = "STUB: not implemented"
	return *new(EndpointOpt)
}

func WithEndpointQueueGroupDisabled() EndpointOpt {
	_ = "STUB: not implemented"
	return *new(EndpointOpt)
}

func WithEndpointPendingLimits(msgLimit, bytesLimit int) EndpointOpt {
	_ = "STUB: not implemented"
	return *new(EndpointOpt)
}

func WithGroupQueueGroup(queueGroup string) GroupOpt {
	_ = "STUB: not implemented"
	return *new(GroupOpt)
}

func WithGroupQueueGroupDisabled() GroupOpt { _ = "STUB: not implemented"; return *new(GroupOpt) }
