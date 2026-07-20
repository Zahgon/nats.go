package nats

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"math/rand"
	"net"
	"net/http"
	"net/textproto"
	"net/url"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nkeys"
)

const (
	Version                   = "1.52.0"
	DefaultURL                = "nats://127.0.0.1:4222"
	DefaultPort               = 4222
	DefaultMaxReconnect       = 60
	DefaultReconnectWait      = 2 * time.Second
	DefaultReconnectJitter    = 100 * time.Millisecond
	DefaultReconnectJitterTLS = time.Second
	DefaultTimeout            = 2 * time.Second
	DefaultPingInterval       = 2 * time.Minute
	DefaultMaxPingOut         = 2
	DefaultMaxChanLen         = 64 * 1024
	DefaultReconnectBufSize   = 8 * 1024 * 1024
	DefaultWriteBufSize       = defaultBufSize
	RequestChanLen            = 8
	DefaultDrainTimeout       = 30 * time.Second
	DefaultFlusherTimeout     = time.Minute
	LangString                = "go"
)

const (
	STALE_CONNECTION = "stale connection"

	PERMISSIONS_ERR = "permissions violation"

	AUTHORIZATION_ERR = "authorization violation"

	AUTHENTICATION_EXPIRED_ERR = "user authentication expired"

	AUTHENTICATION_REVOKED_ERR = "user authentication revoked"

	ACCOUNT_AUTHENTICATION_EXPIRED_ERR = "account authentication expired"

	MAX_CONNECTIONS_ERR = "maximum connections exceeded"

	MAX_ACCOUNT_CONNECTIONS_ERR = `maximum account active connections exceeded`

	MAX_SUBSCRIPTIONS_ERR = "maximum subscriptions exceeded"
)

var (
	ErrConnectionClosed              = errors.New("nats: connection closed")
	ErrConnectionDraining            = errors.New("nats: connection draining")
	ErrDrainTimeout                  = errors.New("nats: draining connection timed out")
	ErrConnectionReconnecting        = errors.New("nats: connection reconnecting")
	ErrSecureConnRequired            = errors.New("nats: secure connection required")
	ErrSecureConnWanted              = errors.New("nats: secure connection not available")
	ErrBadSubscription               = errors.New("nats: invalid subscription")
	ErrTypeSubscription              = errors.New("nats: invalid subscription type")
	ErrBadSubject                    = errors.New("nats: invalid subject")
	ErrBadQueueName                  = errors.New("nats: invalid queue name")
	ErrSlowConsumer                  = errors.New("nats: slow consumer, messages dropped")
	ErrTimeout                       = errors.New("nats: timeout")
	ErrBadTimeout                    = errors.New("nats: timeout invalid")
	ErrAuthorization                 = errors.New("nats: authorization violation")
	ErrAuthExpired                   = errors.New("nats: authentication expired")
	ErrAuthRevoked                   = errors.New("nats: authentication revoked")
	ErrPermissionViolation           = errors.New("nats: permissions violation")
	ErrAccountAuthExpired            = errors.New("nats: account authentication expired")
	ErrNoServers                     = errors.New("nats: no servers available for connection")
	ErrJsonParse                     = errors.New("nats: connect message, json parse error")
	ErrChanArg                       = errors.New("nats: argument needs to be a channel type")
	ErrMaxPayload                    = errors.New("nats: maximum payload exceeded")
	ErrMaxMessages                   = errors.New("nats: maximum messages delivered")
	ErrSyncSubRequired               = errors.New("nats: illegal call on an async subscription")
	ErrMultipleTLSConfigs            = errors.New("nats: multiple tls.Configs not allowed")
	ErrClientCertOrRootCAsRequired   = errors.New("nats: at least one of certCB or rootCAsCB must be set")
	ErrNoInfoReceived                = errors.New("nats: protocol exception, INFO not received")
	ErrReconnectBufExceeded          = errors.New("nats: outbound buffer limit exceeded")
	ErrInvalidConnection             = errors.New("nats: invalid connection")
	ErrInvalidMsg                    = errors.New("nats: invalid message or message nil")
	ErrInvalidArg                    = errors.New("nats: invalid argument")
	ErrInvalidContext                = errors.New("nats: invalid context")
	ErrNoDeadlineContext             = errors.New("nats: context requires a deadline")
	ErrNoEchoNotSupported            = errors.New("nats: no echo option not supported by this server")
	ErrClientIDNotSupported          = errors.New("nats: client ID not supported by this server")
	ErrUserButNoSigCB                = errors.New("nats: user callback defined without a signature handler")
	ErrNkeyButNoSigCB                = errors.New("nats: nkey defined without a signature handler")
	ErrNoUserCB                      = errors.New("nats: user callback not defined")
	ErrNkeyAndUser                   = errors.New("nats: user callback and nkey defined")
	ErrNkeysNotSupported             = errors.New("nats: nkeys not supported by the server")
	ErrStaleConnection               = errors.New("nats: " + STALE_CONNECTION)
	ErrTokenAlreadySet               = errors.New("nats: token and token handler both set")
	ErrUserInfoAlreadySet            = errors.New("nats: cannot set user info callback and user/pass")
	ErrMsgNotBound                   = errors.New("nats: message is not bound to subscription/connection")
	ErrMsgNoReply                    = errors.New("nats: message does not have a reply")
	ErrClientIPNotSupported          = errors.New("nats: client IP not supported by this server")
	ErrDisconnected                  = errors.New("nats: server is disconnected")
	ErrHeadersNotSupported           = errors.New("nats: headers not supported by this server")
	ErrBadHeaderMsg                  = errors.New("nats: message could not decode headers")
	ErrNoResponders                  = errors.New("nats: no responders available for request")
	ErrMaxConnectionsExceeded        = errors.New("nats: server maximum connections exceeded")
	ErrMaxAccountConnectionsExceeded = errors.New("nats: maximum account active connections exceeded")
	ErrConnectionNotTLS              = errors.New("nats: connection is not tls")
	ErrTLS                           = errors.New("nats: tls error")
	ErrMaxSubscriptionsExceeded      = errors.New("nats: server maximum subscriptions exceeded")
	ErrWebSocketHeadersAlreadySet    = errors.New("nats: websocket connection headers already set")
	ErrServerNotInPool               = errors.New("nats: selected server is not in the pool")
	ErrMixingWebsocketSchemes        = errors.New("nats: mixing of websocket and non websocket URLs is not allowed")
)

func GetDefaultOptions() Options { _ = "STUB: not implemented"; return *new(Options) }

var DefaultOptions = GetDefaultOptions()

type Status int

const (
	DISCONNECTED = Status(iota)
	CONNECTED
	CLOSED
	RECONNECTING
	CONNECTING
	DRAINING_SUBS
	DRAINING_PUBS
)

func (s Status) String() string { _ = "STUB: not implemented"; return "" }

type ConnHandler func(*Conn)

type ConnErrHandler func(*Conn, error)

type ErrHandler func(*Conn, *Subscription, error)

type UserJWTHandler func() (string, error)

type TLSCertHandler func() (tls.Certificate, error)

type RootCAsHandler func() (*x509.CertPool, error)

type SignatureHandler func([]byte) ([]byte, error)

type AuthTokenHandler func() string

type UserInfoCB func() (string, string)

type ReconnectDelayHandler func(attempts int) time.Duration

type WebSocketHeadersHandler func() (http.Header, error)

type ReconnectToServerHandler func([]Server, ServerInfo) (*Server, time.Duration)

type asyncCB struct {
	f    func()
	next *asyncCB
}

type asyncCallbacksHandler struct {
	mu   sync.Mutex
	cond *sync.Cond
	head *asyncCB
	tail *asyncCB
}

type Option func(*Options) error

type CustomDialer interface {
	Dial(network, address string) (net.Conn, error)
}

type InProcessConnProvider interface {
	InProcessConn() (net.Conn, error)
}

type Options struct {
	Url string

	InProcessServer InProcessConnProvider

	Servers []string

	NoRandomize bool

	NoEcho bool

	Name string

	Verbose bool

	Pedantic bool

	Secure bool

	TLSConfig *tls.Config

	TLSCertCB TLSCertHandler

	TLSHandshakeFirst bool

	RootCAsCB RootCAsHandler

	AllowReconnect bool

	MaxReconnect int

	ReconnectWait time.Duration

	CustomReconnectDelayCB ReconnectDelayHandler

	ReconnectJitter time.Duration

	ReconnectJitterTLS time.Duration

	Timeout time.Duration

	DrainTimeout time.Duration

	FlusherTimeout time.Duration

	ReconnectOnFlusherError bool

	PingInterval time.Duration

	MaxPingsOut int

	ClosedCB ConnHandler

	DisconnectedCB ConnHandler

	DisconnectedErrCB ConnErrHandler

	ConnectedCB ConnHandler

	ReconnectedCB ConnHandler

	DiscoveredServersCB ConnHandler

	AsyncErrorCB ErrHandler

	ReconnectErrCB ConnErrHandler

	ReconnectToServerCB ReconnectToServerHandler

	ReconnectBufSize int

	SubChanLen int

	UserJWT UserJWTHandler

	Nkey string

	SignatureCB SignatureHandler

	User string

	Password string

	UserInfo UserInfoCB

	Token string

	TokenHandler AuthTokenHandler

	Dialer *net.Dialer

	CustomDialer CustomDialer

	UseOldRequestStyle bool

	NoCallbacksAfterClientClose bool

	LameDuckModeHandler ConnHandler

	RetryOnFailedConnect bool

	Compression bool

	ProxyPath string

	InboxPrefix string

	IgnoreAuthErrorAbort bool

	SkipHostLookup bool

	PermissionErrOnSubscribe bool

	WebSocketConnectionHeaders http.Header

	WebSocketConnectionHeadersHandler WebSocketHeadersHandler

	SkipSubjectValidation bool

	IgnoreDiscoveredServers bool

	WriteBufferSize int
}

const (
	scratchSize = 512

	defaultBufSize = 32768

	flushChanSize = 1

	srvPoolSize = 4

	nuidSize = 22

	defaultWSPortString  = "80"
	defaultWSSPortString = "443"
	defaultPortString    = "4222"
)

type Conn struct {
	Statistics
	mu sync.RWMutex

	Opts          Options
	wg            sync.WaitGroup
	srvPool       []*Server
	current       *Server
	urls          map[string]struct{}
	conn          net.Conn
	bw            *natsWriter
	br            *natsReader
	fch           chan struct{}
	info          ServerInfo
	ssid          int64
	subsMu        sync.RWMutex
	subs          map[int64]*Subscription
	ach           *asyncCallbacksHandler
	pongs         []chan struct{}
	scratch       [scratchSize]byte
	status        Status
	statListeners map[Status]map[chan Status]struct{}
	initc         bool
	err           error
	ps            *parseState
	ptmr          *time.Timer
	pout          int
	ar            bool
	rqch          chan struct{}
	ws            bool

	respSub       string
	respSubPrefix string
	respSubLen    int
	respMux       *Subscription
	respMap       map[string]chan *Msg
	respRand      *rand.Rand

	filters map[string]msgFilter
}

type natsReader struct {
	r   io.Reader
	buf []byte
	off int
	n   int
}

type natsWriter struct {
	w       io.Writer
	bufs    []byte
	limit   int
	pending *bytes.Buffer
	plimit  int
}

type Subscription struct {
	mu  sync.Mutex
	sid int64

	Subject string

	Queue string

	jsi *jsSub

	delivered      uint64
	max            uint64
	conn           *Conn
	mcb            MsgHandler
	mch            chan *Msg
	errCh          chan (error)
	closed         bool
	sc             bool
	connClosed     bool
	draining       bool
	status         SubStatus
	statListeners  map[chan SubStatus][]SubStatus
	permissionsErr error

	typ SubscriptionType

	pHead *Msg
	pTail *Msg
	pCond *sync.Cond
	pDone func(subject string)

	pMsgs       int
	pBytes      int
	pMsgsMax    int
	pBytesMax   int
	pMsgsLimit  int
	pBytesLimit int
	dropped     int
}

type SubStatus int

const (
	SubscriptionActive = SubStatus(iota)
	SubscriptionDraining
	SubscriptionClosed
	SubscriptionSlowConsumer
)

func (s SubStatus) String() string { _ = "STUB: not implemented"; return "" }

type Msg struct {
	Subject string
	Reply   string
	Header  Header
	Data    []byte
	Sub     *Subscription

	next    *Msg
	wsz     int
	barrier *barrierInfo
	ackd    uint32
}

func (m *Msg) Equal(msg *Msg) bool { _ = "STUB: not implemented"; return false }

func (m *Msg) Size() int { _ = "STUB: not implemented"; return 0 }

func (m *Msg) headerBytes() ([]byte, error) { _ = "STUB: not implemented"; return nil, nil }

type asciiSet [256]bool

func (as *asciiSet) MatchesString(s string) bool { _ = "STUB: not implemented"; return false }

var validHeaderKeyChars = makeValidHeaderKeyAsciiSet()

func makeValidHeaderKeyAsciiSet() asciiSet { _ = "STUB: not implemented"; return *new(asciiSet) }

func isHeaderKeyValid(k string) bool { _ = "STUB: not implemented"; return false }

var headerValueNewlineReplacer = strings.NewReplacer("\r", " ", "\n", " ")

func writeHeaderValue(buffer *bytes.Buffer, value string) { _ = "STUB: not implemented"; return }

type barrierInfo struct {
	refs int64
	f    func()
}

type Statistics struct {
	InMsgs     uint64
	OutMsgs    uint64
	InBytes    uint64
	OutBytes   uint64
	Reconnects uint64
}

type Server struct {
	URL        *url.URL
	Reconnects int
	didConnect bool
	lastErr    error
	isImplicit bool
	tlsName    string
}

func (s Server) clone() Server { _ = "STUB: not implemented"; return *new(Server) }

type ServerInfo struct {
	ID           string   `json:"server_id"`
	Name         string   `json:"server_name"`
	Proto        int      `json:"proto"`
	Version      string   `json:"version"`
	Host         string   `json:"host"`
	Port         int      `json:"port"`
	Headers      bool     `json:"headers"`
	AuthRequired bool     `json:"auth_required,omitempty"`
	TLSRequired  bool     `json:"tls_required,omitempty"`
	TLSAvailable bool     `json:"tls_available,omitempty"`
	MaxPayload   int64    `json:"max_payload"`
	CID          uint64   `json:"client_id,omitempty"`
	ClientIP     string   `json:"client_ip,omitempty"`
	Nonce        string   `json:"nonce,omitempty"`
	Cluster      string   `json:"cluster,omitempty"`
	ConnectURLs  []string `json:"connect_urls,omitempty"`
	LameDuckMode bool     `json:"ldm,omitempty"`

	JetStream bool `json:"jetstream,omitempty"`

	IsSystemAccount bool `json:"acc_is_sys,omitempty"`

	JSApiLevel int `json:"api_lvl,omitempty"`
}

const (
	_ = iota

	clientProtoInfo
)

type connectInfo struct {
	Verbose      bool   `json:"verbose"`
	Pedantic     bool   `json:"pedantic"`
	UserJWT      string `json:"jwt,omitempty"`
	Nkey         string `json:"nkey,omitempty"`
	Signature    string `json:"sig,omitempty"`
	User         string `json:"user,omitempty"`
	Pass         string `json:"pass,omitempty"`
	Token        string `json:"auth_token,omitempty"`
	TLS          bool   `json:"tls_required"`
	Name         string `json:"name"`
	Lang         string `json:"lang"`
	Version      string `json:"version"`
	Protocol     int    `json:"protocol"`
	Echo         bool   `json:"echo"`
	Headers      bool   `json:"headers"`
	NoResponders bool   `json:"no_responders"`
}

type MsgHandler func(msg *Msg)

func Connect(url string, options ...Option) (*Conn, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func Name(name string) Option { _ = "STUB: not implemented"; return *new(Option) }

func InProcessServer(server InProcessConnProvider) Option {
	_ = "STUB: not implemented"
	return *new(Option)
}

func Secure(tls ...*tls.Config) Option { _ = "STUB: not implemented"; return *new(Option) }

func ClientTLSConfig(certCB TLSCertHandler, rootCAsCB RootCAsHandler) Option {
	_ = "STUB: not implemented"
	return *new(Option)
}

func RootCAs(file ...string) Option { _ = "STUB: not implemented"; return *new(Option) }

func ClientCert(certFile, keyFile string) Option { _ = "STUB: not implemented"; return *new(Option) }

func NoReconnect() Option { _ = "STUB: not implemented"; return *new(Option) }

func DontRandomize() Option { _ = "STUB: not implemented"; return *new(Option) }

func NoEcho() Option { _ = "STUB: not implemented"; return *new(Option) }

func ReconnectWait(t time.Duration) Option { _ = "STUB: not implemented"; return *new(Option) }

func MaxReconnects(max int) Option { _ = "STUB: not implemented"; return *new(Option) }

func ReconnectJitter(jitter, jitterForTLS time.Duration) Option {
	_ = "STUB: not implemented"
	return *new(Option)
}

func CustomReconnectDelay(cb ReconnectDelayHandler) Option {
	_ = "STUB: not implemented"
	return *new(Option)
}

func ReconnectToServer(cb ReconnectToServerHandler) Option {
	_ = "STUB: not implemented"
	return *new(Option)
}

func PingInterval(t time.Duration) Option { _ = "STUB: not implemented"; return *new(Option) }

func MaxPingsOutstanding(max int) Option { _ = "STUB: not implemented"; return *new(Option) }

func ReconnectBufSize(size int) Option { _ = "STUB: not implemented"; return *new(Option) }

func WriteBufferSize(size int) Option { _ = "STUB: not implemented"; return *new(Option) }

func Timeout(t time.Duration) Option { _ = "STUB: not implemented"; return *new(Option) }

func FlusherTimeout(t time.Duration) Option { _ = "STUB: not implemented"; return *new(Option) }

func ReconnectOnFlusherError() Option { _ = "STUB: not implemented"; return *new(Option) }

func DrainTimeout(t time.Duration) Option { _ = "STUB: not implemented"; return *new(Option) }

func DisconnectErrHandler(cb ConnErrHandler) Option { _ = "STUB: not implemented"; return *new(Option) }

func DisconnectHandler(cb ConnHandler) Option { _ = "STUB: not implemented"; return *new(Option) }

func ConnectHandler(cb ConnHandler) Option { _ = "STUB: not implemented"; return *new(Option) }

func ReconnectHandler(cb ConnHandler) Option { _ = "STUB: not implemented"; return *new(Option) }

func ReconnectErrHandler(cb ConnErrHandler) Option { _ = "STUB: not implemented"; return *new(Option) }

func ClosedHandler(cb ConnHandler) Option { _ = "STUB: not implemented"; return *new(Option) }

func DiscoveredServersHandler(cb ConnHandler) Option {
	_ = "STUB: not implemented"
	return *new(Option)
}

func ErrorHandler(cb ErrHandler) Option { _ = "STUB: not implemented"; return *new(Option) }

func UserInfo(user, password string) Option { _ = "STUB: not implemented"; return *new(Option) }

func UserInfoHandler(cb UserInfoCB) Option { _ = "STUB: not implemented"; return *new(Option) }

func Token(token string) Option { _ = "STUB: not implemented"; return *new(Option) }

func TokenHandler(cb AuthTokenHandler) Option { _ = "STUB: not implemented"; return *new(Option) }

func UserCredentials(userOrChainedFile string, seedFiles ...string) Option {
	_ = "STUB: not implemented"
	return *new(Option)
}

func UserCredentialBytes(userOrChainedFileBytes []byte, seedFiles ...[]byte) Option {
	_ = "STUB: not implemented"
	return *new(Option)
}

func UserJWTAndSeed(jwt string, seed string) Option { _ = "STUB: not implemented"; return *new(Option) }

func UserJWT(userCB UserJWTHandler, sigCB SignatureHandler) Option {
	_ = "STUB: not implemented"
	return *new(Option)
}

func Nkey(pubKey string, sigCB SignatureHandler) Option {
	_ = "STUB: not implemented"
	return *new(Option)
}

func SyncQueueLen(max int) Option { _ = "STUB: not implemented"; return *new(Option) }

func Dialer(dialer *net.Dialer) Option { _ = "STUB: not implemented"; return *new(Option) }

func SetCustomDialer(dialer CustomDialer) Option { _ = "STUB: not implemented"; return *new(Option) }

func UseOldRequestStyle() Option { _ = "STUB: not implemented"; return *new(Option) }

func NoCallbacksAfterClientClose() Option { _ = "STUB: not implemented"; return *new(Option) }

func LameDuckModeHandler(cb ConnHandler) Option { _ = "STUB: not implemented"; return *new(Option) }

func RetryOnFailedConnect(retry bool) Option { _ = "STUB: not implemented"; return *new(Option) }

func Compression(enabled bool) Option { _ = "STUB: not implemented"; return *new(Option) }

func ProxyPath(path string) Option { _ = "STUB: not implemented"; return *new(Option) }

func CustomInboxPrefix(p string) Option { _ = "STUB: not implemented"; return *new(Option) }

func IgnoreAuthErrorAbort() Option { _ = "STUB: not implemented"; return *new(Option) }

func SkipHostLookup() Option { _ = "STUB: not implemented"; return *new(Option) }

func PermissionErrOnSubscribe(enabled bool) Option { _ = "STUB: not implemented"; return *new(Option) }

func TLSHandshakeFirst() Option { _ = "STUB: not implemented"; return *new(Option) }

func WebSocketConnectionHeaders(headers http.Header) Option {
	_ = "STUB: not implemented"
	return *new(Option)
}

func WebSocketConnectionHeadersHandler(cb WebSocketHeadersHandler) Option {
	_ = "STUB: not implemented"
	return *new(Option)
}

func SkipSubjectValidation() Option { _ = "STUB: not implemented"; return *new(Option) }

func IgnoreDiscoveredServers() Option { _ = "STUB: not implemented"; return *new(Option) }

func (nc *Conn) SetDisconnectHandler(dcb ConnHandler) { _ = "STUB: not implemented"; return }

func (nc *Conn) SetDisconnectErrHandler(dcb ConnErrHandler) { _ = "STUB: not implemented"; return }

func (nc *Conn) DisconnectErrHandler() ConnErrHandler {
	_ = "STUB: not implemented"
	return *new(ConnErrHandler)
}

func (nc *Conn) SetReconnectHandler(rcb ConnHandler) { _ = "STUB: not implemented"; return }

func (nc *Conn) ReconnectHandler() ConnHandler { _ = "STUB: not implemented"; return *new(ConnHandler) }

func (nc *Conn) SetDiscoveredServersHandler(dscb ConnHandler) { _ = "STUB: not implemented"; return }

func (nc *Conn) DiscoveredServersHandler() ConnHandler {
	_ = "STUB: not implemented"
	return *new(ConnHandler)
}

func (nc *Conn) SetClosedHandler(cb ConnHandler) { _ = "STUB: not implemented"; return }

func (nc *Conn) ClosedHandler() ConnHandler { _ = "STUB: not implemented"; return *new(ConnHandler) }

func (nc *Conn) SetErrorHandler(cb ErrHandler) { _ = "STUB: not implemented"; return }

func (nc *Conn) ErrorHandler() ErrHandler { _ = "STUB: not implemented"; return *new(ErrHandler) }

func processUrlString(url string) []string { _ = "STUB: not implemented"; return nil }

func (o Options) Connect() (*Conn, error) { _ = "STUB: not implemented"; return nil, nil }

func defaultErrHandler(nc *Conn, sub *Subscription, err error) { _ = "STUB: not implemented"; return }

const (
	_CRLF_   = "\r\n"
	_EMPTY_  = ""
	_SPC_    = " "
	_PUB_P_  = "PUB "
	_HPUB_P_ = "HPUB "
)

var _CRLF_BYTES_ = []byte(_CRLF_)

const (
	_OK_OP_   = "+OK"
	_ERR_OP_  = "-ERR"
	_PONG_OP_ = "PONG"
	_INFO_OP_ = "INFO"
)

const (
	connectProto = "CONNECT %s" + _CRLF_
	pingProto    = "PING" + _CRLF_
	pongProto    = "PONG" + _CRLF_
	subProto     = "SUB %s %s %d" + _CRLF_
	unsubProto   = "UNSUB %d %s" + _CRLF_
	okProto      = _OK_OP_ + _CRLF_
)

func (nc *Conn) currentServer() (int, *Server) { _ = "STUB: not implemented"; return 0, nil }

func (nc *Conn) selectNextServer() (*Server, error) { _ = "STUB: not implemented"; return nil, nil }

func (nc *Conn) pickServer() error { _ = "STUB: not implemented"; return nil }

const tlsScheme = "tls"

func (nc *Conn) setupServerPool() error { _ = "STUB: not implemented"; return nil }

func (nc *Conn) connScheme() string { _ = "STUB: not implemented"; return "" }

func hostIsIP(u *url.URL) bool { _ = "STUB: not implemented"; return false }

func (nc *Conn) parseServerURL(sURL string, implicit, saveTLSName bool) (*Server, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func (nc *Conn) addURLToPool(sURL string, implicit, saveTLSName bool) error {
	_ = "STUB: not implemented"
	return nil
}

func (nc *Conn) shufflePool(offset int) { _ = "STUB: not implemented"; return }

func (nc *Conn) newReaderWriter() { _ = "STUB: not implemented"; return }

func (nc *Conn) bindToNewConn() { _ = "STUB: not implemented"; return }

func (nc *Conn) newWriter() io.Writer { _ = "STUB: not implemented"; return *new(io.Writer) }

func (w *natsWriter) appendString(str string) error { _ = "STUB: not implemented"; return nil }

func (w *natsWriter) appendBufs(bufs ...[]byte) error { _ = "STUB: not implemented"; return nil }

func (w *natsWriter) writeDirect(strs ...string) error { _ = "STUB: not implemented"; return nil }

func (w *natsWriter) flush() error { _ = "STUB: not implemented"; return nil }

func (w *natsWriter) buffered() int { _ = "STUB: not implemented"; return 0 }

func (w *natsWriter) switchToPending() { _ = "STUB: not implemented"; return }

func (w *natsWriter) flushPendingBuffer() error { _ = "STUB: not implemented"; return nil }

func (w *natsWriter) atLimitIfUsingPending() bool { _ = "STUB: not implemented"; return false }

func (w *natsWriter) doneWithPending() { _ = "STUB: not implemented"; return }

func (r *natsReader) doneWithConnect() { _ = "STUB: not implemented"; return }

func (r *natsReader) Read() ([]byte, error) { _ = "STUB: not implemented"; return nil, nil }

func (r *natsReader) ReadString(delim byte) (string, error) {
	_ = "STUB: not implemented"
	return "", nil
}

func (nc *Conn) createConn() (err error) { _ = "STUB: not implemented"; return nil }

type skipTLSDialer interface {
	SkipTLSHandshake() bool
}

func (nc *Conn) makeTLSConn() error { _ = "STUB: not implemented"; return nil }

func (nc *Conn) TLSConnectionState() (tls.ConnectionState, error) {
	_ = "STUB: not implemented"
	return *new(tls.ConnectionState), nil
}

func (nc *Conn) waitForExits() { _ = "STUB: not implemented"; return }

func (nc *Conn) ForceReconnect() error { _ = "STUB: not implemented"; return nil }

func (nc *Conn) ConnectedUrl() string { _ = "STUB: not implemented"; return "" }

func (nc *Conn) ConnectedUrlRedacted() string { _ = "STUB: not implemented"; return "" }

func (nc *Conn) ConnectedAddr() string { _ = "STUB: not implemented"; return "" }

func (nc *Conn) LocalAddr() string { _ = "STUB: not implemented"; return "" }

func (nc *Conn) ConnectedServerId() string { _ = "STUB: not implemented"; return "" }

func (nc *Conn) ConnectedServerName() string { _ = "STUB: not implemented"; return "" }

var semVerRe = regexp.MustCompile(`\Av?([0-9]+)\.?([0-9]+)?\.?([0-9]+)?`)

func versionComponents(version string) (major, minor, patch int, err error) {
	_ = "STUB: not implemented"
	return 0, 0, 0, nil
}

func (nc *Conn) serverMinVersion(major, minor, patch int) bool {
	_ = "STUB: not implemented"
	return false
}

func (nc *Conn) ConnectedServerVersion() string { _ = "STUB: not implemented"; return "" }

func (nc *Conn) ConnectedClusterName() string { _ = "STUB: not implemented"; return "" }

func (nc *Conn) ConnectedServerJetStream() (bool, int) { _ = "STUB: not implemented"; return false, 0 }

func (nc *Conn) IsSystemAccount() bool { _ = "STUB: not implemented"; return false }

func (nc *Conn) setup() { _ = "STUB: not implemented"; return }

func (nc *Conn) tlsHandshakeEOF(err error) error { _ = "STUB: not implemented"; return nil }

func isConnClosedError(err error) bool { _ = "STUB: not implemented"; return false }

func (nc *Conn) processConnectInit() error { _ = "STUB: not implemented"; return nil }

func (nc *Conn) connect() (bool, error) { _ = "STUB: not implemented"; return false, nil }

func (nc *Conn) checkForSecure() error { _ = "STUB: not implemented"; return nil }

func (nc *Conn) processExpectedInfo() error { _ = "STUB: not implemented"; return nil }

func (nc *Conn) sendProto(proto string) { _ = "STUB: not implemented"; return }

func (nc *Conn) connectProto() (string, error) { _ = "STUB: not implemented"; return "", nil }

func normalizeErr(line string) string { _ = "STUB: not implemented"; return "" }

type natsProtoErr struct {
	description string
}

func (nerr *natsProtoErr) Error() string { _ = "STUB: not implemented"; return "" }

func (nerr *natsProtoErr) Is(err error) bool { _ = "STUB: not implemented"; return false }

func (nc *Conn) sendConnect() error { _ = "STUB: not implemented"; return nil }

func (nc *Conn) readProto() (string, error) { _ = "STUB: not implemented"; return "", nil }

type control struct {
	op, args string
}

func (nc *Conn) readOp(c *control) error { _ = "STUB: not implemented"; return nil }

func parseControl(line string, c *control) { _ = "STUB: not implemented"; return }

func (nc *Conn) flushReconnectPendingItems() error { _ = "STUB: not implemented"; return nil }

func (nc *Conn) stopPingTimer() { _ = "STUB: not implemented"; return }

func (nc *Conn) doReconnect(err error, forceReconnect bool) { _ = "STUB: not implemented"; return }

func (nc *Conn) processOpErr(err error, forceReconnect bool) bool {
	_ = "STUB: not implemented"
	return false
}

func (ac *asyncCallbacksHandler) asyncCBDispatcher() { _ = "STUB: not implemented"; return }

func (ac *asyncCallbacksHandler) push(f func()) { _ = "STUB: not implemented"; return }

func (ac *asyncCallbacksHandler) close() { _ = "STUB: not implemented"; return }

func (ac *asyncCallbacksHandler) pushOrClose(f func(), close bool) {
	_ = "STUB: not implemented"
	return
}

func (nc *Conn) readLoop() { _ = "STUB: not implemented"; return }

func (nc *Conn) waitForMsgs(s *Subscription) { _ = "STUB: not implemented"; return }

type msgFilter func(m *Msg) *Msg

func (nc *Conn) processMsg(data []byte) { _ = "STUB: not implemented"; return }

var (
	permissionsRe      = regexp.MustCompile(`Subscription to "(\S+)"`)
	permissionsQueueRe = regexp.MustCompile(`using queue "(\S+)"`)
)

func (nc *Conn) processTransientError(err error) { _ = "STUB: not implemented"; return }

func (nc *Conn) processAuthError(err error) bool { _ = "STUB: not implemented"; return false }

func (nc *Conn) flusher() { _ = "STUB: not implemented"; return }

func (nc *Conn) processPing() { _ = "STUB: not implemented"; return }

func (nc *Conn) processPong() { _ = "STUB: not implemented"; return }

func (nc *Conn) processOK() { _ = "STUB: not implemented"; return }

func (nc *Conn) processInfo(info string) error { _ = "STUB: not implemented"; return nil }

func (nc *Conn) processAsyncInfo(info []byte) { _ = "STUB: not implemented"; return }

func (nc *Conn) LastError() error { _ = "STUB: not implemented"; return nil }

func checkAuthError(e string) error { _ = "STUB: not implemented"; return nil }

func (nc *Conn) processErr(ie string) { _ = "STUB: not implemented"; return }

func (nc *Conn) kickFlusher() { _ = "STUB: not implemented"; return }

func (nc *Conn) Publish(subj string, data []byte) error { _ = "STUB: not implemented"; return nil }

type Header map[string][]string

func (h Header) Add(key, value string) { _ = "STUB: not implemented"; return }

func (h Header) Set(key, value string) { _ = "STUB: not implemented"; return }

func (h Header) Get(key string) string { _ = "STUB: not implemented"; return "" }

func (h Header) Values(key string) []string { _ = "STUB: not implemented"; return nil }

func (h Header) Del(key string) { _ = "STUB: not implemented"; return }

func NewMsg(subject string) *Msg { _ = "STUB: not implemented"; return nil }

const (
	hdrLine            = "NATS/1.0\r\n"
	crlf               = "\r\n"
	hdrPreEnd          = len(hdrLine) - len(crlf)
	statusHdr          = "Status"
	descrHdr           = "Description"
	lastConsumerSeqHdr = "Nats-Last-Consumer"
	lastStreamSeqHdr   = "Nats-Last-Stream"
	consumerStalledHdr = "Nats-Consumer-Stalled"
	noResponders       = "503"
	noMessagesSts      = "404"
	reqTimeoutSts      = "408"
	jetStream409Sts    = "409"
	controlMsg         = "100"
	statusLen          = 3
)

func DecodeHeadersMsg(data []byte) (Header, error) {
	_ = "STUB: not implemented"
	return *new(Header), nil
}

func readMIMEHeader(tp *textproto.Reader) (textproto.MIMEHeader, error) {
	_ = "STUB: not implemented"
	return *new(textproto.MIMEHeader), nil
}

func (nc *Conn) PublishMsg(m *Msg) error { _ = "STUB: not implemented"; return nil }

func (nc *Conn) PublishRequest(subj, reply string, data []byte) error {
	_ = "STUB: not implemented"
	return nil
}

const digits = "0123456789"

func validateSubject(subj string) error { _ = "STUB: not implemented"; return nil }

func (nc *Conn) publish(subj, reply string, validateReply bool, hdr, data []byte) error {
	_ = "STUB: not implemented"
	return nil
}

func (nc *Conn) respHandler(m *Msg) { _ = "STUB: not implemented"; return }

func (nc *Conn) createNewRequestAndSend(subj string, hdr, data []byte) (chan *Msg, string, error) {
	_ = "STUB: not implemented"
	return nil, "", nil
}

func (nc *Conn) RequestMsg(msg *Msg, timeout time.Duration) (*Msg, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func (nc *Conn) Request(subj string, data []byte, timeout time.Duration) (*Msg, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func (nc *Conn) useOldRequestStyle() bool { _ = "STUB: not implemented"; return false }

func (nc *Conn) request(subj string, hdr, data []byte, timeout time.Duration) (*Msg, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func (nc *Conn) newRequest(subj string, hdr, data []byte, timeout time.Duration) (*Msg, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func (nc *Conn) oldRequest(subj string, hdr, data []byte, timeout time.Duration) (*Msg, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

const (
	InboxPrefix    = "_INBOX."
	inboxPrefixLen = len(InboxPrefix)
	replySuffixLen = 8
	rdigits        = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
	base           = 62
)

func NewInbox() string { _ = "STUB: not implemented"; return "" }

func (nc *Conn) NewInbox() string { _ = "STUB: not implemented"; return "" }

func (nc *Conn) initNewResp() { _ = "STUB: not implemented"; return }

func (nc *Conn) newRespInbox() string { _ = "STUB: not implemented"; return "" }

func (nc *Conn) NewRespInbox() string { _ = "STUB: not implemented"; return "" }

func (nc *Conn) respToken(respInbox string) string { _ = "STUB: not implemented"; return "" }

func (nc *Conn) Subscribe(subj string, cb MsgHandler) (*Subscription, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func (nc *Conn) ChanSubscribe(subj string, ch chan *Msg) (*Subscription, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func (nc *Conn) ChanQueueSubscribe(subj, group string, ch chan *Msg) (*Subscription, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func (nc *Conn) SubscribeSync(subj string) (*Subscription, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func (nc *Conn) QueueSubscribe(subj, queue string, cb MsgHandler) (*Subscription, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func (nc *Conn) QueueSubscribeSync(subj, queue string) (*Subscription, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func (nc *Conn) QueueSubscribeSyncWithChan(subj, queue string, ch chan *Msg) (*Subscription, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func badSubject(subj string) bool { _ = "STUB: not implemented"; return false }

func badQueue(qname string) bool { _ = "STUB: not implemented"; return false }

func (nc *Conn) subscribe(subj, queue string, cb MsgHandler, ch chan *Msg, errCh chan (error), isSync bool, js *jsSub) (*Subscription, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func (nc *Conn) subscribeLocked(subj, queue string, cb MsgHandler, ch chan *Msg, errCh chan (error), isSync bool, js *jsSub) (*Subscription, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func (nc *Conn) NumSubscriptions() int { _ = "STUB: not implemented"; return 0 }

func (nc *Conn) removeSub(s *Subscription) { _ = "STUB: not implemented"; return }

type SubscriptionType int

const (
	AsyncSubscription = SubscriptionType(iota)
	SyncSubscription
	ChanSubscription
	NilSubscription
	PullSubscription
)

func (s *Subscription) Type() SubscriptionType {
	_ = "STUB: not implemented"
	return *new(SubscriptionType)
}

func (s *Subscription) IsValid() bool { _ = "STUB: not implemented"; return false }

func (s *Subscription) Drain() error { _ = "STUB: not implemented"; return nil }

func (s *Subscription) IsDraining() bool { _ = "STUB: not implemented"; return false }

func (s *Subscription) StatusChanged(statuses ...SubStatus) <-chan SubStatus {
	_ = "STUB: not implemented"
	return nil
}

func (s *Subscription) registerStatusChangeListener(status SubStatus, ch chan SubStatus) {
	_ = "STUB: not implemented"
	return
}

func (s *Subscription) sendStatusEvent(status SubStatus) { _ = "STUB: not implemented"; return }

func (s *Subscription) changeSubStatus(status SubStatus) { _ = "STUB: not implemented"; return }

func (s *Subscription) Unsubscribe() error { _ = "STUB: not implemented"; return nil }

func (nc *Conn) checkDrained(sub *Subscription) { _ = "STUB: not implemented"; return }

func (s *Subscription) AutoUnsubscribe(max int) error { _ = "STUB: not implemented"; return nil }

func (s *Subscription) SetClosedHandler(handler func(subject string)) {
	_ = "STUB: not implemented"
	return
}

func (nc *Conn) unsubscribe(sub *Subscription, max int, drainMode bool) error {
	_ = "STUB: not implemented"
	return nil
}

func (s *Subscription) NextMsg(timeout time.Duration) (*Msg, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func (s *Subscription) nextMsgNoTimeout() (*Msg, error) { _ = "STUB: not implemented"; return nil, nil }

func (s *Subscription) validateNextMsgState(pullSubInternal bool) error {
	_ = "STUB: not implemented"
	return nil
}

func (s *Subscription) getNextMsgErr() error { _ = "STUB: not implemented"; return nil }

func (s *Subscription) processNextMsgDelivered(msg *Msg) error {
	_ = "STUB: not implemented"
	return nil
}

func (s *Subscription) QueuedMsgs() (int, error) { _ = "STUB: not implemented"; return 0, nil }

func (s *Subscription) Pending() (int, int, error) { _ = "STUB: not implemented"; return 0, 0, nil }

func (s *Subscription) MaxPending() (int, int, error) { _ = "STUB: not implemented"; return 0, 0, nil }

func (s *Subscription) ClearMaxPending() error { _ = "STUB: not implemented"; return nil }

const (
	DefaultSubPendingMsgsLimit = 500_000

	DefaultSubPendingBytesLimit = 64 * 1024 * 1024
)

func (s *Subscription) PendingLimits() (int, int, error) {
	_ = "STUB: not implemented"
	return 0, 0, nil
}

func (s *Subscription) SetPendingLimits(msgLimit, bytesLimit int) error {
	_ = "STUB: not implemented"
	return nil
}

func (s *Subscription) Delivered() (int64, error) { _ = "STUB: not implemented"; return 0, nil }

func (s *Subscription) Dropped() (int, error) { _ = "STUB: not implemented"; return 0, nil }

func (m *Msg) Respond(data []byte) error { _ = "STUB: not implemented"; return nil }

func (m *Msg) RespondMsg(msg *Msg) error { _ = "STUB: not implemented"; return nil }

func (nc *Conn) removeFlushEntry(ch chan struct{}) bool { _ = "STUB: not implemented"; return false }

func (nc *Conn) sendPing(ch chan struct{}) { _ = "STUB: not implemented"; return }

func (nc *Conn) processPingTimer() { _ = "STUB: not implemented"; return }

func (nc *Conn) FlushTimeout(timeout time.Duration) (err error) {
	_ = "STUB: not implemented"
	return nil
}

func (nc *Conn) RTT() (time.Duration, error) {
	_ = "STUB: not implemented"
	return *new(time.Duration), nil
}

func (nc *Conn) Flush() error { _ = "STUB: not implemented"; return nil }

func (nc *Conn) Buffered() (int, error) { _ = "STUB: not implemented"; return 0, nil }

func (nc *Conn) resendSubscriptions() { _ = "STUB: not implemented"; return }

func (nc *Conn) clearPendingFlushCalls() { _ = "STUB: not implemented"; return }

func (nc *Conn) clearPendingRequestCalls() { _ = "STUB: not implemented"; return }

func (nc *Conn) close(status Status, doCBs bool, err error) { _ = "STUB: not implemented"; return }

func (nc *Conn) Close() { _ = "STUB: not implemented"; return }

func (nc *Conn) IsClosed() bool { _ = "STUB: not implemented"; return false }

func (nc *Conn) IsReconnecting() bool { _ = "STUB: not implemented"; return false }

func (nc *Conn) IsConnected() bool { _ = "STUB: not implemented"; return false }

func (nc *Conn) drainConnection() { _ = "STUB: not implemented"; return }

func (nc *Conn) Drain() error { _ = "STUB: not implemented"; return nil }

func (nc *Conn) IsDraining() bool { _ = "STUB: not implemented"; return false }

func (nc *Conn) getServers(implicitOnly bool) []string { _ = "STUB: not implemented"; return nil }

func (nc *Conn) Servers() []string { _ = "STUB: not implemented"; return nil }

func (nc *Conn) DiscoveredServers() []string { _ = "STUB: not implemented"; return nil }

func (nc *Conn) Status() Status { _ = "STUB: not implemented"; return *new(Status) }

func (nc *Conn) isClosed() bool { _ = "STUB: not implemented"; return false }

func (nc *Conn) isConnecting() bool { _ = "STUB: not implemented"; return false }

func (nc *Conn) isReconnecting() bool { _ = "STUB: not implemented"; return false }

func (nc *Conn) isConnected() bool { _ = "STUB: not implemented"; return false }

func (nc *Conn) isDraining() bool { _ = "STUB: not implemented"; return false }

func (nc *Conn) isDrainingPubs() bool { _ = "STUB: not implemented"; return false }

func (nc *Conn) Stats() Statistics { _ = "STUB: not implemented"; return *new(Statistics) }

func (nc *Conn) MaxPayload() int64 { _ = "STUB: not implemented"; return 0 }

func (nc *Conn) HeadersSupported() bool { _ = "STUB: not implemented"; return false }

func (nc *Conn) AuthRequired() bool { _ = "STUB: not implemented"; return false }

func (nc *Conn) TLSRequired() bool { _ = "STUB: not implemented"; return false }

func (nc *Conn) Barrier(f func()) error { _ = "STUB: not implemented"; return nil }

func (nc *Conn) ServerPool() []Server { _ = "STUB: not implemented"; return nil }

func (nc *Conn) SetServerPool(servers []string) error { _ = "STUB: not implemented"; return nil }

func (nc *Conn) GetClientIP() (net.IP, error) { _ = "STUB: not implemented"; return *new(net.IP), nil }

func (nc *Conn) GetClientID() (uint64, error) { _ = "STUB: not implemented"; return 0, nil }

func (nc *Conn) StatusChanged(statuses ...Status) chan Status {
	_ = "STUB: not implemented"
	return nil
}

func (nc *Conn) RemoveStatusListener(ch chan (Status)) { _ = "STUB: not implemented"; return }

func (nc *Conn) registerStatusChangeListener(status Status, ch chan Status) {
	_ = "STUB: not implemented"
	return
}

func (nc *Conn) sendStatusEvent(s Status) { _ = "STUB: not implemented"; return }

func (nc *Conn) changeConnStatus(status Status) { _ = "STUB: not implemented"; return }

func NkeyOptionFromSeed(seedFile string) (Option, error) {
	_ = "STUB: not implemented"
	return *new(Option), nil
}

func wipeSlice(buf []byte) { _ = "STUB: not implemented"; return }

func userFromFile(userFile string) (string, error) { _ = "STUB: not implemented"; return "", nil }

func homeDir() (string, error) { _ = "STUB: not implemented"; return "", nil }

func expandPath(p string) (string, error) { _ = "STUB: not implemented"; return "", nil }

func nkeyPairFromSeedFile(seedFile string) (nkeys.KeyPair, error) {
	_ = "STUB: not implemented"
	return *new(nkeys.KeyPair), nil
}

func sigHandler(nonce []byte, seedFile string) ([]byte, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

type timeoutWriter struct {
	timeout time.Duration
	conn    net.Conn
}

func (tw *timeoutWriter) Write(p []byte) (int, error) { _ = "STUB: not implemented"; return 0, nil }
