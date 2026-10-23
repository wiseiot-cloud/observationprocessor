package processor

import (
	"cmp"
	"crypto/tls"
	"errors"
	"log/slog"
	"net"
	"os"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/google/uuid"
)

// Config configures a Processor. New applies the defaults of zero fields.
type Config struct {
	// Endpoint is the host:port of the Processor Runtime API, reached over TLS.
	Endpoint string
	// TLSConfig configures TLS; nil verifies the server with the system's roots.
	TLSConfig *tls.Config
	// AllowInsecure reaches Endpoint without TLS, which only a loopback Endpoint may.
	AllowInsecure bool
	// ProcessorID is the processor to run, a lowercase hyphenated UUID.
	ProcessorID string
	// Credential provides the Access API token of every call.
	Credential CredentialProvider
	// InstanceID names this instance in the status of the processor: 1 to 128 characters without
	// control characters. The default is the host name.
	InstanceID string

	// MaxConcurrentPartitions is how many partitions call the Handler at the same time, each for a
	// page of its own: 1 to 16, default 1. Every partition reads on its own, so this bounds only the
	// Handler calls.
	MaxConcurrentPartitions int
	// PageSize is the most observations that a page holds: 1 to 1000, default 100.
	PageSize int
	// LongPollTimeout is how long a Read of an idle partition waits for new observations: 1 s to
	// 30 s, default 20 s.
	LongPollTimeout time.Duration
	// HandlerTimeout ends the context of a Handler call: default 30 s.
	HandlerTimeout time.Duration
	// ShutdownTimeout bounds how long Shutdown waits: default 30 s.
	ShutdownTimeout time.Duration
	// RetryPolicy spaces the retries after failures: default BoundedRetryPolicy{}.
	RetryPolicy RetryPolicy

	// FailureSink records the observations whose Handler returned a Permanent error.
	FailureSink FailureSink
	// RequireFailureSink makes New refuse a Config without FailureSink.
	RequireFailureSink bool
	// IncludePayloadInFailures keeps the payload of the observations in FailureRecords.
	IncludePayloadInFailures bool

	// Observer receives the events of the Processor.
	Observer Observer
	// Logger logs what the Processor does; nil logs nothing.
	Logger *slog.Logger
}

// withDefaults returns c with the defaults of its zero fields, or why it is invalid.
func (c Config) withDefaults() (Config, error) {
	host, port, err := net.SplitHostPort(c.Endpoint)
	switch {
	case err != nil || host == "" || port == "":
		return c, errors.New("Endpoint must be host:port")
	case c.AllowInsecure && host != "localhost" && !net.ParseIP(host).IsLoopback():
		return c, errors.New("AllowInsecure needs a loopback Endpoint")
	}
	if id, err := uuid.Parse(c.ProcessorID); err != nil || id.String() != c.ProcessorID {
		return c, errors.New("ProcessorID must be a lowercase hyphenated UUID")
	}
	if c.Credential == nil {
		return c, errors.New("Credential is required")
	}
	if c.InstanceID == "" {
		if c.InstanceID, err = os.Hostname(); err != nil {
			return c, errors.New("InstanceID is required: the host name is unknown")
		}
	}
	if n := utf8.RuneCountInString(c.InstanceID); n < 1 || n > 128 || !utf8.ValidString(c.InstanceID) ||
		strings.ContainsFunc(c.InstanceID, unicode.IsControl) {
		return c, errors.New("InstanceID must have 1 to 128 characters of UTF-8 text without control characters")
	}
	c.MaxConcurrentPartitions = cmp.Or(c.MaxConcurrentPartitions, 1)
	c.PageSize = cmp.Or(c.PageSize, 100)
	c.LongPollTimeout = cmp.Or(c.LongPollTimeout, 20*time.Second)
	c.HandlerTimeout = cmp.Or(c.HandlerTimeout, 30*time.Second)
	c.ShutdownTimeout = cmp.Or(c.ShutdownTimeout, 30*time.Second)
	switch {
	case c.MaxConcurrentPartitions < 1 || c.MaxConcurrentPartitions > 16:
		return c, errors.New("MaxConcurrentPartitions must be 1 to 16")
	case c.PageSize < 1 || c.PageSize > 1000:
		return c, errors.New("PageSize must be 1 to 1000")
	case c.LongPollTimeout < time.Second || c.LongPollTimeout > 30*time.Second:
		return c, errors.New("LongPollTimeout must be 1s to 30s")
	case c.HandlerTimeout < 0:
		return c, errors.New("HandlerTimeout must be positive")
	case c.ShutdownTimeout < 0:
		return c, errors.New("ShutdownTimeout must be positive")
	case c.RequireFailureSink && c.FailureSink == nil:
		return c, errors.New("RequireFailureSink needs a FailureSink")
	}
	if c.RetryPolicy == nil {
		c.RetryPolicy = BoundedRetryPolicy{}
	}
	if c.Logger == nil {
		c.Logger = slog.New(slog.DiscardHandler)
	}
	return c, nil
}
