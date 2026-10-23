package processor_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	processor "github.com/wiseiot-cloud/observationprocessor/runtime"
)

func TestNewValidatesTheConfig(t *testing.T) {
	t.Parallel()
	valid := func() processor.Config {
		return processor.Config{Endpoint: "access.example:443", ProcessorID: processorID, Credential: processor.StaticToken(token)}
	}
	for name, change := range map[string]func(*processor.Config){
		"no endpoint":                  func(c *processor.Config) { c.Endpoint = "" },
		"no port":                      func(c *processor.Config) { c.Endpoint = "access.example" },
		"no host":                      func(c *processor.Config) { c.Endpoint = ":443" },
		"URL":                          func(c *processor.Config) { c.Endpoint = "https://access.example:443" },
		"insecure remote":              func(c *processor.Config) { c.AllowInsecure = true },
		"no processor":                 func(c *processor.Config) { c.ProcessorID = "" },
		"uppercase processor":          func(c *processor.Config) { c.ProcessorID = strings.ToUpper(processorID) },
		"processor not a UUID":         func(c *processor.Config) { c.ProcessorID = "processor" },
		"no credential":                func(c *processor.Config) { c.Credential = nil },
		"long instance":                func(c *processor.Config) { c.InstanceID = strings.Repeat("i", 129) },
		"instance with a line break":   func(c *processor.Config) { c.InstanceID = "a\nb" },
		"instance not UTF-8":           func(c *processor.Config) { c.InstanceID = "\xff" },
		"no partitions":                func(c *processor.Config) { c.MaxConcurrentPartitions = -1 },
		"17 partitions":                func(c *processor.Config) { c.MaxConcurrentPartitions = 17 },
		"negative page":                func(c *processor.Config) { c.PageSize = -1 },
		"large page":                   func(c *processor.Config) { c.PageSize = 1001 },
		"short long poll":              func(c *processor.Config) { c.LongPollTimeout = time.Second - 1 },
		"long long poll":               func(c *processor.Config) { c.LongPollTimeout = 30*time.Second + 1 },
		"negative handler timeout":     func(c *processor.Config) { c.HandlerTimeout = -1 },
		"negative shutdown timeout":    func(c *processor.Config) { c.ShutdownTimeout = -1 },
		"required FailureSink missing": func(c *processor.Config) { c.RequireFailureSink = true },
	} {
		config := valid()
		change(&config)
		p, err := processor.New(config)
		var e *processor.Error
		if p != nil || !errors.Is(err, processor.ErrorInvalidConfig) || !errors.As(err, &e) || e.Op != "new" {
			t.Errorf("%s: New = %v, %v; want ErrorInvalidConfig", name, p, err)
		}
	}
	for name, change := range map[string]func(*processor.Config){
		"defaults": func(*processor.Config) {},
		"bounds": func(c *processor.Config) {
			c.MaxConcurrentPartitions, c.PageSize, c.LongPollTimeout = 16, 1000, 30*time.Second
		},
		"insecure IPv4":  func(c *processor.Config) { c.Endpoint, c.AllowInsecure = "127.0.0.2:8084", true },
		"insecure IPv6":  func(c *processor.Config) { c.Endpoint, c.AllowInsecure = "[::1]:8084", true },
		"insecure local": func(c *processor.Config) { c.Endpoint, c.AllowInsecure = "localhost:8084", true },
		"instance":       func(c *processor.Config) { c.InstanceID = strings.Repeat("é", 128) },
	} {
		config := valid()
		change(&config)
		if _, err := processor.New(config); err != nil {
			t.Errorf("%s: New = %v", name, err)
		}
	}
}

func TestDefaults(t *testing.T) {
	t.Parallel()
	f := newFake(t, 16)
	p, err := processor.New(processor.Config{Endpoint: f.address, AllowInsecure: true, ProcessorID: processorID, Credential: processor.StaticToken(token)})
	if err != nil {
		t.Fatal(err)
	}
	start(t, p, recording(&offsets{}))
	eventually(t, "a Read", func() bool { return len(f.callsOf("Read")) > 0 })
	host, _ := os.Hostname()
	if opened := f.callsOf("OpenSession"); opened[0].instance != host || p.Snapshot().InstanceID != host {
		t.Errorf("instance %q, want the host name %q", opened[0].instance, host)
	}
	// Pages of 100 observations; a Read of an idle partition waits 20 s.
	if read := f.callsOf("Read")[0]; read.limit != 100 || read.wait != 20*time.Second {
		t.Errorf("Read of %d observations, waiting %v", read.limit, read.wait)
	}
}

func TestCredentialsStayHidden(t *testing.T) {
	t.Parallel()
	for _, value := range []any{
		processor.StaticToken("acs_hidden"),
		processor.Config{Credential: processor.StaticToken("acs_hidden")},
		&processor.Config{Credential: processor.StaticToken("acs_hidden")},
	} {
		for _, format := range []string{"%v", "%+v", "%#v", "%s"} {
			if formatted := fmt.Sprintf(format, value); strings.Contains(formatted, "acs_hidden") {
				t.Errorf("%s shows the token: %s", format, formatted)
			}
		}
	}
	path := filepath.Join(t.TempDir(), "token")
	provider := processor.FileToken(path)
	if _, err := provider.Credential(context.Background()); err == nil {
		t.Error("FileToken of a missing file succeeded")
	}
	for content, want := range map[string]string{"acs_one\n": "acs_one", "  acs_two \r\n": "acs_two"} {
		if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
		if got, err := provider.Credential(context.Background()); err != nil || got != want {
			t.Errorf("FileToken = %q, %v; want %q", got, err, want)
		}
	}
	if formatted := fmt.Sprint(provider); formatted != "FileToken("+path+")" {
		t.Errorf("FileToken formats as %q", formatted)
	}
}

func TestBoundedRetryPolicyDoublesUpToItsMaximum(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		policy  processor.BoundedRetryPolicy
		attempt int
		delay   time.Duration
	}{
		{processor.BoundedRetryPolicy{Initial: 100 * time.Millisecond, Maximum: time.Second}, 1, 100 * time.Millisecond},
		{processor.BoundedRetryPolicy{Initial: 100 * time.Millisecond, Maximum: time.Second}, 2, 200 * time.Millisecond},
		{processor.BoundedRetryPolicy{Initial: 100 * time.Millisecond, Maximum: time.Second}, 4, 800 * time.Millisecond},
		{processor.BoundedRetryPolicy{Initial: 100 * time.Millisecond, Maximum: time.Second}, 5, time.Second},
		{processor.BoundedRetryPolicy{Initial: 100 * time.Millisecond, Maximum: time.Second}, 1000, time.Second},
		{processor.BoundedRetryPolicy{}, 1, 100 * time.Millisecond},
		{processor.BoundedRetryPolicy{}, 100, 10 * time.Second},
	} {
		for range 20 {
			if got := test.policy.RetryDelay(test.attempt); got < test.delay/2 || got > test.delay {
				t.Errorf("%+v.RetryDelay(%d) = %v, want %v to %v", test.policy, test.attempt, got, test.delay/2, test.delay)
			}
		}
	}
}
