package observability

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"
)

func TestLevelFromEnv(t *testing.T) {
	cases := map[string]slog.Level{
		"":         slog.LevelInfo,
		"debug":    slog.LevelDebug,
		"DEBUG":    slog.LevelDebug,
		" warn ":   slog.LevelWarn,
		"warning":  slog.LevelWarn,
		"error":    slog.LevelError,
		"nonsense": slog.LevelInfo, // a typo must not silence a service
	}
	for env, want := range cases {
		t.Run(env, func(t *testing.T) {
			t.Setenv("LOG_LEVEL", env)
			if got := levelFromEnv(); got != want {
				t.Errorf("LOG_LEVEL=%q gave %v, want %v", env, got, want)
			}
		})
	}
}

// Existing log.Printf calls must come out as JSON with the service name once
// SetupLogging has run, or the migration to structured logs would have to be big-bang.
func TestSetupLogging_MakesStandardLogPrintfStructuredToo(t *testing.T) {
	prevOut, prevDefault := os.Stdout, slog.Default()
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	os.Stdout = w
	t.Cleanup(func() {
		os.Stdout = prevOut
		slog.SetDefault(prevDefault)
		log.SetFlags(log.LstdFlags)
	})
	t.Setenv("LOG_LEVEL", "info")

	logger := SetupLogging("auth")
	logger.Info("direct", slog.String("k", "v"))
	log.Printf("legacy line %d", 42)
	_ = w.Close()
	raw, _ := io.ReadAll(r)

	lines := strings.Split(strings.TrimSpace(string(raw)), "\n")
	if len(lines) != 2 {
		t.Fatalf("got %d log lines, want 2: %q", len(lines), raw)
	}
	for i, line := range lines {
		var m map[string]any
		if err := json.Unmarshal([]byte(line), &m); err != nil {
			t.Fatalf("line %d is not JSON: %q", i, line)
		}
		if m["service"] != "auth" {
			t.Errorf("line %d has no service attribute: %v", i, m)
		}
	}
	var legacy map[string]any
	_ = json.Unmarshal([]byte(lines[1]), &legacy)
	if legacy["msg"] != "legacy line 42" {
		t.Errorf("legacy line msg = %v, want the formatted text", legacy["msg"])
	}
}

func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = l.Close() }()
	return l.Addr().String()
}

func TestServeMetrics_ServesPrometheusTextAndStopsCleanly(t *testing.T) {
	addr := freeAddr(t)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- ServeMetrics(ctx, addr, slog.New(slog.DiscardHandler)) }()

	var body string
	deadline := time.Now().Add(3 * time.Second)
	for {
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+addr+"/metrics", nil)
		resp, err := http.DefaultClient.Do(req)
		if err == nil {
			b, _ := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			body = string(b)
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("status = %d, want 200", resp.StatusCode)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("metrics endpoint never came up: %v", err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	// Free with the Go client: proves the runtime collectors are on the default registry.
	for _, want := range []string{"go_goroutines", "go_memstats_alloc_bytes"} {
		if !strings.Contains(body, want) {
			t.Errorf("metrics output lacks %s", want)
		}
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("ServeMetrics returned %v on a normal shutdown, want nil", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("ServeMetrics did not stop after its context was cancelled")
	}
}

func TestServeMetrics_ReportsAPortThatIsAlreadyTaken(t *testing.T) {
	taken, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = taken.Close() }()

	err = ServeMetrics(t.Context(), taken.Addr().String(), slog.New(slog.DiscardHandler))

	if err == nil {
		t.Error("ServeMetrics succeeded on an address that is already in use")
	}
}

func TestStartMetrics_OffMeansDisabled(t *testing.T) {
	// "off" is not a valid address, so if it were not recognised the listener would
	// try to bind it and log an error. Watching the log proves nothing was started.
	var logs strings.Builder
	log := slog.New(slog.NewJSONHandler(&logs, nil))

	StartMetrics(t.Context(), Disabled, log)
	StartMetrics(t.Context(), "", log)
	time.Sleep(100 * time.Millisecond) // long enough for a wrongly started goroutine to fail and log

	if logs.Len() != 0 {
		t.Errorf("a disabled metrics listener still logged: %s", logs.String())
	}
}
