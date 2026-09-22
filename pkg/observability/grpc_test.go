package observability

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net"
	"strings"
	"sync"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

// probe is a gRPC service whose one method the test can script. The standard health
// service is used only because it is already generated; nothing here is about health.
type probe struct {
	grpc_health_v1.UnimplementedHealthServer
	mu       sync.Mutex
	handler  func(ctx context.Context) error
	seenByID []string
}

func (p *probe) Check(ctx context.Context, _ *grpc_health_v1.HealthCheckRequest) (*grpc_health_v1.HealthCheckResponse, error) {
	p.mu.Lock()
	p.seenByID = append(p.seenByID, RequestID(ctx))
	h := p.handler
	p.mu.Unlock()
	if h != nil {
		if err := h(ctx); err != nil {
			return nil, err
		}
	}
	return &grpc_health_v1.HealthCheckResponse{Status: grpc_health_v1.HealthCheckResponse_SERVING}, nil
}

func (p *probe) ids() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.seenByID...)
}

type rig struct {
	client grpc_health_v1.HealthClient
	probe  *probe
	logs   *bytes.Buffer
}

func newRig(t *testing.T, clientOpts ...grpc.DialOption) *rig {
	t.Helper()
	logs := &bytes.Buffer{}
	log := slog.New(slog.NewJSONHandler(&syncWriter{w: logs}, &slog.HandlerOptions{Level: slog.LevelDebug}))

	p := &probe{}
	lis := bufconn.Listen(1 << 20)
	srv := NewGRPCServer(log)
	grpc_health_v1.RegisterHealthServer(srv, p)
	InitGRPCMetrics(srv)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	conn, err := grpc.NewClient("passthrough:///bufnet", append([]grpc.DialOption{
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}, clientOpts...)...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return &rig{client: grpc_health_v1.NewHealthClient(conn), probe: p, logs: logs}
}

// syncWriter lets the server goroutine log while the test goroutine reads.
type syncWriter struct {
	mu sync.Mutex
	w  *bytes.Buffer
}

func (s *syncWriter) Write(b []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.w.Write(b)
}

func (r *rig) logLines(t *testing.T) []map[string]any {
	t.Helper()
	var out []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(r.logs.String()), "\n") {
		if line == "" {
			continue
		}
		var m map[string]any
		if err := json.Unmarshal([]byte(line), &m); err != nil {
			t.Fatalf("log line is not JSON: %q", line)
		}
		out = append(out, m)
	}
	return out
}

func (r *rig) call(ctx context.Context) error {
	_, err := r.client.Check(ctx, &grpc_health_v1.HealthCheckRequest{})
	return err
}

func TestRecovery_APanicBecomesInternalAndTheServerKeepsServing(t *testing.T) {
	r := newRig(t)
	r.probe.handler = func(context.Context) error { panic("secret internal state") }

	err := r.call(t.Context())

	if status.Code(err) != codes.Internal {
		t.Fatalf("code = %v, want Internal", status.Code(err))
	}
	if strings.Contains(err.Error(), "secret internal state") {
		t.Errorf("the panic value reached the caller: %v", err)
	}
	r.probe.handler = nil
	if err := r.call(t.Context()); err != nil {
		t.Errorf("the server did not survive the panic: %v", err)
	}

	var logged bool
	for _, l := range r.logLines(t) {
		if l["msg"] == "panic in rpc handler" && strings.Contains(l["stack"].(string), "goroutine") && l["panic"] == "secret internal state" {
			logged = true
		}
	}
	if !logged {
		t.Error("the panic and its stack were not logged")
	}
}

func TestRequestID_IsAdoptedFromTheCaller(t *testing.T) {
	r := newRig(t)
	ctx := metadata.AppendToOutgoingContext(t.Context(), RequestIDMetadata, "req-abc")

	if err := r.call(ctx); err != nil {
		t.Fatal(err)
	}

	if got := r.probe.ids(); len(got) != 1 || got[0] != "req-abc" {
		t.Errorf("handler saw request IDs %v, want [req-abc]", got)
	}
	for _, l := range r.logLines(t) {
		if l["msg"] == "rpc" && l["request_id"] != "req-abc" {
			t.Errorf("the rpc log line has request_id %v, want req-abc", l["request_id"])
		}
	}
}

func TestRequestID_AbsentMeansEmptyNotInvented(t *testing.T) {
	r := newRig(t)

	if err := r.call(t.Context()); err != nil {
		t.Fatal(err)
	}

	if got := r.probe.ids(); len(got) != 1 || got[0] != "" {
		t.Errorf("handler saw %v, want one empty ID", got)
	}
}

// The client half: an ID in the caller's context must reach the callee.
func TestPropagateRequestID_CarriesTheIDToTheNextService(t *testing.T) {
	r := newRig(t, grpc.WithChainUnaryInterceptor(PropagateRequestID()))

	if err := r.call(WithRequestID(t.Context(), "from-the-edge")); err != nil {
		t.Fatal(err)
	}
	if err := r.call(t.Context()); err != nil { // no ID in ctx: nothing to forward
		t.Fatal(err)
	}

	if got := r.probe.ids(); len(got) != 2 || got[0] != "from-the-edge" || got[1] != "" {
		t.Errorf("handler saw %v, want [from-the-edge, ]", got)
	}
}

func TestLogging_OneLineAtALevelThatMatchesTheOutcome(t *testing.T) {
	r := newRig(t)

	r.probe.handler = nil
	_ = r.call(t.Context())
	r.probe.handler = func(context.Context) error { return status.Error(codes.NotFound, "no such thing") }
	_ = r.call(t.Context())
	r.probe.handler = func(context.Context) error { return status.Error(codes.Internal, "boom") }
	_ = r.call(t.Context())

	want := []struct{ code, level string }{{"OK", "DEBUG"}, {"NotFound", "INFO"}, {"Internal", "ERROR"}}
	var rpcLines []map[string]any
	for _, l := range r.logLines(t) {
		if l["msg"] == "rpc" {
			rpcLines = append(rpcLines, l)
		}
	}
	if len(rpcLines) != len(want) {
		t.Fatalf("got %d rpc log lines, want %d", len(rpcLines), len(want))
	}
	for i, w := range want {
		if rpcLines[i]["code"] != w.code || rpcLines[i]["level"] != w.level {
			t.Errorf("line %d = code %v level %v, want %s %s", i, rpcLines[i]["code"], rpcLines[i]["level"], w.code, w.level)
		}
		if rpcLines[i]["method"] != "/grpc.health.v1.Health/Check" {
			t.Errorf("line %d method = %v", i, rpcLines[i]["method"])
		}
	}
}

func TestLevelFor(t *testing.T) {
	cases := map[codes.Code]slog.Level{
		codes.OK:                 slog.LevelDebug,
		codes.InvalidArgument:    slog.LevelInfo,
		codes.NotFound:           slog.LevelInfo,
		codes.FailedPrecondition: slog.LevelInfo,
		codes.PermissionDenied:   slog.LevelInfo,
		codes.Unavailable:        slog.LevelWarn,
		codes.DeadlineExceeded:   slog.LevelWarn,
		codes.Internal:           slog.LevelError,
		codes.Unknown:            slog.LevelError,
		codes.DataLoss:           slog.LevelError,
	}
	for code, want := range cases {
		if got := levelFor(code); got != want {
			t.Errorf("levelFor(%v) = %v, want %v", code, got, want)
		}
	}
}

func handled(t *testing.T, code string) float64 {
	t.Helper()
	families, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatal(err)
	}
	var total float64
	for _, f := range families {
		if f.GetName() != "grpc_server_handled_total" {
			continue
		}
		for _, m := range f.GetMetric() {
			labels := map[string]string{}
			for _, l := range m.GetLabel() {
				labels[l.GetName()] = l.GetValue()
			}
			if labels["grpc_method"] == "Check" && labels["grpc_code"] == code {
				total += m.GetCounter().GetValue()
			}
		}
	}
	return total
}

func TestMetrics_CountCallsByStatusCode(t *testing.T) {
	r := newRig(t)
	okBefore, internalBefore := handled(t, "OK"), handled(t, "Internal")

	r.probe.handler = nil
	_ = r.call(t.Context())
	_ = r.call(t.Context())
	r.probe.handler = func(context.Context) error { return status.Error(codes.Internal, "x") }
	_ = r.call(t.Context())

	if got := handled(t, "OK") - okBefore; got != 2 {
		t.Errorf("OK calls counted = %v, want 2", got)
	}
	if got := handled(t, "Internal") - internalBefore; got != 1 {
		t.Errorf("Internal calls counted = %v, want 1", got)
	}
}

func TestMetrics_APanickedCallIsStillCounted(t *testing.T) {
	r := newRig(t)
	before := handled(t, "Internal")
	r.probe.handler = func(context.Context) error { panic("x") }

	_ = r.call(t.Context())

	if got := handled(t, "Internal") - before; got != 1 {
		t.Errorf("a panicked call was counted %v times, want 1: crashes must show up in the error rate", got)
	}
}
