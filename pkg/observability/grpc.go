package observability

import (
	"context"
	"log/slog"
	"runtime/debug"
	"sync"
	"time"

	grpcprom "github.com/grpc-ecosystem/go-grpc-middleware/providers/prometheus"
	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

var (
	serverMetricsOnce sync.Once
	serverMetrics     *grpcprom.ServerMetrics
)

// grpcServerMetrics returns the process-wide gRPC metrics, registering them on first
// use. It is a singleton because registering the same metric twice panics, and tests
// build several servers in one process.
func grpcServerMetrics() *grpcprom.ServerMetrics {
	serverMetricsOnce.Do(func() {
		serverMetrics = grpcprom.NewServerMetrics(
			// A histogram of handling time lets Prometheus compute p50/p95/p99 per
			// method. Without it only a total and a count exist, which hides slow tails.
			grpcprom.WithServerHandlingTimeHistogram(
				grpcprom.WithHistogramBuckets([]float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10}),
			),
		)
		prometheus.MustRegister(serverMetrics)
	})
	return serverMetrics
}

// NewGRPCServer returns a gRPC server with the interceptors every service needs.
//
// The stats handler (otelgrpc) runs first, before any interceptor: it is how a
// server-side span gets into the context, continuing the trace the caller's
// ClientTracing dial option attached to the call. With no tracer configured
// (SetupTracing was never called, or was given TracingDisabled) this still runs,
// but produces spans nobody records, so nothing here needs an "is tracing on" branch.
//
// Then the interceptor chain, outermost first:
//
//  1. request ID: adopts the ID sent by the caller so logs line up across services.
//  2. metrics: counts calls and times them by method and status code.
//  3. logging: one line per call, at a level that matches how serious the outcome is.
//  4. recovery: a panic in one handler becomes an Internal error for that one call,
//     instead of crashing the process and every other request in flight.
//
// Recovery is INNERMOST, next to the handler, deliberately. A panic unwinds through
// every interceptor outside the one that catches it, and those never see a result.
// Were recovery outermost, a crashing handler would be invisible to the metrics and
// the log, the one failure the error rate most needs to include. Innermost, the
// layers above see an ordinary Internal error and record it like any other.
func NewGRPCServer(log *slog.Logger, opts ...grpc.ServerOption) *grpc.Server {
	m := grpcServerMetrics()
	all := append([]grpc.ServerOption{
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
		grpc.ChainUnaryInterceptor(
			requestIDServer(),
			m.UnaryServerInterceptor(),
			loggingUnary(log),
			recoveryUnary(log),
		),
	}, opts...)
	return grpc.NewServer(all...)
}

// InitGRPCMetrics pre-creates a zero-valued series for every method of every service
// registered on srv. Without it a method that has not been called yet has no series
// at all, so rate() and error-ratio queries show gaps (or nothing) until the first
// call. Call it after registering services and before serving.
func InitGRPCMetrics(srv *grpc.Server) {
	grpcServerMetrics().InitializeMetrics(srv)
}

func recoveryUnary(log *slog.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp any, err error) {
		defer func() {
			if r := recover(); r != nil {
				log.ErrorContext(ctx, "panic in rpc handler",
					slog.String("method", info.FullMethod),
					slog.String("request_id", RequestID(ctx)),
					slog.String("trace_id", TraceID(ctx)),
					slog.String("span_id", SpanID(ctx)),
					slog.Any("panic", r),
					slog.String("stack", string(debug.Stack())))
				// The caller learns nothing about the panic, only that it was ours.
				err = status.Error(codes.Internal, "internal error")
			}
		}()
		return handler(ctx, req)
	}
}

func requestIDServer() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if md, ok := metadata.FromIncomingContext(ctx); ok {
			if ids := md.Get(RequestIDMetadata); len(ids) > 0 && ids[0] != "" {
				ctx = WithRequestID(ctx, ids[0])
			}
		}
		return handler(ctx, req)
	}
}

func loggingUnary(log *slog.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		start := time.Now()
		resp, err := handler(ctx, req)
		code := status.Code(err)
		log.Log(ctx, levelFor(code), "rpc",
			slog.String("method", info.FullMethod),
			slog.String("code", code.String()),
			slog.Duration("duration", time.Since(start)),
			slog.String("request_id", RequestID(ctx)),
			slog.String("trace_id", TraceID(ctx)),
			slog.String("span_id", SpanID(ctx)))
		return resp, err
	}
}

// levelFor maps a gRPC status to how loud its log line should be. A successful call
// is routine and goes to debug, or the log would be mostly noise. A caller's mistake
// (bad input, not found, a lost seat race) is worth seeing but is not our fault. Only
// codes that mean OUR code or infrastructure failed are errors, because those are
// what an alert should fire on.
func levelFor(c codes.Code) slog.Level {
	switch c {
	case codes.OK:
		return slog.LevelDebug
	case codes.Internal, codes.Unknown, codes.DataLoss, codes.Unimplemented:
		return slog.LevelError
	case codes.Unavailable, codes.DeadlineExceeded, codes.ResourceExhausted:
		return slog.LevelWarn
	default:
		return slog.LevelInfo
	}
}

// PropagateRequestID is a client interceptor that forwards the request ID in the
// call's context to the service being called. Dial every downstream service with it,
// or the trail of one request goes cold at the first hop.
func PropagateRequestID() grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		if id := RequestID(ctx); id != "" {
			ctx = metadata.AppendToOutgoingContext(ctx, RequestIDMetadata, id)
		}
		return invoker(ctx, method, req, reply, cc, opts...)
	}
}
