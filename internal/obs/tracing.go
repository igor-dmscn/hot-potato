// Package obs is tracing: one tracer, W3C context propagation, and a no-op when
// no collector is configured.
package obs

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
)

// tracerName identifies the spans this application produces.
const tracerName = "hotpotato"

type Options struct {
	// Endpoint is an OTLP/HTTP collector. Empty means tracing stays a no-op:
	// spans are created and immediately dropped, and no exporter is started.
	Endpoint string
	Instance string
	Sample   float64
}

// Setup installs a tracer provider and the W3C propagator. The returned function
// flushes whatever is pending.
func Setup(ctx context.Context, o Options) (func(context.Context) error, error) {
	// The propagator goes in either way, so Inject and Extract behave the same
	// whether or not anything is collecting.
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{}, propagation.Baggage{}))

	if o.Endpoint == "" {
		return func(context.Context) error { return nil }, nil
	}
	exporter, err := otlptracehttp.New(ctx, otlptracehttp.WithEndpointURL(o.Endpoint))
	if err != nil {
		return nil, fmt.Errorf("otlp exporter: %w", err)
	}
	res, err := resource.Merge(resource.Default(), resource.NewWithAttributes(
		semconv.SchemaURL,
		semconv.ServiceName("hotpotato"),
		semconv.ServiceInstanceID(o.Instance),
	))
	if err != nil {
		return nil, fmt.Errorf("otel resource: %w", err)
	}
	if o.Sample <= 0 {
		o.Sample = 1
	}
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(o.Sample))),
	)
	otel.SetTracerProvider(tp)

	return func(c context.Context) error {
		c, cancel := context.WithTimeout(c, 5*time.Second)
		defer cancel()
		return tp.Shutdown(c)
	}, nil
}

// Start opens a span. With no provider configured this is a few nanoseconds and
// a no-op span.
func Start(ctx context.Context, name string, attrs ...attribute.KeyValue) (context.Context, trace.Span) {
	return otel.Tracer(tracerName).Start(ctx, name, trace.WithAttributes(attrs...))
}

// Traceparent serialises the active span context into a W3C traceparent.
//
// This is what travels in bus.Event.Trace and on a Transfer: a Transfer's life
// crosses several requests and, once there is more than one instance, several
// processes. Carrying the header rather than the context is what lets a trace
// span offer → accept → ready → relay → complete.
func Traceparent(ctx context.Context) string {
	carrier := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	return carrier.Get("traceparent")
}

// Continue rebuilds a context from a traceparent, so a span started with it is a
// child of the original rather than the root of a new trace.
func Continue(ctx context.Context, traceparent string) context.Context {
	if traceparent == "" {
		return ctx
	}
	return otel.GetTextMapPropagator().Extract(ctx,
		propagation.MapCarrier{"traceparent": traceparent})
}

// Middleware puts a span around every request, named by method and route.
func Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// An inbound traceparent is honoured: spud, a proxy or another instance
		// may already have started the trace.
		ctx := otel.GetTextMapPropagator().Extract(r.Context(),
			propagation.HeaderCarrier(r.Header))

		ctx, span := Start(ctx, r.Method+" "+r.URL.Path,
			semconv.HTTPRequestMethodKey.String(r.Method),
			semconv.URLPath(r.URL.Path))
		defer span.End()

		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// Attr is a shorthand so callers do not import the attribute package.
func Attr(key, value string) attribute.KeyValue { return attribute.String(key, value) }

// Int64Attr is the same for a number.
func Int64Attr(key string, value int64) attribute.KeyValue { return attribute.Int64(key, value) }
