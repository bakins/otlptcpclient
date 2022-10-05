package otlptcpclient

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"sync"

	"github.com/akutz/memconn"
	retry "github.com/avast/retry-go/v4"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	logpb "go.opentelemetry.io/proto/otlp/logs/v1"
	metricpb "go.opentelemetry.io/proto/otlp/metrics/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"
)

const (
	messageTypeUnset uint8 = iota
	messageTypeTrace
	messageTypeMetric
	messageTypeLog
)

// Options are used when creating a Exporter.
type Options interface {
	apply(*Exporter)
}

// Exporter exports logs, metrics, and traces to an oltcpreceiver.
type Exporter struct {
	writer  *writer
	network string
	address string
}

var (
	_ otlpmetric.Client = &Exporter{}
	_ otlptrace.Client  = &Exporter{}
)

// NewExporter creates a new exporter.  It is recommended to create a different exporter
// for logs, metrics, and traces.
func NewExporter(network string, address string, options ...Options) (*Exporter, error) {
	e := Exporter{
		network: network,
		address: address,
		writer:  &writer{},
	}

	for _, o := range options {
		o.apply(&e)
	}

	return &e, nil
}

func (e *Exporter) connect(ctx context.Context) (io.Writer, error) {
	return e.writer.dialContext(ctx, e.network, e.address)
}

func (e *Exporter) disconnect() {
	e.writer.disconnect()
}

// Start the exporter.
func (e *Exporter) Start(ctx context.Context) error {
	_, _ = e.connect(ctx)

	return nil
}

// Stop the exporter. All future uploads will fail.
func (e *Exporter) Stop(ctx context.Context) error {
	return e.Shutdown(ctx)
}

// UploadTraces transmits metric data to an OTLP TCP receiver.
func (e *Exporter) UploadTraces(ctx context.Context, protoSpans []*tracepb.ResourceSpans) error {
	request := tracepb.TracesData{
		ResourceSpans: protoSpans,
	}

	return e.upload(ctx, messageTypeTrace, &request)
}

type writer struct {
	mu     sync.Mutex
	writer io.WriteCloser
	closed bool
}

func (w *writer) Write(data []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed || w.writer == nil {
		return 0, net.ErrClosed
	}

	return w.writer.Write(data)
}

func (w *writer) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return nil
	}

	w.closed = true

	if w.writer == nil {
		return nil
	}

	return w.writer.Close()
}

func (w *writer) disconnect() {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return
	}

	if w.writer == nil {
		return
	}

	w.writer.Close()

	w.writer = nil
}

func (w *writer) dialContext(ctx context.Context, network string, address string) (io.Writer, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.writer != nil {
		return w.writer, nil
	}

	err := retry.Do(func() error {
		c, err := memconn.DialContext(ctx, network, address)
		if err != nil {
			return err
		}

		w.writer = c

		return nil
	},
		retry.Context(ctx),
		retry.LastErrorOnly(true),
		retry.Attempts(10),
	)
	if err != nil {
		return nil, err
	}

	if w.writer != nil {
		return w.writer, nil
	}

	return nil, errors.New("unable to connect")
}

func (e *Exporter) upload(ctx context.Context, messageType uint8, message proto.Message) error {
	data, err := proto.Marshal(message)
	if err != nil {
		return err
	}

	if len(data) == 0 {
		return nil
	}

	prefix := []byte{messageType, 0, 0, 0, 0}
	binary.BigEndian.PutUint32(prefix[1:], uint32(len(data)))

	buf := bufPool.Get().(*bytes.Buffer)
	defer bufPool.Put(buf)

	buf.Grow(5 + len(data))

	if _, err := buf.Write(prefix); err != nil {
		return err
	}

	if _, err := buf.Write(data); err != nil {
		return err
	}

	w, err := e.connect(ctx)
	if err != nil {
		return err
	}

	if _, err := buf.WriteTo(w); err != nil {
		e.disconnect()
		return err
	}

	return nil
}

// UploadMetrics transmits metric data to an OTLP TCP receiver.
func (e *Exporter) UploadMetrics(ctx context.Context, protoMetrics *metricpb.ResourceMetrics) error {
	request := metricpb.MetricsData{
		ResourceMetrics: []*metricpb.ResourceMetrics{protoMetrics},
	}

	return e.upload(ctx, messageTypeMetric, &request)
}

// UploadLogs transmits metric data to an OTLP TCP receiver.
func (e *Exporter) UploadLogs(ctx context.Context, protoLogs *logpb.ResourceLogs) error {
	request := logpb.LogsData{
		ResourceLogs: []*logpb.ResourceLogs{protoLogs},
	}

	return e.upload(ctx, messageTypeLog, &request)
}

var bufPool = sync.Pool{
	New: func() any {
		return &bytes.Buffer{}
	},
}

// ForceFlush flushes any metric data held by an Exporter.
// Currently, it does nothing
func (e *Exporter) ForceFlush(context.Context) error {
	return nil
}

// Shutdown the exporter. All future uploads will fail.
func (e *Exporter) Shutdown(context.Context) error {
	return e.writer.Close()
}
