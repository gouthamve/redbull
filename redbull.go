package main

import (
	"bytes"
	"context"
	"time"

	"github.com/dgraph-io/badger"
	"github.com/jaegertracing/jaeger/model"
	"github.com/jaegertracing/jaeger/storage/dependencystore"
	"github.com/jaegertracing/jaeger/storage/spanstore"
)

const sizeOfTraceID = 16

type redbull struct {
	sybil  sybil
	badger *badger.DB
}

func newRedBull() (*redbull, error) {
	db, err := badger.Open(badger.DefaultOptions("/home/goutham/go/src/github.com/gouthamve/redbull/badger-db"))
	if err != nil {
		return nil, err
	}

	return &redbull{
		sybil: sybil{
			cfg: sybilConfig{
				"sybil",
				"/home/goutham/go/src/github.com/gouthamve/redbull/db",
			},

			buffer: bytes.NewBuffer(nil),
		},

		badger: db,
	}, nil
}

func (rb *redbull) Close() error {
	return rb.badger.Close()
}

func (rb *redbull) DependencyReader() dependencystore.Reader {
	return rb
}

func (rb *redbull) SpanReader() spanstore.Reader {
	return rb
}

func (rb *redbull) SpanWriter() spanstore.Writer {
	return rb
}

func (rb *redbull) GetDependencies(endTs time.Time, lookback time.Duration) ([]model.DependencyLink, error) {
	return nil, nil
}
func (rb *redbull) GetTrace(ctx context.Context, traceID model.TraceID) (*model.Trace, error) {
	return getTraceFromDB(rb.badger, traceID)
}
func (rb *redbull) GetServices(ctx context.Context) ([]string, error) {
	return rb.sybil.getServices(ctx)
}
func (rb *redbull) GetOperations(ctx context.Context, service string) ([]string, error) {
	return rb.sybil.getOperations(ctx, service)
}
func (rb *redbull) FindTraces(ctx context.Context, query *spanstore.TraceQueryParameters) ([]*model.Trace, error) {
	traceIDs, err := rb.sybil.findTraceIDs(ctx, query)
	if err != nil {
		return nil, err
	}

	return getTracesFromDB(rb.badger, traceIDs)
}
func (rb *redbull) FindTraceIDs(ctx context.Context, query *spanstore.TraceQueryParameters) ([]model.TraceID, error) {
	return rb.sybil.findTraceIDs(ctx, query)
}
func (rb *redbull) WriteSpan(span *model.Span) error {
	// Write to KV Store.
	if err := writeSpanToDB(rb.badger, span); err != nil {
		return err
	}

	// Write to columnar index store.
	return rb.sybil.writeSpan(span)
}
