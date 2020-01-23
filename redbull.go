package main

import (
	"context"
	"path/filepath"
	"time"

	"github.com/jaegertracing/jaeger/model"
	"github.com/jaegertracing/jaeger/storage/dependencystore"
	"github.com/jaegertracing/jaeger/storage/spanstore"
)

const sizeOfTraceID = 16

type redbull struct {
	cfg config

	sybil sybil
	kv    *kvstore
}

type config struct {
	retention time.Duration
	dataDir   string
}

func newRedBull() (*redbull, error) {
	cfg := config{
		retention: 24 * time.Hour,
		dataDir:   "/home/goutham/go/src/github.com/gouthamve/redbull/",
	}

	kv, err := newKVStore(filepath.Join(cfg.dataDir, "badger-db"), cfg.retention)
	if err != nil {
		return nil, err
	}

	rb := &redbull{
		cfg: cfg,

		sybil: newSybil(sybilConfig{
			BinPath:   "sybil",
			DBPath:    filepath.Join(cfg.dataDir, "sybil-db"),
			Retention: cfg.retention,
		}),
		kv: kv,
	}

	rb.sybil.start()

	return rb, nil
}

func (rb *redbull) Close() error {
	rb.sybil.stop()
	return rb.kv.stop()
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
	return rb.kv.getTrace(traceID)
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

	return rb.kv.getTraces(traceIDs)
}
func (rb *redbull) FindTraceIDs(ctx context.Context, query *spanstore.TraceQueryParameters) ([]model.TraceID, error) {
	return rb.sybil.findTraceIDs(ctx, query)
}
func (rb *redbull) WriteSpan(span *model.Span) error {
	// Write to KV Store.
	if err := rb.kv.addSpan(span); err != nil {
		return err
	}

	// Write to columnar index store.
	return rb.sybil.writeSpan(span)
}
