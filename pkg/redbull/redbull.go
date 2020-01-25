package redbull

import (
	"context"
	"flag"
	"path/filepath"
	"time"

	"github.com/jaegertracing/jaeger/model"
	"github.com/jaegertracing/jaeger/storage/dependencystore"
	"github.com/jaegertracing/jaeger/storage/spanstore"
	"github.com/spf13/viper"
	"github.com/uber/jaeger-lib/metrics"
	"go.uber.org/zap"
)

var (
	logger *zap.SugaredLogger
)

const (
	sizeOfTraceID = 16

	retentionFlag = "redbull.retention"
	dataDirFlag   = "redbull.data-dir"
	binPathFlag   = "redbull.sybil-path"
)

// Factory is the redbull factory that implements storage.Factory.
type Factory struct {
	cfg Config
	*redbull
}

// NewFactory returns a new factory.
func NewFactory() *Factory {
	return &Factory{}
}

// AddFlags implements plugin.Configurable.
func (f *Factory) AddFlags(flagset *flag.FlagSet) {
	flagset.Duration(retentionFlag, 24*time.Hour, "The retention period for redbull.")
	flagset.String(dataDirFlag, "/data/", "The data directory for redbull.")
	flagset.String(binPathFlag, "sybil", "The path to the sybil binary.")
}

// InitFromViper implements plugin.Configurable.
func (f *Factory) InitFromViper(v *viper.Viper) {
	f.cfg.retention = v.GetDuration(retentionFlag)
	f.cfg.dataDir = v.GetString(dataDirFlag)
	f.cfg.sybilBinpath = v.GetString(binPathFlag)
}

// Initialize implements storage.Factory.
func (f *Factory) Initialize(metricsFactory metrics.Factory, zapLogger *zap.Logger) error {
	logger = zapLogger.Sugar()

	var err error
	f.redbull, err = NewRedBull(f.cfg)
	return err
}

// CreateSpanReader implements storage.Factory.
func (f *Factory) CreateSpanReader() (spanstore.Reader, error) {
	return f.redbull.SpanReader(), nil
}

// CreateSpanWriter implements storage.Factory.
func (f *Factory) CreateSpanWriter() (spanstore.Writer, error) {
	return f.redbull.SpanWriter(), nil
}

// CreateDependencyReader implements storage.Factory.
func (f *Factory) CreateDependencyReader() (dependencystore.Reader, error) {
	return f.redbull.DependencyReader(), nil
}

type redbull struct {
	cfg Config

	sybil sybil
	kv    *kvstore
}

// Config holds the configuration for redbull.
type Config struct {
	retention time.Duration
	dataDir   string

	sybilBinpath string
}

// NewRedBull creates a redbull instance.
func NewRedBull(cfg Config) (*redbull, error) {
	kv, err := newKVStore(filepath.Join(cfg.dataDir, "badger-db"), cfg.retention)
	if err != nil {
		return nil, err
	}

	rb := &redbull{
		cfg: cfg,

		sybil: newSybil(sybilConfig{
			BinPath:   cfg.sybilBinpath,
			DBPath:    filepath.Join(cfg.dataDir, "sybil-db"),
			Retention: cfg.retention,
		}),
		kv: kv,
	}

	rb.sybil.start()
	rb.kv.start()

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
func (rb *redbull) GetOperations(ctx context.Context, query spanstore.OperationQueryParameters) ([]spanstore.Operation, error) {
	operations, err := rb.sybil.getOperations(ctx, query.ServiceName)
	if err != nil {
		return nil, err
	}

	ops := make([]spanstore.Operation, 0, len(operations))
	for _, op := range operations {
		ops = append(ops, spanstore.Operation{Name: op})
	}

	return ops, nil
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
