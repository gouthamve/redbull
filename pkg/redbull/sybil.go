package redbull

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jaegertracing/jaeger/model"
	"github.com/jaegertracing/jaeger/storage/spanstore"
)

const (
	timeKey = "time"

	serviceOpPrefix    = "_idx_so_"
	serviceOpSeparator = "+"
	serviceOnlyPrefix  = "_idx_svc_"
	durationKey        = "_idx_duration"
	tagPrefix          = "_idx_tag_"
	traceIDKey         = "traceID"

	maxSybilRecordsInFlight = 1024

	maxRecordInFuture = 15 * time.Minute
	maxRecordInPast   = 15 * time.Minute
)

var (
	// ErrServiceNameNotSet occurs when attempting to query with an empty service name
	ErrServiceNameNotSet = errors.New("service name must be set")

	// ErrStartTimeMinGreaterThanMax occurs when start time min is above start time max
	ErrStartTimeMinGreaterThanMax = errors.New("min start time is above max")

	// ErrDurationMinGreaterThanMax occurs when duration min is above duration max
	ErrDurationMinGreaterThanMax = errors.New("min duration is above max")

	// ErrMalformedRequestObject occurs when a request object is nil
	ErrMalformedRequestObject = errors.New("malformed request object")

	// ErrStartAndEndTimeNotSet occurs when start time and end time are not set
	ErrStartAndEndTimeNotSet = errors.New("start and end time must be set")

	// ErrUnableToFindTraceIDAggregation occurs when an aggregation query for TraceIDs fail.
	ErrUnableToFindTraceIDAggregation = errors.New("could not find aggregation of traceIDs")

	// ErrNotSupported during development, don't support every option - yet
	ErrNotSupported = errors.New("this query parameter is not supported yet")

	errDropSpanCuzBlockDoesntExistfmt = "error block doesn't exist, period: %d, span start time: %s"

	bufPool = sync.Pool{
		New: func() interface{} {
			return new(bytes.Buffer)
		},
	}

	periodPerBlock = time.Hour.Nanoseconds()

	emptyJSONBytes = []byte("{}\n")
)

type sybilConfig struct {
	BinPath string `json:"bin_path"`
	DBPath  string `json:"db_path"`

	Retention time.Duration `json:"retention"`
}

type sybil struct {
	cfg sybilConfig

	numSpansMtx sync.Mutex
	numSpans    int

	blocksMtx sync.RWMutex
	blocks    map[int]*sybilBlock

	done chan struct{}
}

func newSybil(cfg sybilConfig) sybil {
	return sybil{
		cfg: cfg,

		blocks: make(map[int]*sybilBlock),

		done: make(chan struct{}),
	}
}

func (sy *sybil) start() {
	if err := sy.refreshBlocks(); err != nil {
		logger.Errorw("startup refresh", "err", err)
	}
	go sy.loop()
}

func (sy *sybil) loop() {
	digestTicker := time.NewTicker(2 * time.Second)
	refreshBlocksTicker := time.NewTicker(2 * time.Minute)
	defer digestTicker.Stop()
	defer refreshBlocksTicker.Stop()
	for {
		select {
		case <-sy.done:
			return
		case <-digestTicker.C:
			sy.digestRowStore()
		case <-refreshBlocksTicker.C:
			if err := sy.refreshBlocks(); err != nil {
				logger.Errorw("refresh blocks", "err", err)
			}
		}
	}
}

func (sy *sybil) stop() {
	close(sy.done)
}

func (sy *sybil) refreshBlocks() error {
	// TODO: Refactor this mess and implement retention.
	// Get a list of tables. TODO: Implement this in sybil itself.
	files, err := ioutil.ReadDir(sy.cfg.DBPath)
	if err != nil {
		if os.IsNotExist(err) {
			if err := os.MkdirAll(sy.cfg.DBPath, 0777); err != nil {
				return err
			}
		} else {

			return err
		}
	}
	existingTables := make(map[int]struct{}, len(files))
	for _, file := range files {
		if !file.IsDir() {
			logger.Warnw("list tables", "msg", "non directory file", "file", file.Name())
			continue
		}

		i, err := strconv.Atoi(file.Name())
		if err != nil {
			logger.Warnw("list tables", "msg", "non int table", "table", file.Name())
			continue
		}

		existingTables[i] = struct{}{}
	}

	start := time.Now()
	minWriteAblePeriod := int(time.Now().Add(-maxRecordInPast).UnixNano() / periodPerBlock)
	maxWriteAblePeriod := int(time.Now().Add(maxRecordInFuture).UnixNano() / periodPerBlock)
	minPeriodToExist := int(time.Now().Add(-sy.cfg.Retention).UnixNano() / periodPerBlock)

	tablesToExist := make(map[int]struct{}, len(existingTables))
	for i := minPeriodToExist; i <= maxWriteAblePeriod; i++ {
		tablesToExist[i] = struct{}{}
	}
	fmt.Println(minWriteAblePeriod, maxWriteAblePeriod, minPeriodToExist)

	blocksDeleted := 0
	// Nuke all tables that shouldn't exist.
	for i := range existingTables {
		if _, ok := tablesToExist[i]; !ok {
			sy.blocksMtx.Lock()
			delete(sy.blocks, i)
			sy.blocksMtx.Unlock()

			logger.Infow("nuking table", "table", i)
			if err := os.RemoveAll(filepath.Join(sy.cfg.DBPath, strconv.Itoa(i))); err != nil {
				logger.Errorw("nuking table", "table", i, "err", err)
				return err
			}
			blocksDeleted++
		}
	}

	blocksToCreate := map[int]*sybilBlock{}

	sy.blocksMtx.Lock()
	for i := range sy.blocks {
		if _, ok := tablesToExist[i]; !ok {
			delete(sy.blocks, i)
		}
	}

	for i := range tablesToExist {
		if _, ok := sy.blocks[i]; !ok {
			block := newSybilBlock(sy.cfg.BinPath, sy.cfg.DBPath, i)
			sy.blocks[i] = block
			blocksToCreate[i] = block
		}
	}
	sy.blocksMtx.Unlock()

	numMutabilitySet := 0
	sy.blocksMtx.RLock()
	for blockPeriod, block := range sy.blocks {
		if blockPeriod >= minWriteAblePeriod {
			continue
		}

		if !block.getMutable() {
			continue
		}

		block.setMutable(false)
		numMutabilitySet++
	}
	sy.blocksMtx.RUnlock()

	for _, block := range blocksToCreate {
		block.runIngest(emptyJSONBytes[:])
	}

	logger.Infow("refresh block status", "duration", time.Since(start).String(), "mutability_changes", numMutabilitySet, "blocks_deleted", blocksDeleted, "blocks_created", len(blocksToCreate))

	return nil
}

func (sy *sybil) digestRowStore() {
	sy.blocksMtx.RLock()
	for _, block := range sy.blocks {
		block.digestRowStore()
	}
	sy.blocksMtx.RUnlock()
}

func (sy *sybil) writeSpan(span *model.Span) error {
	periodForSpan := int(span.StartTime.UnixNano() / periodPerBlock)

	sy.blocksMtx.RLock()
	block, ok := sy.blocks[periodForSpan]
	sy.blocksMtx.RUnlock()
	if !ok {
		return fmt.Errorf(errDropSpanCuzBlockDoesntExistfmt, periodForSpan, span.StartTime.String())
	}

	if err := block.writeSpan(span); err != nil {
		return err
	}

	sy.numSpansMtx.Lock()

	sy.numSpans++
	if sy.numSpans >= maxSybilRecordsInFlight {
		sy.flushAndClearBuffer()
		sy.numSpans = 0
	}

	sy.numSpansMtx.Unlock()
	return nil
}

func (sy *sybil) flushAndClearBuffer() {
	sy.blocksMtx.RLock()
	for _, block := range sy.blocks {
		block.flushAndClearBuffer()
	}
	sy.blocksMtx.RUnlock()
}

func (sy *sybil) getServices(ctx context.Context) ([]string, error) {
	servicesMap := make(map[string]struct{})

	sy.blocksMtx.RLock()
	defer sy.blocksMtx.RUnlock()

	for _, block := range sy.blocks {
		services, err := block.getServices(ctx)
		if err != nil {
			return nil, err
		}

		for _, svc := range services {
			servicesMap[svc] = struct{}{}
		}
	}

	services := make([]string, 0, len(servicesMap))
	for svc := range servicesMap {
		services = append(services, svc)
	}

	sort.Strings(services)

	return services, nil
}

func (sy *sybil) getOperations(ctx context.Context, service string) ([]string, error) {
	opsMap := make(map[string]struct{})

	sy.blocksMtx.RLock()
	defer sy.blocksMtx.RUnlock()

	for _, block := range sy.blocks {
		ops, err := block.getOperations(ctx, service)
		if err != nil {
			return nil, err
		}

		for _, op := range ops {
			opsMap[op] = struct{}{}
		}
	}

	ops := make([]string, 0, len(opsMap))
	for op := range opsMap {
		ops = append(ops, op)
	}

	sort.Strings(ops)

	return ops, nil
}

type queryResult struct {
	TraceID string `json:"traceID"` // traceIDKey
}

func (sy *sybil) findTraceIDs(ctx context.Context, query *spanstore.TraceQueryParameters) ([]model.TraceID, error) {
	start := time.Now()
	if err := validateQuery(query); err != nil {
		return nil, err
	}

	// Make sure the column exists.
	validOp := false || query.OperationName == ""
	if !validOp {
		ops, err := sy.getOperations(ctx, query.ServiceName)
		if err != nil {
			return nil, err
		}
		for _, op := range ops {
			if op == query.OperationName {
				validOp = true
			}
		}
	}

	// Short circuit if the operation doesn't exist in db.
	if !validOp {
		return nil, nil
	}

	// Get all the relevant blocks to query.
	retentionTime := time.Now().Add(-sy.cfg.Retention)
	if retentionTime.After(query.StartTimeMin) {
		query.StartTimeMin = retentionTime
	}
	maxFutureTime := time.Now().Add(maxRecordInFuture)
	if maxFutureTime.Before(query.StartTimeMax) {
		query.StartTimeMax = maxFutureTime
	}

	minPeriod := query.StartTimeMin.UnixNano() / periodPerBlock
	maxPeriod := query.StartTimeMax.UnixNano() / periodPerBlock
	traceIDsMap := make(map[string]struct{}, query.NumTraces)

	// TODO: Do some intelligent paralellising later.
	for i := maxPeriod; i >= minPeriod; i-- {
		sy.blocksMtx.RLock()
		block, ok := sy.blocks[int(i)]
		sy.blocksMtx.RUnlock()
		if !ok {
			logger.Warnw("missing block for valid time", "table", i, "now", time.Now().String())
			continue
		}

		traceIDs, err := block.query(ctx, query)
		if err != nil {
			return nil, err
		}

		for _, tid := range traceIDs {
			traceIDsMap[tid] = struct{}{}
		}

		if len(traceIDsMap) >= query.NumTraces {
			break
		}
	}

	traceIDs := make([]model.TraceID, 0, len(traceIDsMap))
	for tidStr := range traceIDsMap {
		tid, err := model.TraceIDFromString(tidStr)
		if err != nil {
			return nil, err
		}

		traceIDs = append(traceIDs, tid)
	}

	logger.Warnw("sybil query", "duration", time.Since(start).String(), "traceIDs", len(traceIDs))
	return traceIDs, nil
}

func generateFlagsFromQuery(query *spanstore.TraceQueryParameters) []string {
	// We group by the service/op column so that we only pick traces for which it is relevant.
	groupColumns := []string{}
	intFilters := []string{}
	strFilters := []string{}

	groupColumns = append(groupColumns, traceIDKey)

	// service and operation filters.
	if query.OperationName == "" {
		intFilters = append(intFilters, serviceOnlyPrefix+query.ServiceName+":eq:1")
	} else {
		intFilters = append(intFilters, serviceOpPrefix+query.ServiceName+serviceOpSeparator+query.OperationName+":eq:1")
	}
	// StartTime filters.
	lt := strconv.FormatUint(model.TimeAsEpochMicroseconds(query.StartTimeMax), 10)
	gt := strconv.FormatUint(model.TimeAsEpochMicroseconds(query.StartTimeMin), 10)
	intFilters = append(intFilters, "time:lt:"+lt, "time:gt:"+gt)

	// Duration filters.
	if query.DurationMin > 0 {
		intFilters = append(intFilters, durationKey+":gt:"+strconv.FormatInt(query.DurationMin.Nanoseconds(), 10))
	}
	if query.DurationMax > 0 {
		intFilters = append(intFilters, durationKey+":lt:"+strconv.FormatInt(query.DurationMax.Nanoseconds(), 10))
	}

	// The tag filters.
	for key, value := range query.Tags {
		strFilters = append(strFilters, tagPrefix+key+":eq:"+value)
	}

	flags := []string{}
	flags = append(flags, "-int-filter", strings.Join(intFilters, ","))
	flags = append(flags, "-group", strings.Join(groupColumns, ","))
	flags = append(flags, "-limit", strconv.Itoa(query.NumTraces))
	if len(strFilters) > 0 {
		flags = append(flags, "-str-filter", strings.Join(strFilters, ","))
	}

	return flags
}

func validateQuery(p *spanstore.TraceQueryParameters) error {
	if p == nil {
		return ErrMalformedRequestObject
	}
	if p.ServiceName == "" {
		return ErrServiceNameNotSet
	}

	if p.StartTimeMin.IsZero() || p.StartTimeMax.IsZero() {
		return ErrStartAndEndTimeNotSet
	}

	if p.StartTimeMax.Before(p.StartTimeMin) {
		return ErrStartTimeMinGreaterThanMax
	}
	if p.DurationMin != 0 && p.DurationMax != 0 && p.DurationMin > p.DurationMax {
		return ErrDurationMinGreaterThanMax
	}
	return nil
}

type sybilBlock struct {
	sync.RWMutex

	binPath   string
	dbPath    string
	tableName int

	buffer *bytes.Buffer

	// If this block accepts writes.
	isMutable bool
	// int columns.
	intCols []string

	digestMtx sync.RWMutex
}

func newSybilBlock(binPath, dbPath string, tableName int) *sybilBlock {
	return &sybilBlock{
		binPath:   binPath,
		dbPath:    dbPath,
		tableName: tableName,

		buffer:    bytes.NewBuffer(nil),
		isMutable: true,
	}
}

func (syb *sybilBlock) getMutable() bool {
	syb.RLock()
	defer syb.RUnlock()
	return syb.isMutable
}

func (syb *sybilBlock) setMutable(mutable bool) {
	syb.Lock()
	syb.isMutable = mutable
	syb.Unlock()
}

func (syb *sybilBlock) writeSpan(span *model.Span) error {
	mutable := syb.getMutable()
	if !mutable {
		logger.Warnw("write span: dropping span", "msg", "too old", "tableName", syb.tableName, "spanStart", span.StartTime.String())
		return nil
	}

	byt, err := jsonFromSpan(span)
	if err != nil {
		return err
	}
	byt = append(byt, []byte("\n")...)

	syb.Lock()
	_, err = syb.buffer.Write(byt)
	if err != nil {
		syb.Unlock()
		return err
	}
	syb.Unlock()
	return nil
}

func jsonFromSpan(span *model.Span) ([]byte, error) {
	inputMap := make(map[string]interface{})
	inputMap[timeKey] = model.TimeAsEpochMicroseconds(span.StartTime)

	// These 3 are special. The reason for the first two is that there is a GetServices() and GetOperations()
	// lookup on the global space. I can think of easy ways to do this in a jaeger tuned columnar store, because its
	// just loading the symbol table and not the entire column. For now, we use this hack.
	inputMap[serviceOpPrefix+span.Process.ServiceName+serviceOpSeparator+span.OperationName] = 1
	inputMap[serviceOnlyPrefix+span.Process.ServiceName] = 1
	inputMap[durationKey] = span.Duration.Nanoseconds()

	for _, kv := range span.Tags {
		inputMap[tagPrefix+kv.Key] = kv.AsString()
	}
	for _, kv := range span.Process.Tags {
		inputMap[tagPrefix+kv.Key] = kv.AsString()
	}

	inputMap[traceIDKey] = span.TraceID.String()

	return json.Marshal(inputMap)
}
func (syb *sybilBlock) flushAndClearBuffer() {
	mutable := syb.getMutable()
	if !mutable {
		return
	}

	syb.Lock()
	oldBuf := syb.buffer
	syb.buffer = bufPool.Get().(*bytes.Buffer)
	syb.buffer.Reset()
	syb.Unlock()

	go syb.flushJSON(oldBuf)
}

func (syb *sybilBlock) flushJSON(buf *bytes.Buffer) {
	syb.runIngest(buf.Bytes())

	buf.Reset()
	bufPool.Put(buf)
}

func (syb *sybilBlock) runIngest(b []byte) {
	if len(b) == 0 {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	flags := []string{"ingest", "-table", strconv.Itoa(syb.tableName), "-dir", syb.dbPath, "-skip-compact", "-save-srb"}
	cmd := exec.CommandContext(ctx, syb.binPath, flags...)
	cmd.Stdin = bytes.NewReader(b)

	if out, err := cmd.CombinedOutput(); err != nil {
		logger.Errorw("sybil run ingest", "err", err, "message", string(out), "table", syb.tableName, "flags", flags)
	}

}

func (syb *sybilBlock) digestRowStore() {
	mutable := syb.getMutable()
	if !mutable {
		return
	}

	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()

	syb.digestMtx.Lock()

	cmd := exec.CommandContext(ctx, syb.binPath, "digest", "-debug", "-table", strconv.Itoa(syb.tableName), "-dir", syb.dbPath)

	if out, err := cmd.CombinedOutput(); err != nil {
		logger.Errorw("sybil digest", "err", err, "message", string(out), "table", syb.tableName)
	}

	syb.digestMtx.Unlock()

	logger.Warnw("sybil digest", "duration", time.Since(start).String(), "table", syb.tableName)
	return
}

type tableInfo struct {
	Columns struct {
		Ints []string `json:"ints"`
	} `json:"columns"`
}

func (syb *sybilBlock) getIntColumns(ctx context.Context) ([]string, error) {
	mutable := syb.getMutable()
	if !mutable && len(syb.intCols) > 0 {
		return syb.intCols, nil
	}

	cmd := exec.CommandContext(ctx, syb.binPath, "query", "-table", strconv.Itoa(syb.tableName), "-info", "-json", "-dir", syb.dbPath)

	out, err := cmd.CombinedOutput()
	if err != nil {
		logger.Errorw("err", err, "message", string(out))
		return nil, err
	}

	ti := &tableInfo{}
	if err := json.Unmarshal(out, ti); err != nil {
		return nil, err
	}

	// TODO: Fix race.
	if !mutable {
		syb.intCols = ti.Columns.Ints
	}

	return ti.Columns.Ints, nil
}

func (syb *sybilBlock) getServices(ctx context.Context) ([]string, error) {
	cols, err := syb.getIntColumns(ctx)
	if err != nil {
		return nil, err
	}

	services := make([]string, 10)
	for _, col := range cols {
		if !strings.HasPrefix(col, serviceOnlyPrefix) {
			continue
		}

		services = append(services, strings.TrimPrefix(col, serviceOnlyPrefix))
	}

	sort.Strings(services)

	return services, nil
}

func (syb *sybilBlock) getOperations(ctx context.Context, service string) ([]string, error) {
	cols, err := syb.getIntColumns(ctx)

	if err != nil {
		return nil, err
	}

	ops := []string{}
	prefix := serviceOpPrefix + service + serviceOpSeparator
	for _, col := range cols {
		if !strings.HasPrefix(col, prefix) {
			continue
		}

		ops = append(ops, strings.TrimPrefix(col, prefix))
	}

	sort.Strings(ops)
	return ops, nil
}

func (syb *sybilBlock) query(ctx context.Context, query *spanstore.TraceQueryParameters) ([]string, error) {
	flags := generateFlagsFromQuery(query)
	flags = append([]string{"query", "-table", strconv.Itoa(syb.tableName), "-json", "-dir", syb.dbPath, "-cache-queries"}, flags...)
	logger.Warnw("sybil query", "flags", strings.Join(flags, " "))

	// Stop digestion while a query is running.
	// TODO: This means that digestion is blocked.
	syb.digestMtx.RLock()
	cmdStartTime := time.Now()
	cmd := exec.CommandContext(ctx, syb.binPath, flags...)
	out, err := cmd.CombinedOutput()
	cmdEndTime := time.Now()
	syb.digestMtx.RUnlock()

	logger.Warnw("sybil query command exec", "duration", cmdEndTime.Sub(cmdStartTime).String())

	if err != nil {
		logger.Errorw("error querying", "err", err, "message", string(out))
		return nil, err
	}

	results := make([]queryResult, 0, query.NumTraces)
	if err := json.Unmarshal(out, &results); err != nil {
		return nil, err
	}

	traceIDs := make([]string, 0, len(results))
	for _, qr := range results {
		traceIDs = append(traceIDs, qr.TraceID)
	}

	return traceIDs, nil
}

// TODO have a much simpler schema with service: service, op: op and put the getServices and getOperations into KVStore.
