package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os/exec"
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
)

type sybilConfig struct {
	BinPath string `json:"bin_path"`
	DBPath  string `json:"db_path"`
}

type sybil struct {
	cfg sybilConfig

	sync.Mutex
	numSpans int
	buffer   *bytes.Buffer
}

func (sy *sybil) writeSpan(span *model.Span) error {
	byt, err := jsonFromSpan(span)
	if err != nil {
		return err
	}
	byt = append(byt, []byte("\n")...)

	sy.Lock()
	_, err = sy.buffer.Write(byt)
	if err != nil {
		sy.Unlock()
		return err
	}

	sy.numSpans++

	if sy.numSpans >= 256 {
		sy.flushAndClearBuffer()
	}

	sy.Unlock()
	return nil
}

func jsonFromSpan(span *model.Span) ([]byte, error) {
	inputMap := make(map[string]interface{})
	inputMap[timeKey] = model.TimeAsEpochMicroseconds(span.StartTime)

	inputMap[serviceOpPrefix+span.Process.ServiceName+serviceOpSeparator+span.OperationName] = true
	inputMap[serviceOnlyPrefix+span.Process.ServiceName] = true
	inputMap[durationKey] = span.Duration.Nanoseconds()

	for _, kv := range span.Tags {
		inputMap[tagPrefix+kv.Key] = kv.AsString()
	}
	for _, kv := range span.Process.Tags {
		inputMap[tagPrefix+kv.Key] = kv.AsString()
	}
	for _, log := range span.Logs {
		for _, kv := range log.Fields {
			inputMap[tagPrefix+kv.Key] = kv.AsString()
		}
	}

	inputMap[traceIDKey] = span.TraceID.String()

	return json.Marshal(inputMap)
}

func (sy *sybil) flushAndClearBuffer() {
	jsonbytes := sy.buffer.Bytes()
	sy.buffer = bytes.NewBuffer(make([]byte, 0, len(jsonbytes)))
	sy.numSpans = 0

	go sy.flushJSON(jsonbytes)
}

func (sy *sybil) flushJSON(input []byte) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, sy.cfg.BinPath, "ingest", "-table", "jaeger", "-dir", sy.cfg.DBPath)
	cmd.Stdin = bytes.NewReader(input)

	if out, err := cmd.CombinedOutput(); err != nil {
		logger.Error("sybil flush json", "err", err, "message", string(out))
	}
}

func (sy *sybil) getServices(ctx context.Context) ([]string, error) {
	cols, err := sy.getIntColumns(ctx)
	if err != nil {
		return nil, err
	}

	services := make([]string, 0, 10)
	for _, col := range cols {
		if !strings.HasPrefix(col, serviceOnlyPrefix) {
			continue
		}

		services = append(services, strings.TrimPrefix(col, serviceOnlyPrefix))
	}

	sort.Strings(services)

	return services, nil
}

func (sy *sybil) getOperations(ctx context.Context, service string) ([]string, error) {
	cols, err := sy.getIntColumns(ctx)

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

type tableInfo struct {
	Columns struct {
		Ints []string `json:"ints"`
	} `json:"columns"`
}

func (sy *sybil) getIntColumns(ctx context.Context) ([]string, error) {
	cmd := exec.CommandContext(ctx, sy.cfg.BinPath, "query", "-table", "jaeger", "-info", "-json", "-dir", sy.cfg.DBPath)

	out, err := cmd.CombinedOutput()
	if err != nil {
		logger.Error("err", err, "message", string(out))
		return nil, err
	}

	ti := &tableInfo{}
	if err := json.Unmarshal(out, ti); err != nil {
		return nil, err
	}

	return ti.Columns.Ints, nil
}

type queryResult struct {
	TraceID string `json:"traceID"` // traceIDKey
}

func (sy *sybil) findTraceIDs(ctx context.Context, query *spanstore.TraceQueryParameters) ([]model.TraceID, error) {
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

	flags := generateFlagsFromQuery(query)
	flags = append([]string{"query", "-table", "jaeger", "-json", "-dir", sy.cfg.DBPath, "-read-log"}, flags...)
	cmd := exec.CommandContext(ctx, sy.cfg.BinPath, flags...)

	out, err := cmd.CombinedOutput()
	if err != nil {
		logger.Error("err", err, "message", string(out))
		return nil, err
	}

	results := make([]queryResult, 0, query.NumTraces)
	if err := json.Unmarshal(out, &results); err != nil {
		return nil, err
	}

	traceIDs := make([]model.TraceID, 0, len(results))
	for _, qr := range results {
		tid, err := model.TraceIDFromString(qr.TraceID)
		if err != nil {
			return nil, err
		}

		traceIDs = append(traceIDs, tid)
	}

	return traceIDs, nil
}

func generateFlagsFromQuery(query *spanstore.TraceQueryParameters) []string {
	// We group by the service/op column so that we only pick traces for which it is relevant.
	groupColumns := []string{}
	intFilters := []string{}
	strFilters := []string{}

	if query.OperationName == "" {
		groupColumns = append(groupColumns, serviceOnlyPrefix+query.ServiceName)
	} else {
		groupColumns = append(groupColumns, serviceOpPrefix+query.ServiceName+serviceOpSeparator+query.OperationName)
	}
	groupColumns = append(groupColumns, traceIDKey)

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

	logger.Warn("flags", "flags", flags)

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

// TODO have a much simpler schema with service: service, op: op and put the getServices and getOperations into KVStore.
