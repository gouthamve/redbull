package main

import (
	"encoding/binary"
	"time"

	"github.com/dgraph-io/badger"
	"github.com/gogo/protobuf/proto"
	"github.com/jaegertracing/jaeger/model"
)

func writeSpanToDB(db *badger.DB, span *model.Span, expiryTime time.Time) error {
	// Write to KV Store.
	key := make([]byte, sizeOfTraceID+8+8)
	pos := 0
	binary.BigEndian.PutUint64(key[pos:], span.TraceID.High)
	pos += 8
	binary.BigEndian.PutUint64(key[pos:], span.TraceID.Low)
	pos += 8
	binary.BigEndian.PutUint64(key[pos:], model.TimeAsEpochMicroseconds(span.StartTime))
	pos += 8
	binary.BigEndian.PutUint64(key[pos:], uint64(span.SpanID))

	var bb []byte
	var err error

	bb, err = proto.Marshal(span)
	if err != nil {
		return err
	}

	entry := &badger.Entry{
		Key:       key,
		Value:     bb,
		ExpiresAt: uint64(expiryTime.Unix()),
	}

	return db.Update(func(txn *badger.Txn) error {
		// Write the entries
		err = txn.SetEntry(entry)
		if err != nil {
			// Most likely primary key conflict, but let the caller check this
			return err
		}

		return nil
	})
}

func getTraceFromDB(db *badger.DB, traceID model.TraceID) (*model.Trace, error) {
	traces, err := getTracesFromDB(db, []model.TraceID{traceID})
	if err != nil {
		return nil, err
	}
	if len(traces) == 1 {
		return traces[0], nil
	}

	return nil, nil
}

func getTracesFromDB(db *badger.DB, traceIDs []model.TraceID) ([]*model.Trace, error) {
	// Get by PK
	traces := make([]*model.Trace, 0, len(traceIDs))
	prefixes := make([][]byte, 0, len(traceIDs))

	for _, traceID := range traceIDs {
		prefixes = append(prefixes, createPrimaryKeySeekPrefix(traceID))
	}

	err := db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		it := txn.NewIterator(opts)
		defer it.Close()

		val := []byte{}
		for _, prefix := range prefixes {
			spans := make([]*model.Span, 0, 32) // reduce reallocation requirements by defining some initial length

			for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
				// Add value to the span store (decode from JSON / defined encoding first)
				// These are in the correct order because of the sorted nature
				item := it.Item()
				val, err := item.ValueCopy(val)
				if err != nil {
					return err
				}

				sp, err := decodeValue(val)
				if err != nil {
					return err
				}
				spans = append(spans, sp)
			}
			if len(spans) > 0 {
				trace := &model.Trace{
					Spans: spans,
				}
				traces = append(traces, trace)
			}
		}
		return nil
	})

	return traces, err
}

func createPrimaryKeySeekPrefix(traceID model.TraceID) []byte {
	key := make([]byte, sizeOfTraceID)
	pos := 0
	binary.BigEndian.PutUint64(key[pos:], traceID.High)
	pos += 8
	binary.BigEndian.PutUint64(key[pos:], traceID.Low)

	return key
}

func decodeValue(val []byte) (*model.Span, error) {
	sp := model.Span{}
	if err := proto.Unmarshal(val, &sp); err != nil {
		return nil, err
	}
	return &sp, nil
}
