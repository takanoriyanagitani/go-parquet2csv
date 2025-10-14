package parquet2csv

import (
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

type releasableRecord struct {
	arrow.RecordBatch
	released bool
}

func (r *releasableRecord) Release() {
	r.released = true
	r.RecordBatch.Release()
}

func newTestRecords(t *testing.T, rowCount int) (Records, *releasableRecord) {
	t.Helper()

	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "int_field", Type: arrow.PrimitiveTypes.Int64},
		},
		nil,
	)

	pool := memory.NewGoAllocator()
	b := array.NewRecordBuilder(pool, schema)
	defer b.Release()

	for i := 0; i < rowCount; i++ {
		b.Field(0).(*array.Int64Builder).Append(int64(i))
	}

	rec := &releasableRecord{RecordBatch: b.NewRecord()}

	return func(yield func(arrow.RecordBatch, error) bool) {
		yield(rec, nil)
	}, rec
}

func TestDiscardCsvWriter(t *testing.T) {
	t.Run("without release", func(t *testing.T) {
		records, rec := newTestRecords(t, 10)
		defer rec.Release()

		writer := DiscardCsvWriter()
		err := writer(context.Background(), records)
		if err != nil {
			t.Fatal(err)
		}

		if rec.released {
			t.Fatal("expected record to not be released")
		}
	})

	t.Run("with release", func(t *testing.T) {
		records, rec := newTestRecords(t, 10)

		writer := DiscardCsvWriter(WithReleaseRecords(true))
		err := writer(context.Background(), records)
		if err != nil {
			t.Fatal(err)
		}

		if !rec.released {
			t.Fatal("expected record to be released")
		}
	})
}

func TestCountCsvWriter(t *testing.T) {
	const rowCount = 10

	t.Run("without release", func(t *testing.T) {
		records, rec := newTestRecords(t, rowCount)
		defer rec.Release()

		var count int
		writer := CountCsvWriter(&count)
		err := writer(context.Background(), records)
		if err != nil {
			t.Fatal(err)
		}

		if count != rowCount {
			t.Fatalf("expected count to be %d, but got %d", rowCount, count)
		}

		if rec.released {
			t.Fatal("expected record to not be released")
		}
	})

	t.Run("with release", func(t *testing.T) {
		records, rec := newTestRecords(t, rowCount)

		var count int
		writer := CountCsvWriter(&count, WithReleaseRecords(true))
		err := writer(context.Background(), records)
		if err != nil {
			t.Fatal(err)
		}

		if count != rowCount {
			t.Fatalf("expected count to be %d, but got %d", rowCount, count)
		}

		if !rec.released {
			t.Fatal("expected record to be released")
		}
	})
}
