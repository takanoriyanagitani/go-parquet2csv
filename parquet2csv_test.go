package parquet2csv

import (
	"bytes"
	"context"
	"errors"
	"os"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/csv"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

func createTestParquetData(t *testing.T) []byte {
	t.Helper()

	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "int_field", Type: arrow.PrimitiveTypes.Int64},
			{Name: "string_field", Type: arrow.BinaryTypes.String},
		},
		nil,
	)

	pool := memory.NewGoAllocator()
	b := array.NewRecordBuilder(pool, schema)
	defer b.Release()

	b.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 3}, nil)
	b.Field(1).(*array.StringBuilder).AppendValues([]string{"a", "b", "c"}, nil)

	rec := b.NewRecord()
	defer rec.Release()

	buf := new(bytes.Buffer)
	w, err := pqarrow.NewFileWriter(schema, buf, nil, pqarrow.NewArrowWriterProperties())
	if err != nil {
		t.Fatal(err)
	}

	err = w.Write(rec)
	if err != nil {
		t.Fatal(err)
	}

	err = w.Close()
	if err != nil {
		t.Fatal(err)
	}

	return buf.Bytes()
}

func createTestParquetFile(t *testing.T) string {
	t.Helper()

	data := createTestParquetData(t)
	tmpfile, err := os.CreateTemp("", "test.parquet")
	if err != nil {
		t.Fatal(err)
	}
	defer tmpfile.Close()

	if _, err := tmpfile.Write(data); err != nil {
		t.Fatal(err)
	}

	return tmpfile.Name()
}

func TestParquetFileToCsvDefault(t *testing.T) {
	parquetFile := createTestParquetFile(t)
	defer os.Remove(parquetFile)

	file, err := os.Open(parquetFile)
	if err != nil {
		t.Fatal(err)
	}

	var buf bytes.Buffer
	csvWriter := func(ctx context.Context, records Records) error {
		var w *csv.Writer
		for rec, err := range records {
			if err != nil {
				return err
			}
			if w == nil {
				w = csv.NewWriter(&buf, rec.Schema(), csv.WithHeader(true))
				defer w.Flush()
			}
			if err := w.Write(rec); err != nil {
				return err
			}
		}
		return nil
	}

	var opts ParquetReadOpts
	err = opts.ParquetFileToCsvDefault(context.Background(), file, csvWriter)
	if err != nil {
		t.Fatal(err)
	}

	expected := `int_field,string_field
1,a
2,b
3,c
`
	if buf.String() != expected {
		t.Fatalf("expected:\n%s\ngot:\n%s", expected, buf.String())
	}
}

type closableBuffer struct {
	*bytes.Reader
	closeCount int
}

func (cb *closableBuffer) Close() error {
	cb.closeCount++
	return nil
}

func TestParquetFileToCsvDefaultClose(t *testing.T) {
	data := createTestParquetData(t)
	closable := &closableBuffer{Reader: bytes.NewReader(data)}

	var opts ParquetReadOpts
	csvWriter := DiscardCsvWriter()
	err := opts.ParquetFileToCsvDefault(context.Background(), closable, csvWriter)
	if err != nil {
		t.Fatal(err)
	}

	if closable.closeCount != 1 {
		t.Fatalf("expected close to be called once, but got %d", closable.closeCount)
	}
}

func TestParquetFileToCsvDefaultError(t *testing.T) {
	data := createTestParquetData(t)
	closable := &closableBuffer{Reader: bytes.NewReader(data)}

	var opts ParquetReadOpts
	wantErr := errors.New("test error")
	csvWriter := func(ctx context.Context, records Records) error {
		return wantErr
	}
	err := opts.ParquetFileToCsvDefault(context.Background(), closable, csvWriter)
	if !errors.Is(err, wantErr) {
		t.Fatalf("expected error %v, but got %v", wantErr, err)
	}

	if closable.closeCount != 1 {
		t.Fatalf("expected close to be called once, but got %d", closable.closeCount)
	}
}

func TestParquetReadOpts_WithBatchSize(t *testing.T) {
	opts := ParquetReadOpts{}
	newOpts := opts.WithBatchSize(123)

	if newOpts.BatchSize != 123 {
		t.Fatalf("expected batch size to be 123, but got %d", newOpts.BatchSize)
	}
}

func TestParquetReadOpts_WithParallel(t *testing.T) {
	opts := ParquetReadOpts{}
	newOpts := opts.WithParallel(true)

	if !newOpts.Parallel {
		t.Fatal("expected parallel to be true")
	}
}

func TestParquetReadOpts_WithForceLarge(t *testing.T) {
	opts := ParquetReadOpts{}
	newOpts := opts.WithForceLarge(0, true)

	if !newOpts.ForceLarge(0) {
		t.Fatal("expected force large to be true for column 0")
	}
}

func TestParquetReadOpts_WithReadDict(t *testing.T) {
	opts := ParquetReadOpts{}
	newOpts := opts.WithReadDict(0, true)

	if !newOpts.ReadDict(0) {
		t.Fatal("expected read dict to be true for column 0")
	}
}

func TestNewParquetReadOpts(t *testing.T) {
	opts := NewParquetReadOpts()

	if opts.BatchSize != 1024 {
		t.Fatalf("expected batch size to be 1024, but got %d", opts.BatchSize)
	}

	if opts.Parallel {
		t.Fatal("expected parallel to be false")
	}
}

func TestParquetFileToCsv(t *testing.T) {
	data := createTestParquetData(t)
	closable := &closableBuffer{Reader: bytes.NewReader(data)}

	var count int
	csvWriter := CountCsvWriter(&count)

	err := ParquetFileToCsv(context.Background(), closable, csvWriter)
	if err != nil {
		t.Fatal(err)
	}

	if count != 3 {
		t.Fatalf("expected count to be 3, but got %d", count)
	}
}
