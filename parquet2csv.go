package parquet2csv

import (
	"context"
	"errors"
	"io"
	"iter"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	am "github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	pf "github.com/apache/arrow-go/v18/parquet/file"
	pa "github.com/apache/arrow-go/v18/parquet/pqarrow"
)

type ParquetReader struct {
	*pa.FileReader
	io.Closer
}

func (p ParquetReader) ToRecordReaderDefault(
	ctx context.Context,
) (RecordReader, error) {
	rr, e := p.FileReader.GetRecordReader(ctx, nil, nil)
	return RecordReader{RecordReader: rr}, e
}

func (p ParquetReader) ToIterDefault(
	ctx context.Context,
) iter.Seq2[arrow.RecordBatch, error] {
	rdr, err := p.ToRecordReaderDefault(ctx)
	if nil == err {
		return rdr.ToIter()
	}

	return func(yield func(arrow.RecordBatch, error) bool) {
		yield(nil, err)
	}
}

type RecordReader struct{ pa.RecordReader }

func (r RecordReader) AsArrayRecordReader() array.RecordReader {
	return r.RecordReader
}

func (r RecordReader) ToIter() iter.Seq2[arrow.RecordBatch, error] {
	return array.IterFromReader(r.AsArrayRecordReader())
}

type ParquetReadOpts struct {
	pa.ArrowReadProperties

	FileReadOptions []pf.ReadOption
}

const DefaultBatchSize = 1024

func NewParquetReadOpts() ParquetReadOpts {
	return ParquetReadOpts{
		ArrowReadProperties: pa.ArrowReadProperties{
			BatchSize: DefaultBatchSize,
			Parallel:  false,
		},
		FileReadOptions: nil,
	}
}

func (o ParquetReadOpts) WithBatchSize(batchSize int) ParquetReadOpts {
	o.BatchSize = int64(batchSize)
	return o
}

func (o ParquetReadOpts) WithParallel(parallel bool) ParquetReadOpts {
	o.Parallel = parallel
	return o
}

func (o ParquetReadOpts) WithForceLarge(colIdx int, forceLarge bool) ParquetReadOpts {
	o.SetForceLarge(colIdx, forceLarge)
	return o
}

func (o ParquetReadOpts) WithReadDict(colIdx int, readDict bool) ParquetReadOpts {
	o.SetReadDict(colIdx, readDict)
	return o
}

// Creates ParquetReader from the ReaderAtSeeker and the allocator.
//
// Note: if the ReaderAtSeeker is an io.Closer, it'll be closed too
// when the ParquetReader is closed.
func (o ParquetReadOpts) ToReader(
	ras parquet.ReaderAtSeeker,
	mem am.Allocator,
) (ParquetReader, error) {
	pfrdr, err := pf.NewParquetReader(
		ras,
		o.FileReadOptions...,
	)
	if nil != err {
		return ParquetReader{}, err
	}

	frdr, err := pa.NewFileReader(pfrdr, o.ArrowReadProperties, mem)
	if nil != err {
		return ParquetReader{}, errors.Join(err, pfrdr.Close())
	}
	return ParquetReader{
		FileReader: frdr,
		Closer:     pfrdr,
	}, err
}

// Creates ParquetReader from the ReaderAtSeeker using default opts.
func (o ParquetReadOpts) ToReaderDefault(
	ras parquet.ReaderAtSeeker,
) (ParquetReader, error) {
	return o.ToReader(ras, am.DefaultAllocator)
}

type File interface {
	io.ReaderAt
	io.Seeker
	io.Closer
}

func (o ParquetReadOpts) FileToReaderDefault(
	f File,
) (ParquetReader, error) {
	return o.ToReaderDefault(f)
}

func (o ParquetReadOpts) ParquetFileToCsvDefault(
	ctx context.Context,
	parquetFile File,
	cwtr CsvWriter,
) error {
	rdr, err := o.FileToReaderDefault(parquetFile)
	if nil != err {
		return errors.Join(err, parquetFile.Close())
	}

	var rows Records = rdr.ToIterDefault(ctx)
	return errors.Join(
		cwtr(ctx, rows),
		rdr.Close(),
	)
}

type Records = iter.Seq2[arrow.RecordBatch, error]

type CsvWriter func(context.Context, Records) error

func ParquetFileToCsv(ctx context.Context, parquetFile File, cwtr CsvWriter) error {
	opts := NewParquetReadOpts()
	return opts.ParquetFileToCsvDefault(ctx, parquetFile, cwtr)
}
