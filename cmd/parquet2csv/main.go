package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	"github.com/apache/arrow-go/v18/arrow/csv"
	pc "github.com/takanoriyanagitani/go-parquet2csv"
)

type Options struct {
	pc.ParquetReadOpts

	// Write options from flags
	crlf    bool
	comma   string
	header  bool
	nullStr string
}

func (o *Options) ParquetFileToReader(f *os.File) (pc.ParquetReader, error) {
	return o.ParquetReadOpts.FileToReaderDefault(f)
}

func (o *Options) ParquetFileToRowsToCsvToStdout(
	ctx context.Context,
	pfile *os.File,
) (err error) {
	rdr, err := o.ParquetFileToReader(pfile)
	if nil != err {
		return err
	}
	defer func() {
		closeErr := rdr.Close()
		if nil == err {
			err = closeErr
		}
	}()

	schema, err := rdr.Schema()
	if nil != err {
		return err
	}

	commaRune := ','
	if len(o.comma) > 0 {
		commaRune = []rune(o.comma)[0]
	}

	writer := csv.NewWriter(os.Stdout, schema,
		csv.WithHeader(o.header),
		csv.WithComma(commaRune),
		csv.WithNullWriter(o.nullStr),
		csv.WithCRLF(o.crlf),
	)
	defer func() {
		flushErr := writer.Flush()
		if nil == err {
			err = flushErr
		}
	}()

	recReader, err := rdr.GetRecordReader(ctx, nil, nil)
	if nil != err {
		return err
	}

	for recReader.Next() {
		rec := recReader.Record()
		writeErr := writer.Write(rec)
		if nil != writeErr {
			return writeErr
		}
	}

	return recReader.Err()
}

func (o *Options) Convert(
	ctx context.Context,
	parquetFilename string,
) error {
	pfile, err := os.Open(parquetFilename) //nolint:gosec
	if nil != err {
		return err
	}
	defer func() {
		closeErr := pfile.Close()
		if nil == err {
			err = closeErr
		}
	}()
	return o.ParquetFileToRowsToCsvToStdout(ctx, pfile)
}

func main() {
	var opts Options

	var batchSize int
	flag.IntVar(&batchSize, "batch-size", pc.DefaultBatchSize, "batch size for reading parquet files")
	flag.BoolVar(&opts.Parallel, "parallel", false, "read parquet files in parallel")

	flag.BoolVar(&opts.crlf, "crlf", false, "use CRLF as line terminator")
	flag.StringVar(&opts.comma, "comma", ",", "field delimiter")
	flag.BoolVar(&opts.header, "header", true, "write header row")
	flag.StringVar(&opts.nullStr, "null", "", "string representation of null values")

	flag.Parse()

	if flag.NArg() < 1 {
		fmt.Fprintln(os.Stderr, "missing parquet filename")
		os.Exit(1)
	}

	opts.ParquetReadOpts = pc.NewParquetReadOpts().
		WithBatchSize(batchSize).
		WithParallel(opts.Parallel)

	for _, filename := range flag.Args() {
		err := opts.Convert(context.Background(), filename)
		if nil != err {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}
}
