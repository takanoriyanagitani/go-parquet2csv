package parquet2csv

import (
	"context"
)

type CsvWriterOption func(*csvWriterOptions)

type csvWriterOptions struct {
	releaseRecords bool
}

func WithReleaseRecords(release bool) CsvWriterOption {
	return func(o *csvWriterOptions) {
		o.releaseRecords = release
	}
}

// DiscardCsvWriter returns a CsvWriter that discards all records.
func DiscardCsvWriter(opts ...CsvWriterOption) CsvWriter {
	options := csvWriterOptions{
		releaseRecords: false,
	}
	for _, o := range opts {
		o(&options)
	}

	return func(ctx context.Context, records Records) error {
		for rec, err := range records {
			if err != nil {
				return err
			}
			if options.releaseRecords {
				rec.Release()
			}
		}
		return nil
	}
}

// CountCsvWriter returns a CsvWriter that counts the number of records.
func CountCsvWriter(count *int, opts ...CsvWriterOption) CsvWriter {
	options := csvWriterOptions{
		releaseRecords: false,
	}
	for _, o := range opts {
		o(&options)
	}

	return func(ctx context.Context, records Records) error {
		for rec, err := range records {
			if err != nil {
				return err
			}
			(*count) += int(rec.NumRows())
			if options.releaseRecords {
				rec.Release()
			}
		}
		return nil
	}
}
