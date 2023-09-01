package reader

import (
	"os"

	"github.com/parquet-go/parquet-go"
	"golang.org/x/xerrors"
)

type Parquet struct {
}

// Open opens a parquet file.
func (Parquet) Open(name string) (*parquet.File, error) {
	file, err := os.Open(name)
	if err != nil {
		return nil, xerrors.Errorf("file open error: %w", err)
	}
	fi, err := file.Stat()
	if err != nil {
		return nil, xerrors.Errorf("file stat error: %w", err)
	}
	p, err := parquet.OpenFile(file, fi.Size())
	if err != nil {
		return nil, xerrors.Errorf("parquet open error: %w", err)
	}
	return p, nil
}
