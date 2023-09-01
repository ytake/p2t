package value

import "github.com/parquet-go/parquet-go"

type DDLType string

const (
	Table = DDLType("table")
	View  = DDLType("view")
)

// ColumnDefinition is a struct for column definition.
type ColumnDefinition struct {
	Name     string
	Required bool
	Optional bool
	Type     parquet.Type
}
