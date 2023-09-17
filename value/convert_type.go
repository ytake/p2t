package value

import "github.com/parquet-go/parquet-go"

type DDLType string

const (
	Table                  = DDLType("table")
	View                   = DDLType("view")
	TerraformTableResource = DDLType("tf")
	NoDDLType              = DDLType("")
)

// ColumnDefinition is a struct for column definition.
type ColumnDefinition struct {
	Name     string
	Required bool
	Optional bool
	Type     parquet.Type
}

// DDLTypeFromString is a method for getting DDLType from string.
func DDLTypeFromString(s string) DDLType {
	switch s {
	case "table":
		return Table
	case "view":
		return View
	case "tf":
		return TerraformTableResource
	default:
		return NoDDLType
	}
}
