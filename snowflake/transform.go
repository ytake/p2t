package snowflake

import (
	"github.com/parquet-go/parquet-go"
	"github.com/ytake/p2t/value"
)

// DDL is a struct for snowflake ddl.
type DDL struct {
	schema *parquet.Schema
	ddl    value.DDLType
}

// NewDDL is a constructor for SnowflakeTable.
func NewDDL(schema *parquet.Schema, ddl value.DDLType) *DDL {
	return &DDL{
		schema: schema,
		ddl:    ddl,
	}
}

// Transform is a method for transforming parquet schema to snowflake ddl.
func (t *DDL) Transform() string {
	var cd []value.ColumnDefinition
	for _, v := range t.schema.Fields() {
		cd = append(cd, value.ColumnDefinition{
			Name:     v.Name(),
			Required: v.Required(),
			Optional: v.Optional(),
			Type:     v.Type(),
		})
	}
	switch t.ddl {
	case value.Table:
		return NewCreateTable(cd).Generate()
	case value.View:
		return NewCreateViewFromStaging(cd).Generate()
	}
	return ""
}
