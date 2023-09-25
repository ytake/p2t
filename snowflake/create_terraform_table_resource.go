package snowflake

import (
	"fmt"
	"strings"

	"github.com/ytake/p2t/value"
)

type CreateTableResource struct {
	columns []value.ColumnDefinition
}

func NewCreateTerraformTableResource(cd []value.ColumnDefinition) *CreateTableResource {
	return &CreateTableResource{
		columns: cd,
	}
}

type TableResourceColumn struct{}
type TableResourceColumnNoTypeCast struct{}

// Name is a no type cast method for getting fully qualified name.
func (n TableResourceColumnNoTypeCast) Name(c Column) string {
	row := `
  column {
    name = "%s"
    type = "%s"
  }`
	return fmt.Sprintf(row, c.UpperName(), c.Type)
}

// Indent is a no type cast method for getting indent column name.
func (n TableResourceColumnNoTypeCast) Indent(c Column) string {
	return n.Name(c)
}

// Name is a with type method for getting fully qualified name.
func (n TableResourceColumn) Name(c Column) string {
	var row string
	casts := c.DetectTypeName().ColumnCast()
	if c.IsNullable() {
		row = `
  column {
    name = "%s"
    type = "%s"
    nullable = %t
  }`
		return fmt.Sprintf(row, c.UpperName(), casts[len(casts)-1], c.IsNullable())
	}
	row = `
  column {
    name = "%s"
    type = "%s"
  }`
	return fmt.Sprintf(row, c.UpperName(), casts[len(casts)-1])
}

// Indent is a with type method for getting indent column name.
func (n TableResourceColumn) Indent(c Column) string {
	return n.Name(c)
}

// Generate is a method for generating create terraform snowflake table resource.
func (c *CreateTableResource) Generate() string {
	var columns []string
	for _, v := range c.columns {
		columns = append(columns, TableResourceColumn{}.Indent(Column{Name: v.Name, Type: v.Type.String()}))
	}
	return c.createResource(columns)
}

func (c *CreateTableResource) metaDataColumns() []string {
	var columns []string
	for _, v := range metaDataName {
		t, x := metaDataType[v]
		if x {
			columns = append(columns,
				TableResourceColumnNoTypeCast{}.Indent(Column{Name: v, Type: t}))
		}
	}
	return columns
}

func (c *CreateTableResource) createResource(cols []string) string {
	sql := `resource "snowflake_table" "replace_me" {
  database            = snowflake_schema.schema.database
  schema              = snowflake_schema.schema.name
  name                = "replace me"
  comment             = "A table."
%s
}`
	return fmt.Sprintf(sql, strings.Join(append(cols, c.metaDataColumns()...), "\n"))
}
