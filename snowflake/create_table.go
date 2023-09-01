package snowflake

import (
	"fmt"
	"strings"

	"github.com/ytake/p2t/value"
)

type NameWithType struct{}

// Name is a with type method for getting fully qualified name.
func (n NameWithType) Name(c Column) string {
	return fmt.Sprintf("%s %s", c.UpperName(), c.DetectTypeName().String())
}

// Indent is a with type method for getting indent column name.
func (n NameWithType) Indent(c Column) string {
	return fmt.Sprintf("    %s", n.Name(c))
}

// NewCreateTable is a constructor for CreateTable.
func NewCreateTable(cd []value.ColumnDefinition) *CreateTable {
	return &CreateTable{
		columns: cd,
	}
}

// Generate is a method for generating create table sql.
func (c *CreateTable) Generate() string {
	var columns []string
	for _, v := range c.columns {
		columns = append(columns, NameWithType{}.Indent(Column{Name: v.Name, Type: v.Type.String()}))
	}
	return c.createSQL(columns)
}

func (c *CreateTable) createSQL(cols []string) string {
	sql := `CREATE OR REPLACE TABLE REPLACE.ME (
%s
) COMMENT = 'replace me';`
	return fmt.Sprintf(sql, strings.Join(cols, ",\n"))
}
