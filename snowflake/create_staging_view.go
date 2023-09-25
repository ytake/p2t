package snowflake

import (
	"fmt"
	"strings"

	"github.com/ytake/p2t/value"
)

type NameWithTypeCast struct{}

var metaData = []string{
	fmt.Sprintf("    %s", "METADATA$FILE_LAST_MODIFIED AS FILE_LOAD_DATE"),
	fmt.Sprintf("    %s", "METADATA$FILENAME AS FILE_NAME"),
	fmt.Sprintf("    %s", "METADATA$FILE_ROW_NUMBER AS FILE_ROW_NUMBER"),
}

func NewCreateViewFromStaging(cd []value.ColumnDefinition) *CreateView {
	return &CreateView{
		columns: cd,
	}
}

func (c *CreateView) Generate() string {
	var rows []string
	var columns []string
	for _, v := range c.columns {
		col := Column{Name: v.Name, Type: v.Type.String()}
		columns = append(columns, col.Indent())
		rows = append(rows, NameWithTypeCast{}.Indent(col))
	}
	return c.createSQL(columns, rows)
}

func (c *CreateView) createSQL(cols, rows []string) string {
	sql := `SELECT
%s
FROM REPLACE.ME;`
	return fmt.Sprintf(sql, strings.Join(append(rows, metaData...), ",\n"))
}

// Name is a method for getting fully qualified name.
func (n NameWithTypeCast) Name(c Column) string {
	pn := []string{fmt.Sprintf("%s:%s", ParquetPrefix, c.Name)}
	return strings.Join(append(pn, c.DetectTypeName().ColumnCast()...), "::")
}

// Indent is a method for getting indent column name.
func (n NameWithTypeCast) Indent(c Column) string {
	return fmt.Sprintf("    %s AS %s", n.Name(c), c.UpperName())
}
