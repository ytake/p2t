package snowflake

import (
	"fmt"
	"strings"

	"github.com/ytake/p2t/value"
)

type NameWithTypeCast struct{}

func NewCreateViewFromStaging(cd []value.ColumnDefinition) *CreateView {
	return &CreateView{
		columns: cd,
	}
}

func (c *CreateView) indentMetaData() []string {
	var md []string
	for _, v := range metaDataName {
		md = append(
			md,
			fmt.Sprintf(
				"    %s AS %s",
				fmt.Sprintf("%s%s", metadataPrefix, v), v))
	}
	return md
}

func (c *CreateView) Generate() string {
	var rows []string
	for _, v := range c.columns {
		col := Column{Name: v.Name, Type: v.Type.String()}
		rows = append(rows, NameWithTypeCast{}.Indent(col))
	}
	return c.createSQL(rows)
}

func (c *CreateView) createSQL(rows []string) string {
	sql := `SELECT
%s
FROM REPLACE.ME;`
	return fmt.Sprintf(sql, strings.Join(append(rows, c.indentMetaData()...), ",\n"))
}

// Name is a method for getting fully qualified name.
func (n NameWithTypeCast) Name(c Column) string {
	pn := []string{fmt.Sprintf("%s:%s", parquetPrefix, c.Name)}
	return strings.Join(append(pn, c.DetectTypeName().ColumnCast()...), "::")
}

// Indent is a method for getting indent column name.
func (n NameWithTypeCast) Indent(c Column) string {
	return fmt.Sprintf("    %s AS %s", n.Name(c), c.UpperName())
}
