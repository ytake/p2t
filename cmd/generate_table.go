package cmd

import (
	"bytes"
	"os"

	"github.com/urfave/cli/v2"
	"github.com/ytake/p2t/convert"
	"github.com/ytake/p2t/reader"
	"github.com/ytake/p2t/snowflake"
	"github.com/ytake/p2t/value"
)

// GenerateTable is a cmd for generating table.
type GenerateTable struct {
}

// Run executes the cmd.
func (g GenerateTable) Run(context *cli.Context) error {
	p, err := reader.Parquet{}.Open(context.String("file"))
	if err != nil {
		return err
	}
	buf := bytes.NewBuffer(
		convert.To(snowflake.NewDDL(p.Schema(), value.DDLType(
			context.String("type")))))
	if _, err := buf.WriteTo(os.Stdout); err != nil {
		return err
	}
	return nil
}
