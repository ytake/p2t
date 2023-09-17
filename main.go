package main

import (
	"log"
	"os"

	"github.com/urfave/cli/v2"
	"github.com/ytake/p2t/cmd"
	"github.com/ytake/p2t/value"
)

func main() {
	app := &cli.App{
		Commands: []*cli.Command{
			{
				Name:    "snowflake",
				Aliases: []string{"sf"},
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name: "file", Required: true, Usage: "input file", Aliases: []string{"f"}},
					&cli.StringFlag{
						Name:     "type",
						Required: true,
						Usage:    "type of ddl. choose from 'table' or 'view'",
						Aliases:  []string{"t"},
						Action: func(context *cli.Context, s string) error {
							if value.DDLTypeFromString(s) == value.NoDDLType {
								return cli.Exit("invalid type", 1)
							}
							return nil
						}},
				},
				Action: cmd.GenerateTable{}.Run,
			},
		},
	}
	app.Name = "p2t"
	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}
