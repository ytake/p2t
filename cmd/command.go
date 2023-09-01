package cmd

import "github.com/urfave/cli/v2"

// Executor is an interface for executing commands.
type Executor interface {
	// Run executes the cmd.
	Run(context *cli.Context) error
}
