// Package main provides the CLI entrypoint for the reverse-engineering tool.
package main

import (
	"fmt"
	"os"

	"github.com/gmirsky/golang-snowflake-reverse-engineer/internal/app"
	appconfig "github.com/gmirsky/golang-snowflake-reverse-engineer/internal/config"
)

var parseConfig = appconfig.Parse
var runApp = app.Run

// run: Given CLI args, when execution starts, then the full pipeline is driven
// and an exit code is returned: 0 for success, 2 for flag parse errors, 1 for
// runtime failures.
func run(args []string) int {
	cfg, err := parseConfig(args)
	if err != nil {
		// CLI parse failures are user-input errors; exit code 2 is conventional.
		fmt.Fprintln(os.Stderr, err)
		return 2
	}

	// Runtime failures indicate processing/connectivity problems, not usage issues.
	if err := runApp(cfg); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}

	return 0
}
