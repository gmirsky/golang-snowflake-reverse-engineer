// Package config defines the CLI flag set and runtime configuration for
// the reverse-engineering tool.
package config

import (
	"errors"
	"flag"
	"fmt"
	"path/filepath"
	"strings"
	"time"
)

const (
	defaultMaxConnections = 3 // used when --max-connections is not provided
	minimumMaxConnections = 1 // lower bound enforced by Validate
	maximumMaxConnections = 9 // upper bound enforced by Validate
)

// Config holds all runtime parameters derived from CLI flags.
type Config struct {
	User                               string // Snowflake user name
	Account                            string // Snowflake account identifier
	Warehouse                          string // Snowflake virtual warehouse
	Database                           string // target database to reverse-engineer
	OutputDir                          string // directory for generated .sql files
	LogDir                             string // directory for the run log
	PrivateKeyPath                     string // path to the RSA private key file
	MaxConnections                     int    // concurrent Snowflake connections
	Passphrase                         string // private-key passphrase (may be empty)
	CompactPackages                    bool   // group PACKAGES rows into compact lines
	CompactPackagesMaxRuntimes         int    // max runtimes per compact group (0 = unlimited)
	CompactPackagesOmitTruncationCount bool   // suppress "(truncated, N more)" suffix
	TimestampedOutput                  bool   // append run timestamp to output file names
	Verbose                            bool   // emit extra diagnostic log lines
	RunTimestamp                       string // UTC timestamp set at parse time
}

// Parse parses args (typically os.Args[1:]) into a validated Config.
// Returns an error for unknown flags or constraint violations.
func Parse(args []string) (Config, error) {
	fs := flag.NewFlagSet("snowflake-reverse-engineer", flag.ContinueOnError)
	fs.Usage = func() {} // suppress the default usage output on parse error

	var cfg Config
	fs.StringVar(&cfg.User, "user", "", "Snowflake user name")
	fs.StringVar(&cfg.Account, "account", "", "Snowflake account identifier")
	fs.StringVar(&cfg.Warehouse, "warehouse", "", "Snowflake warehouse name")
	fs.StringVar(&cfg.Database, "database", "", "Snowflake database name")
	fs.StringVar(&cfg.OutputDir, "output-dir", "", "Directory path for SQL output files")
	fs.StringVar(&cfg.LogDir, "log-dir", "", "Directory path for log output")
	fs.StringVar(&cfg.PrivateKeyPath, "private-key", "", "Private key file path")
	fs.IntVar(&cfg.MaxConnections, "max-connections", defaultMaxConnections, "Maximum Snowflake connections")
	fs.StringVar(&cfg.Passphrase, "passphrase", "", "Passphrase for the private key file")
	fs.BoolVar(&cfg.CompactPackages, "compact-packages", false, "Group INFORMATION_SCHEMA.PACKAGES rows by package/language/version")
	fs.IntVar(&cfg.CompactPackagesMaxRuntimes, "compact-packages-max-runtimes", 0, "Optional cap for runtimes shown per compact package group (0 means unlimited)")
	fs.BoolVar(&cfg.CompactPackagesOmitTruncationCount, "compact-packages-omit-truncation-count", true, "Omit '(truncated, N more)' suffix in compact package output")
	fs.BoolVar(&cfg.TimestampedOutput, "timestamped-output", false, "Append a timestamp to generated file names")
	fs.BoolVar(&cfg.Verbose, "verbose", false, "Enable verbose diagnostic logging")

	if err := fs.Parse(args); err != nil {
		return Config{}, err
	}

	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}

	// Record a single timestamp shared by all output file names in this run.
	cfg.RunTimestamp = time.Now().UTC().Format("20060102T150405Z")
	return cfg, nil
}

// Validate checks that all required flags are present and that numeric
// constraints are satisfied. It does not verify whether paths exist on disk.
func (c Config) Validate() error {
	required := map[string]string{
		"user":        c.User,
		"account":     c.Account,
		"warehouse":   c.Warehouse,
		"database":    c.Database,
		"output-dir":  c.OutputDir,
		"log-dir":     c.LogDir,
		"private-key": c.PrivateKeyPath,
	}

	missing := make([]string, 0)
	for key, value := range required {
		if strings.TrimSpace(value) == "" {
			missing = append(missing, key)
		}
	}
	if len(missing) > 0 {
		return fmt.Errorf("missing required flags: %s", strings.Join(missing, ", "))
	}

	// Enforce the bounded connection pool range.
	if c.MaxConnections < minimumMaxConnections || c.MaxConnections > maximumMaxConnections {
		return fmt.Errorf("max-connections must be between %d and %d", minimumMaxConnections, maximumMaxConnections)
	}

	if c.CompactPackagesMaxRuntimes < 0 {
		return errors.New("compact-packages-max-runtimes must be 0 or greater")
	}

	// Reject degenerate paths such as "." or "/" to prevent accidental writes.
	for _, dir := range []string{c.OutputDir, c.LogDir} {
		cleanDir := filepath.Clean(dir)
		if cleanDir == "." || cleanDir == string(filepath.Separator) {
			return errors.New("output and log directories must be explicit paths")
		}
	}

	return nil
}

// LogFileName returns the log file base name, optionally suffixed with the
// run timestamp when TimestampedOutput is enabled.
func (c Config) LogFileName() string {
	return withTimestamp("snowflake-reverse-engineer.log", c.TimestampedOutput, c.RunTimestamp)
}

// OutputFileName converts a view name into a lowercase .sql file name,
// optionally suffixed with the run timestamp.
func (c Config) OutputFileName(viewName string) string {
	base := strings.ToLower(viewName) + ".sql"
	return withTimestamp(base, c.TimestampedOutput, c.RunTimestamp)
}

// RedactedParameters returns every config field as a string map suitable for
// logging; the passphrase is replaced with "***" when non-empty.
func (c Config) RedactedParameters() map[string]string {
	passphrase := "null"
	if c.Passphrase != "" {
		passphrase = "***"
	}

	return map[string]string{
		"user":                               c.User,
		"account":                            c.Account,
		"warehouse":                          c.Warehouse,
		"database":                           c.Database,
		"outputDir":                          filepath.Clean(c.OutputDir),
		"logDir":                             filepath.Clean(c.LogDir),
		"privateKeyPath":                     filepath.Clean(c.PrivateKeyPath),
		"maxConnections":                     fmt.Sprintf("%d", c.MaxConnections),
		"passphrase":                         passphrase,
		"compactPackages":                    fmt.Sprintf("%t", c.CompactPackages),
		"compactPackagesMaxRuntimes":         fmt.Sprintf("%d", c.CompactPackagesMaxRuntimes),
		"compactPackagesOmitTruncationCount": fmt.Sprintf("%t", c.CompactPackagesOmitTruncationCount),
		"timestampedOutput":                  fmt.Sprintf("%t", c.TimestampedOutput),
		"verbose":                            fmt.Sprintf("%t", c.Verbose),
		"runTimestamp":                       c.RunTimestamp,
		"informationSchema":                  "INFORMATION_SCHEMA",
	}
}

// withTimestamp appends "_<timestamp>" before the file extension when enabled
// is true; otherwise it returns fileName unchanged.
func withTimestamp(fileName string, enabled bool, timestamp string) string {
	if !enabled {
		return fileName
	}

	ext := filepath.Ext(fileName)
	name := strings.TrimSuffix(fileName, ext)
	return fmt.Sprintf("%s_%s%s", name, timestamp, ext)
}
