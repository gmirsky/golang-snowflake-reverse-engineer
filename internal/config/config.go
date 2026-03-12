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

// Parse: Given CLI args, when parsing and validation run, then a normalized
// Config is returned or an error explains invalid input.
func Parse(args []string) (Config, error) {
	// ContinueOnError lets us return parse errors to callers instead of exiting.
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

	// Normalize user, account, warehouse, and database to uppercase for consistency.
	cfg.User = strings.ToUpper(cfg.User)
	cfg.Account = strings.ToUpper(cfg.Account)
	cfg.Warehouse = strings.ToUpper(cfg.Warehouse)
	cfg.Database = strings.ToUpper(cfg.Database)

	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}

	// Record a single timestamp shared by all output file names in this run.
	cfg.RunTimestamp = time.Now().UTC().Format("20060102T150405Z")
	return cfg, nil
}

// Validate: Given a Config, when constraints are checked, then missing flags
// or invalid numeric/path values are rejected.
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
		// Treat whitespace-only values as missing to avoid invalid downstream paths.
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

// LogFileName: Given timestamp settings, when a log filename is requested,
// then the base name is returned with optional timestamp suffix.
func (c Config) LogFileName() string {
	return withTimestamp("snowflake-reverse-engineer.log", c.TimestampedOutput, c.RunTimestamp)
}

// OutputFileName: Given a view name, when output naming runs, then a lowercase
// .sql filename is returned with optional timestamp suffix.
func (c Config) OutputFileName(viewName string) string {
	base := strings.ToLower(viewName) + ".sql"
	return withTimestamp(base, c.TimestampedOutput, c.RunTimestamp)
}

// RedactedParameters: Given runtime config values, when log-safe parameters are
// requested, then all fields are returned with secrets redacted.
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

// withTimestamp: Given a file name and toggle, when timestamping is enabled,
// then the timestamp is inserted before the extension.
func withTimestamp(fileName string, enabled bool, timestamp string) string {
	if !enabled {
		return fileName
	}

	ext := filepath.Ext(fileName)
	name := strings.TrimSuffix(fileName, ext)
	return fmt.Sprintf("%s_%s%s", name, timestamp, ext)
}
