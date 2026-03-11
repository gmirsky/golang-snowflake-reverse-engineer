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
	defaultMaxConnections = 3
	minimumMaxConnections = 1
	maximumMaxConnections = 9
)

type Config struct {
	User                               string
	Account                            string
	Warehouse                          string
	Database                           string
	OutputDir                          string
	LogDir                             string
	PrivateKeyPath                     string
	MaxConnections                     int
	Passphrase                         string
	CompactPackages                    bool
	CompactPackagesMaxRuntimes         int
	CompactPackagesOmitTruncationCount bool
	TimestampedOutput                  bool
	Verbose                            bool
	RunTimestamp                       string
}

func Parse(args []string) (Config, error) {
	fs := flag.NewFlagSet("snowflake-reverse-engineer", flag.ContinueOnError)
	fs.Usage = func() {}

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

	cfg.RunTimestamp = time.Now().UTC().Format("20060102T150405Z")
	return cfg, nil
}

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

	if c.MaxConnections < minimumMaxConnections || c.MaxConnections > maximumMaxConnections {
		return fmt.Errorf("max-connections must be between %d and %d", minimumMaxConnections, maximumMaxConnections)
	}

	if c.CompactPackagesMaxRuntimes < 0 {
		return errors.New("compact-packages-max-runtimes must be 0 or greater")
	}

	for _, dir := range []string{c.OutputDir, c.LogDir} {
		cleanDir := filepath.Clean(dir)
		if cleanDir == "." || cleanDir == string(filepath.Separator) {
			return errors.New("output and log directories must be explicit paths")
		}
	}

	return nil
}

func (c Config) LogFileName() string {
	return withTimestamp("snowflake-reverse-engineer.log", c.TimestampedOutput, c.RunTimestamp)
}

func (c Config) OutputFileName(viewName string) string {
	base := strings.ToLower(viewName) + ".sql"
	return withTimestamp(base, c.TimestampedOutput, c.RunTimestamp)
}

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

func withTimestamp(fileName string, enabled bool, timestamp string) string {
	if !enabled {
		return fileName
	}

	ext := filepath.Ext(fileName)
	name := strings.TrimSuffix(fileName, ext)
	return fmt.Sprintf("%s_%s%s", name, timestamp, ext)
}
