// Package config provides configuration management for the MySQL to Dgraph pipeline.
// It supports YAML files, environment variable overrides, and provides sensible defaults.
package config

import (
	"fmt"
	"os"
	"strconv"
	"time"

	"gopkg.in/yaml.v2"
)

// Config represents the complete application configuration
type Config struct {
	MySQL    MySQLConfig    `yaml:"mysql"`    // MySQL database connection settings
	Dgraph   DgraphConfig   `yaml:"dgraph"`   // Dgraph database connection settings
	Pipeline PipelineConfig `yaml:"pipeline"` // Pipeline execution parameters
	Logger   LoggerConfig   `yaml:"logger"`   // Logging configuration
	Output   OutputConfig   `yaml:"output"`   // Output file configuration
}

// MySQLConfig contains MySQL database connection and performance settings
type MySQLConfig struct {
	Host            string        `yaml:"host"`               // MySQL server hostname
	Port            int           `yaml:"port"`               // MySQL server port
	User            string        `yaml:"user"`               // Database username
	Password        string        `yaml:"password"`           // Database password
	Database        string        `yaml:"database"`           // Target database name
	MaxConnections  int           `yaml:"max_connections"`    // Connection pool size
	ConnMaxLifetime time.Duration `yaml:"conn_max_lifetime"`  // Maximum connection lifetime
	ConnMaxIdleTime time.Duration `yaml:"conn_max_idle_time"` // Maximum connection idle time
	Timeout         time.Duration `yaml:"timeout"`            // Query timeout
}

// DgraphConfig contains Dgraph database connection and performance settings
type DgraphConfig struct {
	Alpha       []string      `yaml:"alpha"`       // Dgraph Alpha server endpoints
	Timeout     time.Duration `yaml:"timeout"`     // Request timeout
	BatchSize   int           `yaml:"batch_size"`  // Batch size for bulk operations
	MaxRetries  int           `yaml:"max_retries"` // Maximum retry attempts
	RetryDelay  time.Duration `yaml:"retry_delay"` // Delay between retry attempts
	Compression bool          `yaml:"compression"` // Enable gRPC compression
}

// PipelineConfig contains pipeline execution and performance settings
type PipelineConfig struct {
	Workers                int           `yaml:"workers"`                  // Number of parallel worker threads
	BatchSize              int           `yaml:"batch_size"`               // Records processed per batch
	MemoryLimit            int64         `yaml:"memory_limit_mb"`          // Memory limit in MB (0 = unlimited)
	DryRun                 bool          `yaml:"dry_run"`                  // Preview mode without writing data
	SkipValidation         bool          `yaml:"skip_validation"`          // Skip data validation step
	CheckpointInterval     int           `yaml:"checkpoint_interval"`      // Records between progress checkpoints
	ProgressReportInterval time.Duration `yaml:"progress_report_interval"` // Progress reporting frequency
	EnableMetrics          bool          `yaml:"enable_metrics"`           // Enable performance metrics
	MetricsPort            int           `yaml:"metrics_port"`             // Metrics server port
}

// LoggerConfig contains logging configuration
type LoggerConfig struct {
	Level  string `yaml:"level"`  // Log level: debug, info, warn, error
	Format string `yaml:"format"` // Log format: json, text
	Output string `yaml:"output"` // Log output: stdout, stderr, file
}

// OutputConfig contains output file paths and settings
type OutputConfig struct {
	Directory      string `yaml:"directory"`       // Output directory path
	RDFFile        string `yaml:"rdf_file"`        // RDF data file name
	SchemaFile     string `yaml:"schema_file"`     // Dgraph schema file name
	JSONFile       string `yaml:"json_file"`       // JSON export file name
	MappingFile    string `yaml:"mapping_file"`    // UID mapping file name
	CheckpointFile string `yaml:"checkpoint_file"` // Progress checkpoint file name
	BackupEnabled  bool   `yaml:"backup_enabled"`  // Enable output file backup
}

// DefaultConfig returns a configuration with sensible defaults for production use
func DefaultConfig() *Config {
	return &Config{
		MySQL: MySQLConfig{
			Host:            "localhost",
			Port:            3306,
			User:            "root",
			Password:        "root",
			Database:        "dump",
			MaxConnections:  10,
			ConnMaxLifetime: 5 * time.Minute,
			ConnMaxIdleTime: 2 * time.Minute,
			Timeout:         30 * time.Second,
		},
		Dgraph: DgraphConfig{
			Alpha:       []string{"localhost:9080"},
			Timeout:     30 * time.Second,
			BatchSize:   10000,
			MaxRetries:  3,
			RetryDelay:  time.Second,
			Compression: true,
		},
		Pipeline: PipelineConfig{
			Workers:                4,
			BatchSize:              1000,
			MemoryLimit:            1024, // 1GB
			DryRun:                 false,
			SkipValidation:         false,
			CheckpointInterval:     10000,
			ProgressReportInterval: 30 * time.Second,
			EnableMetrics:          true,
			MetricsPort:            8080,
		},
		Logger: LoggerConfig{
			Level:  "info",
			Format: "json",
			Output: "stdout",
		},
		Output: OutputConfig{
			Directory:      "output",
			RDFFile:        "data.rdf",
			SchemaFile:     "schema.txt",
			JSONFile:       "data.json",
			MappingFile:    "uid_mapping.json",
			CheckpointFile: "checkpoint.json",
			BackupEnabled:  true,
		},
	}
}

// Load reads configuration from file and applies environment variable overrides
func Load(configPath string) (*Config, error) {
	cfg := DefaultConfig()

	// Load from YAML file if it exists
	if _, err := os.Stat(configPath); err == nil {
		data, err := os.ReadFile(configPath)
		if err != nil {
			return nil, fmt.Errorf("failed to read config file: %w", err)
		}

		if err := yaml.Unmarshal(data, cfg); err != nil {
			return nil, fmt.Errorf("failed to parse config file: %w", err)
		}
	}

	// Apply environment variable overrides
	if err := overrideWithEnv(cfg); err != nil {
		return nil, fmt.Errorf("failed to override with environment variables: %w", err)
	}

	// Validate final configuration
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}

	return cfg, nil
}

// overrideWithEnv applies environment variable overrides to configuration
func overrideWithEnv(cfg *Config) error {
	// Map environment variables to configuration fields
	envOverrides := map[string]interface{}{
		"MYSQL_HOST":          &cfg.MySQL.Host,
		"MYSQL_PORT":          &cfg.MySQL.Port,
		"MYSQL_USER":          &cfg.MySQL.User,
		"MYSQL_PASSWORD":      &cfg.MySQL.Password,
		"MYSQL_DATABASE":      &cfg.MySQL.Database,
		"DGRAPH_ALPHA":        &cfg.Dgraph.Alpha,
		"PIPELINE_WORKERS":    &cfg.Pipeline.Workers,
		"PIPELINE_BATCH_SIZE": &cfg.Pipeline.BatchSize,
		"LOG_LEVEL":           &cfg.Logger.Level,
		"OUTPUT_DIR":          &cfg.Output.Directory,
	}

	// Apply environment variable values
	for envVar, target := range envOverrides {
		if value := os.Getenv(envVar); value != "" {
			switch v := target.(type) {
			case *string:
				*v = value
			case *int:
				if intVal, err := strconv.Atoi(value); err == nil {
					*v = intVal
				}
			case *[]string:
				*v = []string{value} // Simplified for single value
			}
		}
	}

	return nil
}

// Validate ensures all required configuration values are present and valid
func (c *Config) Validate() error {
	// MySQL validation
	if c.MySQL.Host == "" {
		return fmt.Errorf("mysql host is required")
	}
	if c.MySQL.Database == "" {
		return fmt.Errorf("mysql database is required")
	}
	if c.MySQL.Port <= 0 || c.MySQL.Port > 65535 {
		return fmt.Errorf("mysql port must be between 1 and 65535")
	}

	// Dgraph validation
	if len(c.Dgraph.Alpha) == 0 {
		return fmt.Errorf("at least one dgraph alpha endpoint is required")
	}

	// Pipeline validation
	if c.Pipeline.Workers <= 0 {
		return fmt.Errorf("pipeline workers must be positive")
	}
	if c.Pipeline.BatchSize <= 0 {
		return fmt.Errorf("pipeline batch size must be positive")
	}

	// Output validation
	if c.Output.Directory == "" {
		return fmt.Errorf("output directory is required")
	}

	return nil
}

// ConnectionString builds a MySQL DSN (Data Source Name) connection string
func (m *MySQLConfig) ConnectionString() string {
	return fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true&timeout=%s",
		m.User, m.Password, m.Host, m.Port, m.Database, m.Timeout)
}
