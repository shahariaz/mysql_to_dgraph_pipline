// Package main provides the entry point for the MySQL to Dgraph migration pipeline.
// This tool extracts schema and data from MySQL databases and converts them to
// Dgraph-compatible RDF format with proper relationship mapping.
package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/shahariaz/mysql_to_dgraph_pipeline/internal/config"
	"github.com/shahariaz/mysql_to_dgraph_pipeline/internal/pipeline"
	"github.com/shahariaz/mysql_to_dgraph_pipeline/pkg/logger"
)

func main() {
	// Parse command line arguments
	var (
		configPath = flag.String("config", "config/config.yaml", "Path to YAML configuration file")
		mode       = flag.String("mode", "full", "Pipeline execution mode: schema, data, full, validate")
		dryRun     = flag.Bool("dry-run", false, "Preview mode - analyze without writing data")
		tables     = flag.String("tables", "", "Specific tables to process (comma-separated, empty = all)")
		parallel   = flag.Int("parallel", 4, "Number of parallel worker threads")
		batchSize  = flag.Int("batch-size", 1000, "Records per batch for processing")
	)
	flag.Parse()

	// Load and validate configuration
	cfg, err := config.Load(*configPath)
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}

	// Override configuration with command line flags
	if *parallel > 0 {
		cfg.Pipeline.Workers = *parallel
	}
	if *batchSize > 0 {
		cfg.Pipeline.BatchSize = *batchSize
	}
	cfg.Pipeline.DryRun = *dryRun

	// Initialize structured logger
	logger := logger.New(cfg.Logger.Level, cfg.Logger.Format)
	logger.Info("Starting MySQL to Dgraph migration pipeline",
		"mode", *mode,
		"config", *configPath,
		"dry_run", *dryRun,
		"workers", cfg.Pipeline.Workers,
		"batch_size", cfg.Pipeline.BatchSize)

	// Create and initialize the migration pipeline
	p, err := pipeline.New(cfg, logger)
	if err != nil {
		logger.Fatal("Failed to initialize pipeline", "error", err)
	}

	// Setup graceful shutdown handling
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		logger.Info("Received shutdown signal, stopping pipeline...")
		p.Stop()
	}()

	// Execute pipeline based on selected mode
	if err := runPipelineMode(p, *mode, *tables, logger); err != nil {
		logger.Fatal("Pipeline execution failed", "error", err)
	}

	logger.Info("Pipeline completed successfully")
}

// runPipelineMode executes the appropriate pipeline operation based on mode
func runPipelineMode(p *pipeline.Pipeline, mode, tables string, logger *logger.Logger) error {
	switch mode {
	case "schema":
		// Extract MySQL schema and generate Dgraph schema
		logger.Info("Running schema extraction and generation")
		if err := p.ExtractSchema(); err != nil {
			return err
		}
		return p.GenerateDgraphSchema()

	case "data":
		// Migrate data from MySQL to RDF format
		logger.Info("Running data migration")
		return p.MigrateData(tables)

	case "full":
		// Complete pipeline: schema + data + validation
		logger.Info("Running complete pipeline")
		return p.RunFull(tables)

	case "validate":
		// Validate migrated data integrity
		logger.Info("Running data validation")
		return p.ValidateData()

	default:
		logger.Fatal("Invalid pipeline mode", "mode", mode,
			"valid_modes", []string{"schema", "data", "full", "validate"})
		return nil
	}
}
