// Package pipeline provides the core MySQL to Dgraph migration functionality.
// It orchestrates schema extraction, data processing, and output generation.
package pipeline

import (
	"bufio"
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/shahariaz/mysql_to_dgraph_pipeline/internal/config"
	"github.com/shahariaz/mysql_to_dgraph_pipeline/pkg/logger"
)

// Pipeline manages the complete MySQL to Dgraph migration process
type Pipeline struct {
	// Configuration and dependencies
	cfg    *config.Config
	logger *logger.Logger

	// Database connections
	mysqlDB *sql.DB

	// Execution context and control
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// Progress tracking and monitoring
	progress *ProgressTracker

	// Core components
	schema          *SchemaExtractor // Handles MySQL schema extraction
	extractedSchema *Schema          // Cached extracted schema
	processor       *DataProcessor   // Handles data processing and conversion
	validator       *DataValidator   // Handles data validation
}

// ProgressTracker monitors and reports migration progress
type ProgressTracker struct {
	mu              sync.RWMutex // Protects concurrent access to progress data
	TotalTables     int          // Total number of tables to process
	ProcessedTables int          // Number of tables completed
	TotalRows       int64        // Total number of rows to process
	ProcessedRows   int64        // Number of rows processed
	CurrentTable    string       // Currently processing table name
	StartTime       time.Time    // Pipeline start time
	LastReportTime  time.Time    // Last progress report time
	ErrorCount      int64        // Number of errors encountered
}

// New creates and initializes a new Pipeline instance
func New(cfg *config.Config, logger *logger.Logger) (*Pipeline, error) {
	// Validate configuration before proceeding
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}

	// Create cancellable context for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())

	// Establish MySQL database connection
	mysqlDB, err := connectToMySQL(cfg, ctx)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to connect to MySQL: %w", err)
	}

	// Initialize progress tracking
	progress := &ProgressTracker{
		StartTime:      time.Now(),
		LastReportTime: time.Now(),
	}

	// Create pipeline instance
	p := &Pipeline{
		cfg:      cfg,
		logger:   logger,
		mysqlDB:  mysqlDB,
		ctx:      ctx,
		cancel:   cancel,
		progress: progress,
	}

	// Initialize core components
	p.schema = NewSchemaExtractor(mysqlDB, logger)
	p.processor = NewDataProcessor(cfg, logger, progress)
	p.validator = NewDataValidator(mysqlDB, cfg, logger)

	return p, nil
}

// connectToMySQL establishes and configures MySQL database connection
func connectToMySQL(cfg *config.Config, ctx context.Context) (*sql.DB, error) {
	// Build connection string
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true&timeout=%s",
		cfg.MySQL.User, cfg.MySQL.Password, cfg.MySQL.Host, cfg.MySQL.Port,
		cfg.MySQL.Database, cfg.MySQL.Timeout)

	// Open database connection
	mysqlDB, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}

	// Configure connection pool for optimal performance
	mysqlDB.SetMaxOpenConns(cfg.MySQL.MaxConnections)
	mysqlDB.SetMaxIdleConns(cfg.MySQL.MaxConnections / 2)
	mysqlDB.SetConnMaxLifetime(cfg.MySQL.ConnMaxLifetime)
	mysqlDB.SetConnMaxIdleTime(cfg.MySQL.ConnMaxIdleTime)

	// Test connection
	if err := mysqlDB.PingContext(ctx); err != nil {
		mysqlDB.Close()
		return nil, fmt.Errorf("failed to ping MySQL: %w", err)
	}

	return mysqlDB, nil
}

// Stop gracefully shuts down the pipeline
func (p *Pipeline) Stop() {
	p.logger.Info("Stopping pipeline...")
	p.cancel()
	p.wg.Wait()
	if p.mysqlDB != nil {
		p.mysqlDB.Close()
	}
	p.logger.Info("Pipeline stopped")
}

func (p *Pipeline) ExtractSchema() error {
	p.logger.Info("Starting schema extraction")
	schema, err := p.schema.ExtractSchema(p.ctx, p.cfg.MySQL.Database)
	if err != nil {
		return fmt.Errorf("schema extraction failed: %w", err)
	}

	// Store the extracted schema
	p.extractedSchema = schema

	p.logger.Info("Schema extracted successfully",
		"tables", len(schema.Tables),
		"relationships", len(schema.Relationships))

	return nil
}

func (p *Pipeline) GenerateDgraphSchema() error {
	p.logger.Info("Generating Dgraph schema")

	schema, err := p.schema.ExtractSchema(p.ctx, p.cfg.MySQL.Database)
	if err != nil {
		return fmt.Errorf("failed to extract schema: %w", err)
	}

	generator := NewSchemaGenerator(p.cfg, p.logger)
	if err := generator.Generate(schema); err != nil {
		return fmt.Errorf("schema generation failed: %w", err)
	}

	p.logger.Info("Dgraph schema generated successfully")
	return nil
}

func (p *Pipeline) MigrateData(tables string) error {
	p.logger.Info("Starting data migration")

	// Extract schema first
	schema, err := p.schema.ExtractSchema(p.ctx, p.cfg.MySQL.Database)
	if err != nil {
		return fmt.Errorf("failed to extract schema: %w", err)
	}

	// Determine tables to process
	tablesToProcess := p.determineTablesToProcess(schema, tables)
	p.progress.TotalTables = len(tablesToProcess)

	p.logger.Info("Starting data processing",
		"tables", len(tablesToProcess),
		"workers", p.cfg.Pipeline.Workers)

	// Start progress reporter
	go p.reportProgress()

	// Process tables
	if err := p.processor.ProcessTables(p.ctx, p.mysqlDB, schema, tablesToProcess); err != nil {
		return fmt.Errorf("data processing failed: %w", err)
	}

	p.logger.Info("Data migration completed successfully")
	return nil
}

func (p *Pipeline) ValidateData() error {
	p.logger.Info("Starting data validation")

	if err := p.validator.ValidateIntegrity(p.ctx); err != nil {
		return fmt.Errorf("data validation failed: %w", err)
	}

	p.logger.Info("Data validation completed successfully")
	return nil
}

// RunFull executes the complete migration pipeline
func (p *Pipeline) RunFull(tables string) error {
	p.logger.Info("Starting complete pipeline execution")

	// Step 1: Extract MySQL schema structure
	if err := p.ExtractSchema(); err != nil {
		return fmt.Errorf("schema extraction failed: %w", err)
	}

	// Step 2: Migrate data to discover actual relationships
	if err := p.MigrateData(tables); err != nil {
		return fmt.Errorf("data migration failed: %w", err)
	}

	// Step 3: Generate final schema with discovered relationships
	if err := p.GenerateDgraphSchemaFromData(); err != nil {
		return fmt.Errorf("schema generation failed: %w", err)
	}

	// Step 4: Validate data integrity (optional)
	if !p.cfg.Pipeline.SkipValidation {
		if err := p.ValidateData(); err != nil {
			return fmt.Errorf("data validation failed: %w", err)
		}
	}

	p.logger.Info("Complete pipeline executed successfully")
	return nil
}

// determineTablesToProcess returns the list of tables to process based on input
func (p *Pipeline) determineTablesToProcess(schema *Schema, tables string) []string {
	if tables == "" {
		// Process all tables in the schema
		var allTables []string
		for tableName := range schema.Tables {
			allTables = append(allTables, tableName)
		}
		return allTables
	}

	// Parse and validate specified tables
	var result []string
	for _, table := range strings.Split(tables, ",") {
		table = strings.TrimSpace(table)
		if table == "" {
			continue
		}

		if _, exists := schema.Tables[table]; exists {
			result = append(result, table)
		} else {
			p.logger.Warn("Table not found in schema", "table", table)
		}
	}
	return result
}

// reportProgress runs a background goroutine to report pipeline progress
func (p *Pipeline) reportProgress() {
	ticker := time.NewTicker(p.cfg.Pipeline.ProgressReportInterval)
	defer ticker.Stop()

	for {
		select {
		case <-p.ctx.Done():
			return
		case <-ticker.C:
			p.logProgress()
		}
	}
}

// logProgress logs the current pipeline progress with performance metrics
func (p *Pipeline) logProgress() {
	p.progress.mu.RLock()
	defer p.progress.mu.RUnlock()

	elapsed := time.Since(p.progress.StartTime)

	// Calculate processing rate
	var rowsPerSecond float64
	if elapsed.Seconds() > 0 {
		rowsPerSecond = float64(p.progress.ProcessedRows) / elapsed.Seconds()
	}

	// Estimate time remaining
	var eta time.Duration
	if p.progress.ProcessedRows > 0 && p.progress.TotalRows > 0 {
		remainingRows := p.progress.TotalRows - p.progress.ProcessedRows
		if rowsPerSecond > 0 {
			eta = time.Duration(float64(remainingRows)/rowsPerSecond) * time.Second
		}
	}

	p.logger.Info("Pipeline progress report",
		"current_table", p.progress.CurrentTable,
		"processed_tables", p.progress.ProcessedTables,
		"total_tables", p.progress.TotalTables,
		"processed_rows", p.progress.ProcessedRows,
		"total_rows", p.progress.TotalRows,
		"rows_per_second", fmt.Sprintf("%.2f", rowsPerSecond),
		"elapsed", elapsed.Round(time.Second),
		"eta", eta.Round(time.Second),
		"errors", p.progress.ErrorCount,
	)
}

// GenerateDgraphSchemaFromData generates Dgraph schema by analyzing the processed RDF data
func (p *Pipeline) GenerateDgraphSchemaFromData() error {
	p.logger.Info("Generating Dgraph schema from processed data")

	// Use the stored extracted schema
	if p.extractedSchema == nil {
		return fmt.Errorf("no schema available - run ExtractSchema first")
	}

	// Read the RDF file to discover actual relationships
	rdfFile := filepath.Join(p.cfg.Output.Directory, p.cfg.Output.RDFFile)
	if _, err := os.Stat(rdfFile); os.IsNotExist(err) {
		return fmt.Errorf("RDF file not found: %s", rdfFile)
	}

	// Parse RDF to discover relationships
	discoveredRelationships, err := p.parseRDFForRelationships(rdfFile)
	if err != nil {
		return fmt.Errorf("failed to parse RDF for relationships: %w", err)
	}

	// Update schema with discovered relationships
	p.extractedSchema.Relationships = append(p.extractedSchema.Relationships, discoveredRelationships...)

	// Generate final schema with all relationships
	generator := NewSchemaGenerator(p.cfg, p.logger)
	if err := generator.Generate(p.extractedSchema); err != nil {
		return fmt.Errorf("failed to generate schema: %w", err)
	}

	p.logger.Info("Dgraph schema generated from data successfully",
		"discovered_relationships", len(discoveredRelationships))
	return nil
}

// parseRDFForRelationships parses the RDF file to discover actual relationships used
func (p *Pipeline) parseRDFForRelationships(rdfFile string) ([]ForeignKey, error) {
	file, err := os.Open(rdfFile)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var relationships []ForeignKey
	relationshipMap := make(map[string]ForeignKey) // To avoid duplicates

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		// Look for relationship patterns: _:table_id <table.column> _:ref_table_id
		if strings.Contains(line, "_:") && strings.Contains(line, ">") && strings.Contains(line, "<") {
			parts := strings.Fields(line)
			if len(parts) >= 3 {
				// subject := parts[0]  // _:table_id (not needed for relationship discovery)
				predicate := parts[1] // <table.column>
				object := parts[2]    // _:ref_table_id or "value"

				// Check if this is a relationship (object is a blank node)
				if strings.HasPrefix(object, "_:") && strings.HasPrefix(predicate, "<") && strings.HasSuffix(predicate, ">") {
					// Extract table and column from predicate
					pred := strings.Trim(predicate, "<>")
					predParts := strings.Split(pred, ".")
					if len(predParts) == 2 {
						tableName := predParts[0]
						columnName := predParts[1]

						// Extract referenced table from object
						refTableName := strings.TrimPrefix(object, "_:")
						if underscoreIdx := strings.LastIndex(refTableName, "_"); underscoreIdx > 0 {
							refTableName = refTableName[:underscoreIdx]
						}

						// Create relationship key to avoid duplicates
						relationshipKey := fmt.Sprintf("%s.%s->%s", tableName, columnName, refTableName)

						if _, exists := relationshipMap[relationshipKey]; !exists {
							relationshipMap[relationshipKey] = ForeignKey{
								TableName:      tableName,
								ColumnName:     columnName,
								RefTableName:   refTableName,
								RefColumnName:  "id", // Assume primary key is 'id'
								ConstraintName: fmt.Sprintf("fk_%s_%s", tableName, columnName),
							}
						}
					}
				}
			}
		}
	}

	// Convert map to slice
	for _, rel := range relationshipMap {
		relationships = append(relationships, rel)
	}

	return relationships, scanner.Err()
}
