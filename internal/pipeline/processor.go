package pipeline

import (
	"bufio"
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/shahariaz/mysql_to_dgraph_pipeline/internal/config"
	"github.com/shahariaz/mysql_to_dgraph_pipeline/pkg/logger"
)

// PerformanceMetrics tracks processing performance
type PerformanceMetrics struct {
	StartTime       time.Time
	TotalRows       int64
	ProcessedRows   int64
	CurrentTable    string
	TablesCount     int
	ProcessedTables int
	RecordsPerSec   float64
	MemoryUsageMB   float64
	mu              sync.RWMutex
}

func (pm *PerformanceMetrics) UpdateProgress(processedRows int64, currentTable string) {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	pm.ProcessedRows = processedRows
	pm.CurrentTable = currentTable

	elapsed := time.Since(pm.StartTime).Seconds()
	if elapsed > 0 {
		pm.RecordsPerSec = float64(pm.ProcessedRows) / elapsed
	}

	// Get memory usage
	var m runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&m)
	pm.MemoryUsageMB = float64(m.Alloc) / 1024 / 1024
}

func (pm *PerformanceMetrics) GetStats() (int64, float64, float64, string) {
	pm.mu.RLock()
	defer pm.mu.RUnlock()
	return pm.ProcessedRows, pm.RecordsPerSec, pm.MemoryUsageMB, pm.CurrentTable
}

func (pm *PerformanceMetrics) EstimateCompletion() time.Duration {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	if pm.RecordsPerSec <= 0 || pm.ProcessedRows >= pm.TotalRows {
		return 0
	}

	remaining := pm.TotalRows - pm.ProcessedRows
	secondsRemaining := float64(remaining) / pm.RecordsPerSec
	return time.Duration(secondsRemaining) * time.Second
}

// DataProcessor handles the conversion and processing of MySQL data to RDF format
type DataProcessor struct {
	cfg        *config.Config
	logger     *logger.Logger
	progress   *ProgressTracker
	metrics    *PerformanceMetrics
	uidMap     map[string]string // Global UID mapping
	uidMapMu   sync.RWMutex
	outputFile *os.File
	outputMu   sync.Mutex
}

// TableJob represents a table processing job
type TableJob struct {
	TableName string
	Schema    *Schema
	BatchSize int
	Offset    int64
	Limit     int64
}

// ProcessingResult contains the results of table processing
type ProcessingResult struct {
	TableName     string
	RowsProcessed int64
	Error         error
	Duration      time.Duration
}

func NewDataProcessor(cfg *config.Config, logger *logger.Logger, progress *ProgressTracker) *DataProcessor {
	return &DataProcessor{
		cfg:      cfg,
		logger:   logger,
		progress: progress,
		metrics: &PerformanceMetrics{
			StartTime: time.Now(),
		},
		uidMap: make(map[string]string),
	}
}

// StartPerformanceMonitoring starts a goroutine to periodically log performance metrics
func (dp *DataProcessor) StartPerformanceMonitoring(ctx context.Context) {
	ticker := time.NewTicker(10 * time.Second) // Log every 10 seconds
	defer ticker.Stop()

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				processed, rps, memMB, currentTable := dp.metrics.GetStats()
				eta := dp.metrics.EstimateCompletion()

				dp.logger.Info("Performance metrics",
					"processed_rows", processed,
					"records_per_second", fmt.Sprintf("%.2f", rps),
					"memory_mb", fmt.Sprintf("%.2f", memMB),
					"current_table", currentTable,
					"eta", eta.String(),
				)
			}
		}
	}()
}

func (dp *DataProcessor) ProcessTables(ctx context.Context, db *sql.DB, schema *Schema, tables []string) error {
	// Create output directory
	if err := os.MkdirAll(dp.cfg.Output.Directory, 0755); err != nil {
		return fmt.Errorf("failed to create output directory: %w", err)
	}

	// Open output file
	outputPath := filepath.Join(dp.cfg.Output.Directory, dp.cfg.Output.RDFFile)
	outputFile, err := os.Create(outputPath)
	if err != nil {
		return fmt.Errorf("failed to create output file: %w", err)
	}
	defer outputFile.Close()

	dp.outputFile = outputFile

	// Create buffered writer for better performance
	writer := bufio.NewWriterSize(outputFile, 64*1024) // 64KB buffer
	defer writer.Flush()

	// Calculate total rows for progress tracking
	totalRows, err := dp.calculateTotalRows(ctx, db, tables)
	if err != nil {
		dp.logger.Warn("Failed to calculate total rows", "error", err)
	} else {
		dp.progress.mu.Lock()
		dp.progress.TotalRows = totalRows
		dp.progress.mu.Unlock()
	}

	// Create worker pool
	jobChan := make(chan TableJob, dp.cfg.Pipeline.Workers)
	resultChan := make(chan ProcessingResult, dp.cfg.Pipeline.Workers)

	var wg sync.WaitGroup

	// Start workers
	for i := 0; i < dp.cfg.Pipeline.Workers; i++ {
		wg.Add(1)
		go dp.worker(ctx, &wg, db, schema, jobChan, resultChan, writer)
	}

	// Start result collector
	go dp.collectResults(resultChan)

	// Submit jobs
	go func() {
		defer close(jobChan)
		for _, tableName := range tables {
			if err := dp.submitTableJobs(ctx, db, schema, tableName, jobChan); err != nil {
				dp.logger.Error("Failed to submit jobs for table", "table", tableName, "error", err)
			}
		}
	}()

	// Wait for all workers to complete
	wg.Wait()
	close(resultChan)

	// Write UID mappings to separate file
	if err := dp.writeUIDMappings(); err != nil {
		dp.logger.Error("Failed to write UID mappings", "error", err)
	}

	dp.logger.Info("Data processing completed", "tables", len(tables))
	return nil
}

func (dp *DataProcessor) worker(ctx context.Context, wg *sync.WaitGroup, db *sql.DB, schema *Schema,
	jobChan <-chan TableJob, resultChan chan<- ProcessingResult, writer *bufio.Writer) {

	defer wg.Done()

	for job := range jobChan {
		select {
		case <-ctx.Done():
			return
		default:
			result := dp.processTableBatch(ctx, db, job, writer)
			resultChan <- result
		}
	}
}

func (dp *DataProcessor) processTableBatch(ctx context.Context, db *sql.DB, job TableJob, writer *bufio.Writer) ProcessingResult {
	startTime := time.Now()

	dp.progress.mu.Lock()
	dp.progress.CurrentTable = job.TableName
	dp.progress.mu.Unlock()

	table := job.Schema.Tables[job.TableName]
	if table == nil {
		return ProcessingResult{
			TableName: job.TableName,
			Error:     fmt.Errorf("table schema not found"),
			Duration:  time.Since(startTime),
		}
	}

	// Build query
	query := fmt.Sprintf("SELECT * FROM `%s` LIMIT %d OFFSET %d",
		job.TableName, job.Limit, job.Offset)

	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return ProcessingResult{
			TableName: job.TableName,
			Error:     fmt.Errorf("query failed: %w", err),
			Duration:  time.Since(startTime),
		}
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return ProcessingResult{
			TableName: job.TableName,
			Error:     fmt.Errorf("failed to get columns: %w", err),
			Duration:  time.Since(startTime),
		}
	}

	// Prepare scan arguments
	values := make([]sql.RawBytes, len(cols))
	scanArgs := make([]interface{}, len(cols))
	for i := range values {
		scanArgs[i] = &values[i]
	}

	var processedRows int64
	var rdfLines []string

	for rows.Next() {
		if err := rows.Scan(scanArgs...); err != nil {
			dp.logger.Error("Failed to scan row", "table", job.TableName, "error", err)
			continue
		}

		rdfData, err := dp.convertRowToRDF(job.TableName, cols, values, job.Schema)
		if err != nil {
			dp.logger.Error("Failed to convert row to RDF", "table", job.TableName, "error", err)
			continue
		}

		rdfLines = append(rdfLines, rdfData...)
		processedRows++

		// Memory management - write in batches
		if len(rdfLines) >= 100 {
			dp.writeRDFLines(writer, rdfLines)
			rdfLines = rdfLines[:0] // Clear slice but keep capacity
		}
	}

	// Write remaining lines
	if len(rdfLines) > 0 {
		dp.writeRDFLines(writer, rdfLines)
	}

	// Update progress
	dp.progress.mu.Lock()
	dp.progress.ProcessedRows += processedRows
	dp.progress.mu.Unlock()

	// Force garbage collection periodically
	if processedRows > 0 && processedRows%1000 == 0 {
		runtime.GC()
	}

	return ProcessingResult{
		TableName:     job.TableName,
		RowsProcessed: processedRows,
		Duration:      time.Since(startTime),
	}
}

func (dp *DataProcessor) convertRowToRDF(tableName string, cols []string, values []sql.RawBytes, schema *Schema) ([]string, error) {
	var rdfLines []string

	// Generate UID for this row
	rowUID := dp.generateRowUID(tableName, cols, values)

	// Add type declaration
	rdfLines = append(rdfLines, fmt.Sprintf("%s <dgraph.type> \"%s\" .", rowUID, tableName))

	// Process each column
	for i, col := range cols {
		val := string(values[i])
		if val == "" || strings.ToLower(val) == "null" {
			continue
		}

		predicate := fmt.Sprintf("%s.%s", tableName, col)

		// Check if this is a foreign key
		isFK, refTable := dp.isForeignKey(tableName, col, schema)

		if isFK {
			// Create reference to foreign entity
			refUID := dp.getOrCreateUID(refTable, val)
			rdfLines = append(rdfLines, fmt.Sprintf("%s <%s> %s .", rowUID, predicate, refUID))

			// Add reverse edge
			reversePredicate := fmt.Sprintf("%s.%s_reverse", tableName, col)
			rdfLines = append(rdfLines, fmt.Sprintf("%s <%s> %s .", refUID, reversePredicate, rowUID))
		} else {
			// Regular data predicate
			escapedVal := dp.escapeRDFValue(val)
			rdfLines = append(rdfLines, fmt.Sprintf("%s <%s> \"%s\" .", rowUID, predicate, escapedVal))
		}
	}

	return rdfLines, nil
}

func (dp *DataProcessor) generateRowUID(tableName string, cols []string, values []sql.RawBytes) string {
	// Try to find primary key
	var pkValue string
	for i, col := range cols {
		if strings.ToLower(col) == "id" || strings.HasSuffix(strings.ToLower(col), "_id") {
			pkValue = string(values[i])
			break
		}
	}

	// If no primary key found, use first column
	if pkValue == "" && len(values) > 0 {
		pkValue = string(values[0])
	}

	return fmt.Sprintf("_:%s_%s", tableName, pkValue)
}

func (dp *DataProcessor) isForeignKey(tableName, columnName string, schema *Schema) (bool, string) {
	// Check explicit foreign key relationships first (most reliable)
	var foundRelationships []string
	for _, fk := range schema.Relationships {
		if fk.TableName == tableName && fk.ColumnName == columnName {
			foundRelationships = append(foundRelationships, fk.RefTableName)
		}
	}
	
	// If we found multiple relationships for the same column, log them for debugging
	if len(foundRelationships) > 1 {
		dp.logger.Warn("Multiple relationships found for column",
			"table", tableName,
			"column", columnName,
			"targets", foundRelationships)
	}
	
	// Return the first (most prioritized) relationship
	if len(foundRelationships) > 0 {
		dp.logger.Debug("Using relationship",
			"table", tableName,
			"column", columnName,
			"target", foundRelationships[0])
		return true, foundRelationships[0]
	}

	// Check naming conventions using the same logic as schema detection
	if IsForeignKey(columnName) {
		// Extract base name based on different FK naming patterns
		var baseName string
		columnLower := strings.ToLower(columnName)
		
		switch {
		case strings.HasSuffix(columnLower, "_id"):
			baseName = strings.TrimSuffix(columnLower, "_id")
		case strings.HasSuffix(columnLower, "_key"):
			baseName = strings.TrimSuffix(columnLower, "_key")
		case strings.HasSuffix(columnLower, "_ref"):
			baseName = strings.TrimSuffix(columnLower, "_ref")
		case strings.HasPrefix(columnLower, "id_"):
			baseName = strings.TrimPrefix(columnLower, "id_")
		case strings.HasPrefix(columnLower, "fk_"):
			baseName = strings.TrimPrefix(columnLower, "fk_")
		default:
			baseName = columnLower
		}

		// Try multiple table name patterns
		candidates := []string{
			baseName,           // Direct match
			baseName + "s",     // Plural
			baseName + "es",    // Plural with 'es'
			baseName + "ies",   // Plural with 'ies'
		}

		// Handle self-referential foreign keys
		if baseName == "parent" || baseName == "original" || columnName == tableName+"_id" {
			candidates = append(candidates, tableName)
		}

		// Try to find existing table that matches any candidate
		for _, candidate := range candidates {
			if _, exists := schema.Tables[candidate]; exists {
				return true, candidate
			}
		}
	}

	return false, ""
}

func (dp *DataProcessor) getOrCreateUID(tableName, id string) string {
	key := fmt.Sprintf("%s:%s", tableName, id)

	dp.uidMapMu.RLock()
	if uid, exists := dp.uidMap[key]; exists {
		dp.uidMapMu.RUnlock()
		return uid
	}
	dp.uidMapMu.RUnlock()

	dp.uidMapMu.Lock()
	defer dp.uidMapMu.Unlock()

	// Double-check after acquiring write lock
	if uid, exists := dp.uidMap[key]; exists {
		return uid
	}

	uid := fmt.Sprintf("_:%s_%s", tableName, id)
	dp.uidMap[key] = uid
	return uid
}

func (dp *DataProcessor) escapeRDFValue(value string) string {
	value = strings.ReplaceAll(value, `\`, `\\`)
	value = strings.ReplaceAll(value, `"`, `\"`)
	value = strings.ReplaceAll(value, "\n", `\n`)
	value = strings.ReplaceAll(value, "\r", `\r`)
	value = strings.ReplaceAll(value, "\t", `\t`)
	return value
}

func (dp *DataProcessor) writeRDFLines(writer *bufio.Writer, lines []string) {
	dp.outputMu.Lock()
	defer dp.outputMu.Unlock()

	for _, line := range lines {
		writer.WriteString(line + "\n")
	}
}

func (dp *DataProcessor) submitTableJobs(ctx context.Context, db *sql.DB, schema *Schema, tableName string, jobChan chan<- TableJob) error {
	table := schema.Tables[tableName]
	if table == nil {
		return fmt.Errorf("table %s not found in schema", tableName)
	}

	batchSize := int64(dp.cfg.Pipeline.BatchSize)
	totalRows := table.RowCount

	// If table is small, process in single batch
	if totalRows <= batchSize {
		select {
		case jobChan <- TableJob{
			TableName: tableName,
			Schema:    schema,
			BatchSize: int(batchSize),
			Offset:    0,
			Limit:     totalRows,
		}:
		case <-ctx.Done():
			return ctx.Err()
		}
		return nil
	}

	// Split into batches for large tables
	for offset := int64(0); offset < totalRows; offset += batchSize {
		limit := batchSize
		if offset+batchSize > totalRows {
			limit = totalRows - offset
		}

		select {
		case jobChan <- TableJob{
			TableName: tableName,
			Schema:    schema,
			BatchSize: int(batchSize),
			Offset:    offset,
			Limit:     limit,
		}:
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return nil
}

func (dp *DataProcessor) calculateTotalRows(ctx context.Context, db *sql.DB, tables []string) (int64, error) {
	var total int64

	for _, tableName := range tables {
		var count int64
		query := fmt.Sprintf("SELECT COUNT(*) FROM `%s`", tableName)

		if err := db.QueryRowContext(ctx, query).Scan(&count); err != nil {
			dp.logger.Warn("Failed to count rows", "table", tableName, "error", err)
			continue
		}

		total += count
	}

	return total, nil
}

func (dp *DataProcessor) collectResults(resultChan <-chan ProcessingResult) {
	for result := range resultChan {
		if result.Error != nil {
			dp.logger.Error("Table processing failed",
				"table", result.TableName,
				"error", result.Error,
				"duration", result.Duration)

			dp.progress.mu.Lock()
			dp.progress.ErrorCount++
			dp.progress.mu.Unlock()
		} else {
			dp.logger.Debug("Table batch processed successfully",
				"table", result.TableName,
				"rows", result.RowsProcessed,
				"duration", result.Duration)
		}
	}
}

func (dp *DataProcessor) writeUIDMappings() error {
	mappingPath := filepath.Join(dp.cfg.Output.Directory, dp.cfg.Output.MappingFile)

	file, err := os.Create(mappingPath)
	if err != nil {
		return fmt.Errorf("failed to create mapping file: %w", err)
	}
	defer file.Close()

	dp.uidMapMu.RLock()
	defer dp.uidMapMu.RUnlock()

	writer := bufio.NewWriter(file)
	defer writer.Flush()

	// Write as simple key=value format for efficiency
	for key, uid := range dp.uidMap {
		fmt.Fprintf(writer, "%s=%s\n", key, uid)
	}

	dp.logger.Info("UID mappings written", "count", len(dp.uidMap), "file", mappingPath)
	return nil
}

// getTableRowCount returns the total number of rows in a table
func (dp *DataProcessor) getTableRowCount(tableName string) (int64, error) {
	db, err := sql.Open("mysql", dp.cfg.MySQL.ConnectionString())
	if err != nil {
		return 0, fmt.Errorf("failed to open database: %w", err)
	}
	defer db.Close()

	query := fmt.Sprintf("SELECT COUNT(*) FROM `%s`", tableName)
	var count int64
	err = db.QueryRow(query).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to count rows in table %s: %w", tableName, err)
	}

	return count, nil
}

// processTableBatchToWriter processes a batch from a table and writes to the provided writer
func (dp *DataProcessor) processTableBatchToWriter(ctx context.Context, tableName string, table *Table, offset, limit int64, writer *bufio.Writer, schema *Schema) (int64, error) {
	db, err := sql.Open("mysql", dp.cfg.MySQL.ConnectionString())
	if err != nil {
		return 0, fmt.Errorf("failed to open database: %w", err)
	}
	defer db.Close()

	// Build query
	query := fmt.Sprintf("SELECT * FROM `%s` LIMIT %d OFFSET %d", tableName, limit, offset)

	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return 0, fmt.Errorf("failed to query table %s: %w", tableName, err)
	}
	defer rows.Close()

	// Process rows
	var processedCount int64
	for rows.Next() {
		select {
		case <-ctx.Done():
			return processedCount, ctx.Err()
		default:
		}

		// Get row data
		columns, err := rows.Columns()
		if err != nil {
			return processedCount, fmt.Errorf("failed to get columns: %w", err)
		}

		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		err = rows.Scan(valuePtrs...)
		if err != nil {
			return processedCount, fmt.Errorf("failed to scan row: %w", err)
		}

		// Convert to RDF
		err = dp.writeRowAsRDF(writer, tableName, table, columns, values, schema)
		if err != nil {
			return processedCount, fmt.Errorf("failed to write RDF: %w", err)
		}

		processedCount++
	}

	return processedCount, rows.Err()
}

// writeRowAsRDF writes a single row as RDF triples
func (dp *DataProcessor) writeRowAsRDF(writer *bufio.Writer, tableName string, table *Table, columns []string, values []interface{}, schema *Schema) error {
	// Generate blank node ID
	var pkValue string
	for i, col := range columns {
		if len(table.PrimaryKeys) > 0 && col == table.PrimaryKeys[0] {
			pkValue = fmt.Sprintf("%v", values[i])
			break
		}
	}

	if pkValue == "" {
		return fmt.Errorf("primary key not found for table %s", tableName)
	}

	blankNodeID := fmt.Sprintf("_%s_%s", tableName, pkValue)

	// Store UID mapping
	dp.uidMapMu.Lock()
	dp.uidMap[fmt.Sprintf("%s:%s", tableName, pkValue)] = blankNodeID
	dp.uidMapMu.Unlock()

	// Write type
	fmt.Fprintf(writer, "%s <dgraph.type> \"%s\" .\n", blankNodeID, tableName)

	// Write properties
	for i, col := range columns {
		if values[i] == nil {
			continue
		}

		predicate := fmt.Sprintf("%s.%s", tableName, col)

		// Check if this is a foreign key by looking in schema relationships
		var refTable string
		isForeignKey := false
		for _, fk := range schema.Relationships {
			if fk.TableName == tableName && fk.ColumnName == col {
				refTable = fk.RefTableName
				isForeignKey = true
				break
			}
		}

		if isForeignKey {
			// This is a foreign key - create edge
			refBlankNodeID := fmt.Sprintf("_%s_%v", refTable, values[i])
			fmt.Fprintf(writer, "%s <%s> %s .\n", blankNodeID, predicate, refBlankNodeID)
		} else {
			// Regular property
			value := fmt.Sprintf("%v", values[i])
			fmt.Fprintf(writer, "%s <%s> \"%s\" .\n", blankNodeID, predicate, value)
		}
	}

	return nil
}
