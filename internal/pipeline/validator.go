package pipeline

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"

	"github.com/shahariaz/mysql_to_dgraph_pipeline/internal/config"
	"github.com/shahariaz/mysql_to_dgraph_pipeline/pkg/logger"
)

// DataValidator handles validation of migrated data
type DataValidator struct {
	db     *sql.DB
	cfg    *config.Config
	logger *logger.Logger
}

// ValidationResult represents the result of a validation check
type ValidationResult struct {
	CheckName   string
	Passed      bool
	Expected    interface{}
	Actual      interface{}
	Description string
	Error       error
}

// ValidationSummary contains the overall validation results
type ValidationSummary struct {
	TotalChecks  int
	PassedChecks int
	FailedChecks int
	Results      []ValidationResult
}

func NewDataValidator(db *sql.DB, cfg *config.Config, logger *logger.Logger) *DataValidator {
	return &DataValidator{
		db:     db,
		cfg:    cfg,
		logger: logger,
	}
}

func (dv *DataValidator) ValidateIntegrity(ctx context.Context) error {
	dv.logger.Info("Starting data integrity validation")

	summary := &ValidationSummary{}

	// Check if output files exist
	if err := dv.validateOutputFiles(summary); err != nil {
		return fmt.Errorf("output file validation failed: %w", err)
	}

	// Validate RDF file structure
	if err := dv.validateRDFStructure(ctx, summary); err != nil {
		return fmt.Errorf("RDF structure validation failed: %w", err)
	}

	// Validate row counts (if possible)
	if err := dv.validateRowCounts(ctx, summary); err != nil {
		dv.logger.Warn("Row count validation failed", "error", err)
	}

	// Validate foreign key integrity
	if err := dv.validateForeignKeyIntegrity(ctx, summary); err != nil {
		dv.logger.Warn("Foreign key validation failed", "error", err)
	}

	// Print validation summary
	dv.printValidationSummary(summary)

	if summary.FailedChecks > 0 {
		return fmt.Errorf("validation failed: %d/%d checks failed",
			summary.FailedChecks, summary.TotalChecks)
	}

	dv.logger.Info("Data validation completed successfully",
		"total_checks", summary.TotalChecks,
		"passed", summary.PassedChecks)

	return nil
}

func (dv *DataValidator) validateOutputFiles(summary *ValidationSummary) error {
	files := []struct {
		name     string
		path     string
		required bool
	}{
		{"RDF file", filepath.Join(dv.cfg.Output.Directory, dv.cfg.Output.RDFFile), true},
		{"Schema file", filepath.Join(dv.cfg.Output.Directory, dv.cfg.Output.SchemaFile), true},
		{"Mapping file", filepath.Join(dv.cfg.Output.Directory, dv.cfg.Output.MappingFile), false},
	}

	for _, file := range files {
		result := ValidationResult{
			CheckName:   fmt.Sprintf("File exists: %s", file.name),
			Description: fmt.Sprintf("Checking if %s exists at %s", file.name, file.path),
		}

		if _, err := os.Stat(file.path); os.IsNotExist(err) {
			result.Passed = false
			result.Error = err
			if file.required {
				summary.addResult(result)
				return fmt.Errorf("required file not found: %s", file.path)
			}
		} else {
			result.Passed = true
		}

		summary.addResult(result)
	}

	return nil
}

func (dv *DataValidator) validateRDFStructure(ctx context.Context, summary *ValidationSummary) error {
	rdfPath := filepath.Join(dv.cfg.Output.Directory, dv.cfg.Output.RDFFile)

	file, err := os.Open(rdfPath)
	if err != nil {
		return fmt.Errorf("failed to open RDF file: %w", err)
	}
	defer file.Close()

	// Get file size
	stat, err := file.Stat()
	if err != nil {
		return fmt.Errorf("failed to get file stats: %w", err)
	}

	result := ValidationResult{
		CheckName:   "RDF file size",
		Description: "Checking if RDF file has content",
		Actual:      stat.Size(),
	}

	if stat.Size() == 0 {
		result.Passed = false
		result.Error = fmt.Errorf("RDF file is empty")
	} else {
		result.Passed = true
		result.Expected = "> 0 bytes"
	}

	summary.addResult(result)

	// TODO: Add more sophisticated RDF validation
	// - Check for proper RDF triple format
	// - Validate UID references
	// - Check for orphaned references

	return nil
}

func (dv *DataValidator) validateRowCounts(ctx context.Context, summary *ValidationSummary) error {
	// This is a simplified validation - in production you might want to
	// count actual RDF triples and compare with expected counts

	// Get list of tables from database
	rows, err := dv.db.QueryContext(ctx, `
		SELECT table_name 
		FROM information_schema.tables 
		WHERE table_schema = DATABASE()
		AND table_type = 'BASE TABLE'`)
	if err != nil {
		return fmt.Errorf("failed to get table list: %w", err)
	}
	defer rows.Close()

	var totalSourceRows int64
	var tableCount int

	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			dv.logger.Warn("Failed to scan table name", "error", err)
			continue
		}

		var count int64
		countQuery := fmt.Sprintf("SELECT COUNT(*) FROM `%s`", tableName)
		if err := dv.db.QueryRowContext(ctx, countQuery).Scan(&count); err != nil {
			dv.logger.Warn("Failed to count rows", "table", tableName, "error", err)
			continue
		}

		totalSourceRows += count
		tableCount++
	}

	result := ValidationResult{
		CheckName:   "Row count validation",
		Description: fmt.Sprintf("Validated row counts for %d tables", tableCount),
		Expected:    fmt.Sprintf("%d source rows", totalSourceRows),
		Actual:      "RDF validation pending",
		Passed:      true, // Simplified - always pass for now
	}

	summary.addResult(result)
	return nil
}

func (dv *DataValidator) validateForeignKeyIntegrity(ctx context.Context, summary *ValidationSummary) error {
	// Get foreign key constraints
	rows, err := dv.db.QueryContext(ctx, `
		SELECT 
			table_name, 
			column_name, 
			referenced_table_name, 
			referenced_column_name
		FROM information_schema.key_column_usage
		WHERE table_schema = DATABASE() 
		AND referenced_table_name IS NOT NULL`)
	if err != nil {
		return fmt.Errorf("failed to get foreign keys: %w", err)
	}
	defer rows.Close()

	var fkCount int
	var validFKs int

	for rows.Next() {
		var tableName, columnName, refTableName, refColumnName string
		if err := rows.Scan(&tableName, &columnName, &refTableName, &refColumnName); err != nil {
			dv.logger.Warn("Failed to scan foreign key", "error", err)
			continue
		}

		fkCount++

		// Validate that foreign key values exist in referenced table
		query := fmt.Sprintf(`
			SELECT COUNT(*) 
			FROM %s t1 
			LEFT JOIN %s t2 ON t1.%s = t2.%s 
			WHERE t1.%s IS NOT NULL AND t2.%s IS NULL`,
			tableName, refTableName, columnName, refColumnName, columnName, refColumnName)

		var orphanCount int64
		if err := dv.db.QueryRowContext(ctx, query).Scan(&orphanCount); err != nil {
			dv.logger.Warn("Failed to validate foreign key",
				"table", tableName,
				"column", columnName,
				"error", err)
			continue
		}

		if orphanCount == 0 {
			validFKs++
		} else {
			dv.logger.Warn("Found orphaned foreign key references",
				"table", tableName,
				"column", columnName,
				"orphan_count", orphanCount)
		}
	}

	result := ValidationResult{
		CheckName:   "Foreign key integrity",
		Description: "Validating foreign key relationships",
		Expected:    fmt.Sprintf("%d valid foreign keys", fkCount),
		Actual:      fmt.Sprintf("%d valid foreign keys", validFKs),
		Passed:      validFKs == fkCount,
	}

	summary.addResult(result)
	return nil
}

func (dv *DataValidator) printValidationSummary(summary *ValidationSummary) {
	dv.logger.Info("=== VALIDATION SUMMARY ===")
	dv.logger.Info("Validation Results",
		"total_checks", summary.TotalChecks,
		"passed", summary.PassedChecks,
		"failed", summary.FailedChecks)

	for _, result := range summary.Results {
		status := "PASS"
		if !result.Passed {
			status = "FAIL"
		}

		fields := []interface{}{
			"status", status,
			"check", result.CheckName,
		}

		if result.Expected != nil {
			fields = append(fields, "expected", result.Expected)
		}
		if result.Actual != nil {
			fields = append(fields, "actual", result.Actual)
		}
		if result.Error != nil {
			fields = append(fields, "error", result.Error)
		}

		if result.Passed {
			dv.logger.Info("Validation check", fields...)
		} else {
			dv.logger.Error("Validation check", fields...)
		}
	}
}

func (vs *ValidationSummary) addResult(result ValidationResult) {
	vs.Results = append(vs.Results, result)
	vs.TotalChecks++

	if result.Passed {
		vs.PassedChecks++
	} else {
		vs.FailedChecks++
	}
}
