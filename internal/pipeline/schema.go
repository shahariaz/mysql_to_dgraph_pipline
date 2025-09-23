package pipeline

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/shahariaz/mysql_to_dgraph_pipeline/pkg/logger"
)

// Schema represents the MySQL database schema
type Schema struct {
	Database      string             `json:"database"`
	Tables        map[string]*Table  `json:"tables"`
	Relationships []ForeignKey       `json:"relationships"`
	Indexes       map[string][]Index `json:"indexes"`
}

// Table represents a MySQL table
type Table struct {
	Name        string             `json:"name"`
	Columns     map[string]*Column `json:"columns"`
	PrimaryKeys []string           `json:"primary_keys"`
	RowCount    int64              `json:"row_count"`
	Engine      string             `json:"engine"`
}

// Column represents a MySQL column
type Column struct {
	Name          string `json:"name"`
	Type          string `json:"type"`
	Nullable      bool   `json:"nullable"`
	Default       string `json:"default"`
	AutoIncrement bool   `json:"auto_increment"`
	Comment       string `json:"comment"`
}

// ForeignKey represents a foreign key relationship
type ForeignKey struct {
	ConstraintName string `json:"constraint_name"`
	TableName      string `json:"table_name"`
	ColumnName     string `json:"column_name"`
	RefTableName   string `json:"referenced_table_name"`
	RefColumnName  string `json:"referenced_column_name"`
	UpdateRule     string `json:"update_rule"`
	DeleteRule     string `json:"delete_rule"`
}

// Index represents a database index
type Index struct {
	Name      string   `json:"name"`
	TableName string   `json:"table_name"`
	Columns   []string `json:"columns"`
	Unique    bool     `json:"unique"`
	Type      string   `json:"type"`
}

// SchemaExtractor handles MySQL schema extraction
type SchemaExtractor struct {
	db     *sql.DB
	logger *logger.Logger
}

func NewSchemaExtractor(db *sql.DB, logger *logger.Logger) *SchemaExtractor {
	return &SchemaExtractor{
		db:     db,
		logger: logger,
	}
}

func (se *SchemaExtractor) ExtractSchema(ctx context.Context, database string) (*Schema, error) {
	schema := &Schema{
		Database: database,
		Tables:   make(map[string]*Table),
		Indexes:  make(map[string][]Index),
	}

	// Get tables
	tables, err := se.getTables(ctx, database)
	if err != nil {
		return nil, fmt.Errorf("failed to get tables: %w", err)
	}

	se.logger.Info("Found tables", "count", len(tables))

	// Extract table details
	for _, tableName := range tables {
		table, err := se.extractTableSchema(ctx, database, tableName)
		if err != nil {
			se.logger.Error("Failed to extract table schema", "table", tableName, "error", err)
			continue
		}
		schema.Tables[tableName] = table
	}

	// Get foreign keys
	fks, err := se.getForeignKeys(ctx, database)
	if err != nil {
		se.logger.Warn("Failed to get foreign keys", "error", err)
	} else {
		schema.Relationships = fks
	}

	// Detect additional foreign keys by naming convention
	conventionFKs := se.DetectForeignKeysByConvention(ctx, schema)
	if len(conventionFKs) > 0 {
		se.logger.Info("Found additional foreign keys by convention", "count", len(conventionFKs))
		schema.Relationships = append(schema.Relationships, conventionFKs...)
	}

	// Use data analyzer to discover relationships from actual data
	analyzer := NewDataAnalyzer(se.db, se.logger)
	candidates, err := analyzer.AnalyzeDataRelationships(ctx, schema)
	if err != nil {
		se.logger.Debug("Failed to analyze data relationships", "error", err)
	} else {
		dataFKs := []ForeignKey{}

		// Create a map to track existing relationships to avoid duplicates
		existingRelationships := make(map[string]string) // key: table.column, value: refTable
		for _, fk := range schema.Relationships {
			key := fmt.Sprintf("%s.%s", fk.TableName, fk.ColumnName)
			existingRelationships[key] = fk.RefTableName
		}

		for _, candidate := range candidates {
			// Apply high-confidence relationships (>50% match rate)
			if candidate.Confidence > 0.5 {
				key := fmt.Sprintf("%s.%s", candidate.FromTable, candidate.FromColumn)

				// Check if we already have a relationship for this column
				if existingRefTable, exists := existingRelationships[key]; exists {
					se.logger.Info("Data-driven relationship conflicts with existing relationship",
						"table.column", key,
						"existing_target", existingRefTable,
						"data_target", candidate.ToTable,
						"confidence", fmt.Sprintf("%.2f", candidate.Confidence))

					// Data-driven relationships with high confidence should override convention-based ones
					if candidate.Confidence > 0.8 {
						se.logger.Info("Overriding existing relationship with high-confidence data-driven relationship",
							"table.column", key,
							"old_target", existingRefTable,
							"new_target", candidate.ToTable)

						// Remove the existing relationship
						var filteredRelationships []ForeignKey
						for _, fk := range schema.Relationships {
							if !(fk.TableName == candidate.FromTable && fk.ColumnName == candidate.FromColumn) {
								filteredRelationships = append(filteredRelationships, fk)
							}
						}
						schema.Relationships = filteredRelationships
						existingRelationships[key] = candidate.ToTable
					} else {
						se.logger.Info("Keeping existing relationship (data-driven confidence too low)",
							"table.column", key,
							"keeping_target", existingRefTable)
						continue
					}
				} else {
					existingRelationships[key] = candidate.ToTable
				}

				fk := ForeignKey{
					ConstraintName: fmt.Sprintf("data_fk_%s_%s", candidate.FromTable, candidate.FromColumn),
					TableName:      candidate.FromTable,
					ColumnName:     candidate.FromColumn,
					RefTableName:   candidate.ToTable,
					RefColumnName:  candidate.ToColumn,
					UpdateRule:     "CASCADE",
					DeleteRule:     "RESTRICT",
				}
				dataFKs = append(dataFKs, fk)
				se.logger.Info("Applied data-driven relationship",
					"from", fmt.Sprintf("%s.%s", candidate.FromTable, candidate.FromColumn),
					"to", fmt.Sprintf("%s.%s", candidate.ToTable, candidate.ToColumn),
					"confidence", fmt.Sprintf("%.2f", candidate.Confidence))
			}
		}
		if len(dataFKs) > 0 {
			se.logger.Info("Found additional foreign keys from data analysis", "count", len(dataFKs))
			schema.Relationships = append(schema.Relationships, dataFKs...)
		}
	}

	// Get indexes
	indexes, err := se.getIndexes(ctx, database)
	if err != nil {
		se.logger.Warn("Failed to get indexes", "error", err)
	} else {
		schema.Indexes = indexes
	}

	se.logger.Info("Schema extraction completed",
		"tables", len(schema.Tables),
		"relationships", len(schema.Relationships),
		"indexes", len(schema.Indexes))

	return schema, nil
}

func (se *SchemaExtractor) getTables(ctx context.Context, database string) ([]string, error) {
	query := `
		SELECT table_name
		FROM information_schema.tables
		WHERE table_schema = ? 
		AND table_type IN ('BASE TABLE', 'VIEW')
		ORDER BY table_name`

	rows, err := se.db.QueryContext(ctx, query, database)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var table string
		if err := rows.Scan(&table); err != nil {
			return nil, err
		}

		// Skip tables with obviously problematic names (temp files, backups, etc.)
		// But allow legitimate table names that might contain dots
		if strings.HasPrefix(table, ".") || strings.HasSuffix(table, ".tmp") || strings.HasSuffix(table, ".bak") {
			se.logger.Warn("Skipping table with problematic name", "table", table)
			continue
		}

		tables = append(tables, table)
	}
	return tables, rows.Err()
}

func (se *SchemaExtractor) extractTableSchema(ctx context.Context, database, tableName string) (*Table, error) {
	table := &Table{
		Name:    tableName,
		Columns: make(map[string]*Column),
	}

	// Get columns
	columns, err := se.getColumns(ctx, database, tableName)
	if err != nil {
		return nil, fmt.Errorf("failed to get columns: %w", err)
	}
	table.Columns = columns

	// Get primary keys
	pks, err := se.getPrimaryKeys(ctx, database, tableName)
	if err != nil {
		se.logger.Warn("Failed to get primary keys", "table", tableName, "error", err)
	} else {
		table.PrimaryKeys = pks
	}

	// Get row count
	rowCount, err := se.getRowCount(ctx, tableName)
	if err != nil {
		se.logger.Warn("Failed to get row count", "table", tableName, "error", err)
	} else {
		table.RowCount = rowCount
	}

	// Get table engine
	engine, err := se.getTableEngine(ctx, database, tableName)
	if err != nil {
		se.logger.Warn("Failed to get table engine", "table", tableName, "error", err)
	} else {
		table.Engine = engine
	}

	return table, nil
}

func (se *SchemaExtractor) getColumns(ctx context.Context, database, tableName string) (map[string]*Column, error) {
	query := `
		SELECT 
			column_name, 
			data_type, 
			is_nullable, 
			COALESCE(column_default, '') as column_default,
			CASE WHEN extra = 'auto_increment' THEN 1 ELSE 0 END as auto_increment,
			COALESCE(column_comment, '') as column_comment
		FROM information_schema.columns
		WHERE table_schema = ? AND table_name = ?
		ORDER BY ordinal_position`

	rows, err := se.db.QueryContext(ctx, query, database, tableName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	columns := make(map[string]*Column)
	for rows.Next() {
		var col Column
		var nullable string
		var autoInc int

		err := rows.Scan(&col.Name, &col.Type, &nullable, &col.Default, &autoInc, &col.Comment)
		if err != nil {
			return nil, err
		}

		col.Nullable = nullable == "YES"
		col.AutoIncrement = autoInc == 1

		columns[col.Name] = &col
	}

	return columns, rows.Err()
}

func (se *SchemaExtractor) getPrimaryKeys(ctx context.Context, database, tableName string) ([]string, error) {
	query := `
		SELECT column_name
		FROM information_schema.key_column_usage
		WHERE table_schema = ? AND table_name = ? AND constraint_name = 'PRIMARY'
		ORDER BY ordinal_position`

	rows, err := se.db.QueryContext(ctx, query, database, tableName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var pks []string
	for rows.Next() {
		var pk string
		if err := rows.Scan(&pk); err != nil {
			return nil, err
		}
		pks = append(pks, pk)
	}

	return pks, rows.Err()
}

func (se *SchemaExtractor) getRowCount(ctx context.Context, tableName string) (int64, error) {
	query := fmt.Sprintf("SELECT COUNT(*) FROM `%s`", tableName)

	var count int64
	err := se.db.QueryRowContext(ctx, query).Scan(&count)
	return count, err
}

func (se *SchemaExtractor) getTableEngine(ctx context.Context, database, tableName string) (string, error) {
	query := `
		SELECT engine
		FROM information_schema.tables
		WHERE table_schema = ? AND table_name = ?`

	var engine string
	err := se.db.QueryRowContext(ctx, query, database, tableName).Scan(&engine)
	return engine, err
}

func (se *SchemaExtractor) getForeignKeys(ctx context.Context, database string) ([]ForeignKey, error) {
	query := `
		SELECT 
			kcu.constraint_name, 
			kcu.table_name, 
			kcu.column_name,
			kcu.referenced_table_name, 
			kcu.referenced_column_name,
			COALESCE(rc.update_rule, '') as update_rule,
			COALESCE(rc.delete_rule, '') as delete_rule
		FROM information_schema.key_column_usage kcu
		LEFT JOIN information_schema.referential_constraints rc 
			ON kcu.constraint_name = rc.constraint_name 
			AND kcu.table_schema = rc.constraint_schema
		WHERE kcu.table_schema = ? 
		AND kcu.referenced_table_name IS NOT NULL
		ORDER BY kcu.table_name, kcu.ordinal_position`

	rows, err := se.db.QueryContext(ctx, query, database)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var fks []ForeignKey
	for rows.Next() {
		var fk ForeignKey
		err := rows.Scan(&fk.ConstraintName, &fk.TableName, &fk.ColumnName,
			&fk.RefTableName, &fk.RefColumnName, &fk.UpdateRule, &fk.DeleteRule)
		if err != nil {
			return nil, err
		}
		fks = append(fks, fk)
	}

	return fks, rows.Err()
}

func (se *SchemaExtractor) getIndexes(ctx context.Context, database string) (map[string][]Index, error) {
	query := `
		SELECT 
			table_name,
			index_name,
			column_name,
			non_unique,
			index_type
		FROM information_schema.statistics
		WHERE table_schema = ?
		ORDER BY table_name, index_name, seq_in_index`

	rows, err := se.db.QueryContext(ctx, query, database)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	indexMap := make(map[string]map[string]*Index)

	for rows.Next() {
		var tableName, indexName, columnName, indexType string
		var nonUnique int

		err := rows.Scan(&tableName, &indexName, &columnName, &nonUnique, &indexType)
		if err != nil {
			return nil, err
		}

		if indexMap[tableName] == nil {
			indexMap[tableName] = make(map[string]*Index)
		}

		if indexMap[tableName][indexName] == nil {
			indexMap[tableName][indexName] = &Index{
				Name:      indexName,
				TableName: tableName,
				Unique:    nonUnique == 0,
				Type:      indexType,
			}
		}

		indexMap[tableName][indexName].Columns = append(indexMap[tableName][indexName].Columns, columnName)
	}

	// Convert to final format
	result := make(map[string][]Index)
	for tableName, indexes := range indexMap {
		for _, index := range indexes {
			result[tableName] = append(result[tableName], *index)
		}
	}

	return result, rows.Err()
}

// MySQLToDgraphType converts MySQL data types to Dgraph types
func MySQLToDgraphType(mysqlType string) string {
	mysqlType = strings.ToLower(mysqlType)

	switch {
	case strings.Contains(mysqlType, "int") || strings.Contains(mysqlType, "bigint") ||
		strings.Contains(mysqlType, "smallint") || strings.Contains(mysqlType, "mediumint"):
		return "int"
	case strings.Contains(mysqlType, "float") || strings.Contains(mysqlType, "double") ||
		strings.Contains(mysqlType, "decimal"):
		return "float"
	case strings.Contains(mysqlType, "bool") || mysqlType == "tinyint(1)":
		return "bool"
	case mysqlType == "date":
		return "datetime"
	case strings.Contains(mysqlType, "datetime") || strings.Contains(mysqlType, "timestamp"):
		return "datetime"
	case strings.Contains(mysqlType, "json"):
		return "string" // JSON stored as string in Dgraph
	default:
		return "string"
	}
}

// IsForeignKey checks if a column is likely a foreign key based on naming conventions
func IsForeignKey(columnName string) bool {
	columnName = strings.ToLower(columnName)

	// Common foreign key naming patterns:
	// 1. *_id (most common): user_id, customer_id, etc.
	// 2. *_key: user_key, customer_key, etc.
	// 3. *_ref: user_ref, customer_ref, etc.
	// 4. id_* (less common): id_user, id_customer, etc.
	// 5. fk_*: fk_user, fk_customer, etc.

	// Exclude primary key columns
	if columnName == "id" {
		return false
	}

	return strings.HasSuffix(columnName, "_id") ||
		strings.HasSuffix(columnName, "_key") ||
		strings.HasSuffix(columnName, "_ref") ||
		strings.HasPrefix(columnName, "id_") ||
		strings.HasPrefix(columnName, "fk_")
}

// DetectForeignKeysByConvention detects foreign keys based on naming conventions and table existence
func (se *SchemaExtractor) DetectForeignKeysByConvention(ctx context.Context, schema *Schema) []ForeignKey {
	var conventionFKs []ForeignKey

	// Get list of existing tables for reference checking
	existingTables := make(map[string]bool)
	for tableName := range schema.Tables {
		existingTables[tableName] = true
	}

	for tableName, table := range schema.Tables {
		se.logger.Debug("Checking table for convention FKs", "table", tableName, "columns", len(table.Columns))
		for columnName := range table.Columns {
			if IsForeignKey(columnName) {
				se.logger.Debug("Found potential FK column", "table", tableName, "column", columnName)

				// Try to infer the referenced table name using generic conventions
				var baseName string

				// Extract base name based on different FK naming patterns
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
				var referencedTable string
				var referencedColumn = "id" // Default assumption

				// Generic foreign key detection strategies:
				// 1. Direct table name match (user_id -> users, customer_id -> customers)
				// 2. Singular to plural conversion (category_id -> categories)
				// 3. Handle compound table names with common prefixes/suffixes
				// 4. Handle self-referential foreign keys (parent_id in same table)
				// 5. Try with/without common prefixes found in database

				candidates := []string{
					baseName,         // Direct match: user_id -> user
					baseName + "s",   // Plural: user_id -> users
					baseName + "es",  // Plural with 'es': category_id -> categories
					baseName + "ies", // Plural with 'ies': company_id -> companies
				}

				// Handle self-referential foreign keys
				if baseName == "parent" || baseName == "original" || columnName == tableName+"_id" {
					candidates = append(candidates, tableName)
				}

				// Detect common prefixes in the database to handle prefixed table names
				commonPrefixes := se.detectCommonTablePrefixes(existingTables)
				for _, prefix := range commonPrefixes {
					candidates = append(candidates,
						prefix+baseName,
						prefix+baseName+"s",
						prefix+baseName+"es",
					)
				}

				// For compound table names, try removing common suffixes and adding back
				if strings.Contains(baseName, "_") {
					parts := strings.Split(baseName, "_")
					if len(parts) > 1 {
						// Try different combinations for compound names
						lastPart := parts[len(parts)-1]
						withoutLast := strings.Join(parts[:len(parts)-1], "_")

						candidates = append(candidates,
							withoutLast+"_"+lastPart+"s",  // user_profile_id -> user_profiles
							withoutLast+"_"+lastPart+"es", // user_profile_id -> user_profilees (rare but possible)
							strings.Join(parts, "_")+"s",  // full name + s
						)

						// Also try with detected prefixes
						for _, prefix := range commonPrefixes {
							candidates = append(candidates,
								prefix+withoutLast+"_"+lastPart+"s",
								prefix+strings.Join(parts, "_")+"s",
							)
						}
					}
				}

				// Try to find existing table that matches any candidate
				for _, candidate := range candidates {
					if existingTables[candidate] {
						referencedTable = candidate
						break
					}
				}

				// If we found a potential reference, create FK relationship
				if referencedTable != "" {
					fk := ForeignKey{
						ConstraintName: fmt.Sprintf("fk_%s_%s", tableName, columnName),
						TableName:      tableName,
						ColumnName:     columnName,
						RefTableName:   referencedTable,
						RefColumnName:  referencedColumn,
						UpdateRule:     "CASCADE",
						DeleteRule:     "RESTRICT",
					}
					conventionFKs = append(conventionFKs, fk)

					se.logger.Info("Detected foreign key by convention",
						"table", tableName,
						"column", columnName,
						"references", fmt.Sprintf("%s.%s", referencedTable, referencedColumn))
				} else {
					se.logger.Debug("Could not find referenced table for potential FK",
						"table", tableName,
						"column", columnName,
						"tried_candidates", candidates)
				}
			}
		}
	}

	return conventionFKs
}

// detectCommonTablePrefixes analyzes table names to find common prefixes
// This helps detect foreign keys in databases with prefixed table names (e.g., "app_users", "app_posts")
func (se *SchemaExtractor) detectCommonTablePrefixes(existingTables map[string]bool) []string {
	prefixCount := make(map[string]int)

	// Extract potential prefixes from table names
	for tableName := range existingTables {
		if strings.Contains(tableName, "_") {
			parts := strings.Split(tableName, "_")
			if len(parts) >= 2 {
				// Consider first part as potential prefix
				prefix := parts[0] + "_"
				prefixCount[prefix]++
			}
		}
	}

	// Only consider prefixes that appear in multiple tables (at least 2)
	var commonPrefixes []string
	for prefix, count := range prefixCount {
		if count >= 2 {
			commonPrefixes = append(commonPrefixes, prefix)
		}
	}

	return commonPrefixes
}
