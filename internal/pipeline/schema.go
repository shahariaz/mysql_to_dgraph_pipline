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
	return strings.HasSuffix(columnName, "_id") && columnName != "id"
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
				// Try to infer the referenced table name
				baseName := strings.TrimSuffix(strings.ToLower(columnName), "_id")

				var referencedTable string
				var referencedColumn = "id" // Default assumption

				// Try different table name patterns based on actual data patterns
				candidates := []string{
					// Direct patterns from actual database
					"chorki_" + baseName,        // meta_id -> chorki_metas (but we need chorki_metas)
					"chorki_" + baseName + "s",  // meta_id -> chorki_metas, series_id -> chorki_series
					"chorki_" + baseName + "es", // video_id -> chorki_videos (special case)

					// Handle special cases observed in the data
					func() string {
						switch baseName {
						case "meta":
							return "chorki_metas"
						case "series":
							return "chorki_series"
						case "season":
							return "chorki_seasons"
						case "customer":
							return "chorki_customers"
						case "video":
							return "chorki_videos"
						case "stream":
							return "chorki_streams"
						case "content":
							return "chorki_metas" // content_id likely references metas
						case "profile":
							return "chorki_customers" // profile_id likely references customers
						case "parent":
							return tableName // parent_id is self-referential
						case "original":
							return tableName // original_id is self-referential
						case "seo_meta":
							return "chorki_metas" // seo_meta_id references chorki_metas
						case "ad_campaign":
							return "chorki_metas" // ad_campaign_id references chorki_metas (or could be self-ref)
						default:
							return ""
						}
					}(),

					// Generic patterns as fallback
					baseName + "s", // user_id -> users
					baseName,       // seo_meta -> seo_meta
				}

				for _, candidate := range candidates {
					if candidate != "" && existingTables[candidate] {
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
						"base_name", baseName)
				}
			}
		}
	}

	return conventionFKs
}
