package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

// ---------------- Schema Structs ----------------

type Schema struct {
	Database      string            `json:"database"`
	Tables        map[string]*Table `json:"tables"`
	Relationships []ForeignKey      `json:"relationships"`
}

type Table struct {
	Name    string            `json:"name"`
	Columns map[string]string `json:"columns"`
}

type ForeignKey struct {
	ConstraintName string `json:"constraint_name"`
	TableName      string `json:"table_name"`
	ColumnName     string `json:"column_name"`
	RefTableName   string `json:"referenced_table_name"`
	RefColumnName  string `json:"referenced_column_name"`
}

// ---------------- Dgraph JSON Structs ----------------

type DgraphNode struct {
	UID        string                 `json:"uid,omitempty"`
	DgraphType []string               `json:"dgraph.type,omitempty"`
	Data       map[string]interface{} `json:"-"`
}

// Custom marshaler to flatten the structure
func (d DgraphNode) MarshalJSON() ([]byte, error) {
	result := make(map[string]interface{})

	if d.UID != "" {
		result["uid"] = d.UID
	}

	if len(d.DgraphType) > 0 {
		result["dgraph.type"] = d.DgraphType
	}

	for k, v := range d.Data {
		result[k] = v
	}

	return json.Marshal(result)
}

// ---------------- Schema Extraction ----------------

func getTables(db *sql.DB, database string) ([]string, error) {
	query := `
		SELECT table_name
		FROM information_schema.tables
		WHERE table_schema = ? 
		AND table_type = 'BASE TABLE'
		AND table_name NOT LIKE '%.sql'
		AND table_name NOT LIKE '%_backup'
		AND table_name NOT LIKE '%_temp'
		ORDER BY table_name
	`
	rows, err := db.Query(query, database)
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
		// Additional safety check to filter out invalid table names
		if !strings.Contains(table, ".sql") &&
			!strings.Contains(table, " ") &&
			len(strings.TrimSpace(table)) > 0 {
			tables = append(tables, strings.TrimSpace(table))
		}
	}
	return tables, nil
}

func getColumns(db *sql.DB, database, table string) (map[string]string, error) {
	query := `
		SELECT column_name, data_type, is_nullable, column_default
		FROM information_schema.columns
		WHERE table_schema = ? AND table_name = ?
		ORDER BY ordinal_position
	`
	rows, err := db.Query(query, database, table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	columns := make(map[string]string)
	for rows.Next() {
		var colName, colType string
		var isNullable, columnDefault sql.NullString
		if err := rows.Scan(&colName, &colType, &isNullable, &columnDefault); err != nil {
			return nil, err
		}
		columns[colName] = colType
	}
	return columns, nil
}

func getForeignKeys(db *sql.DB, database string) ([]ForeignKey, error) {
	// Try multiple queries to detect foreign keys
	queries := []string{
		// Standard foreign key constraints
		`SELECT constraint_name, table_name, column_name,
		        referenced_table_name, referenced_column_name
		 FROM information_schema.key_column_usage
		 WHERE table_schema = ? AND referenced_table_name IS NOT NULL
		 ORDER BY table_name, column_name`,

		// Alternative query for some MySQL versions
		`SELECT CONSTRAINT_NAME, TABLE_NAME, COLUMN_NAME,
		        REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME
		 FROM information_schema.REFERENTIAL_CONSTRAINTS rc
		 JOIN information_schema.KEY_COLUMN_USAGE kcu 
		 ON rc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
		 WHERE rc.CONSTRAINT_SCHEMA = ? AND kcu.TABLE_SCHEMA = ?
		 ORDER BY kcu.TABLE_NAME, kcu.COLUMN_NAME`,
	}

	var fks []ForeignKey

	for i, query := range queries {
		var rows *sql.Rows
		var err error

		if i == 0 {
			rows, err = db.Query(query, database)
		} else {
			rows, err = db.Query(query, database, database)
		}

		if err != nil {
			fmt.Printf("Query %d failed: %v\n", i+1, err)
			continue
		}

		for rows.Next() {
			var fk ForeignKey
			if err := rows.Scan(&fk.ConstraintName, &fk.TableName, &fk.ColumnName, &fk.RefTableName, &fk.RefColumnName); err != nil {
				continue
			}
			fks = append(fks, fk)
		}
		rows.Close()

		if len(fks) > 0 {
			break // Found foreign keys, no need to try other queries
		}
	}

	// If no foreign keys found through constraints, try to infer them from naming conventions
	if len(fks) == 0 {
		inferredFKs, err := inferForeignKeys(db, database)
		if err == nil {
			fks = append(fks, inferredFKs...)
		}
	}

	return fks, nil
}

// Infer foreign keys based on naming conventions
func inferForeignKeys(db *sql.DB, database string) ([]ForeignKey, error) {
	var fks []ForeignKey

	// Get all tables and their columns
	tables, err := getTables(db, database)
	if err != nil {
		return nil, err
	}

	tableColumns := make(map[string]map[string]string)
	for _, table := range tables {
		cols, err := getColumns(db, database, table)
		if err != nil {
			continue
		}
		tableColumns[table] = cols
	}

	// Create a map of table names for easier matching
	tableNameMap := make(map[string]string)
	for _, table := range tables {
		tableNameMap[strings.ToLower(table)] = table

		// Also try without prefix (e.g., chorki_series -> series)
		parts := strings.Split(strings.ToLower(table), "_")
		if len(parts) > 1 {
			suffix := strings.Join(parts[1:], "_")
			tableNameMap[suffix] = table

			// Also try singular/plural variations
			if strings.HasSuffix(suffix, "s") {
				tableNameMap[strings.TrimSuffix(suffix, "s")] = table
			} else {
				tableNameMap[suffix+"s"] = table
			}
		}
	}

	fmt.Printf("Inferring relationships from naming patterns...\n")

	// Look for foreign key patterns
	for tableName, columns := range tableColumns {
		for colName := range columns {
			colLower := strings.ToLower(colName)

			// Pattern 1: column ends with _id
			if strings.HasSuffix(colLower, "_id") && colLower != "id" {
				refTable := strings.TrimSuffix(colLower, "_id")

				// Try different variations to find the referenced table
				possibleRefs := []string{
					refTable,
					refTable + "s",
					strings.TrimSuffix(refTable, "s"),
				}

				// Add prefixed versions (e.g., series_id -> chorki_series)
				if strings.Contains(tableName, "_") {
					prefix := strings.Split(tableName, "_")[0]
					for _, ref := range []string{refTable, refTable + "s", strings.TrimSuffix(refTable, "s")} {
						possibleRefs = append(possibleRefs, prefix+"_"+ref)
					}
				}

				for _, possibleRef := range possibleRefs {
					if actualTable, exists := tableNameMap[possibleRef]; exists {
						fk := ForeignKey{
							ConstraintName: fmt.Sprintf("inferred_fk_%s_%s", tableName, colName),
							TableName:      tableName,
							ColumnName:     colName,
							RefTableName:   actualTable,
							RefColumnName:  "id",
						}
						fks = append(fks, fk)
						fmt.Printf("  Inferred: %s.%s -> %s.id\n", tableName, colName, actualTable)
						break
					}
				}
			}
		}
	}

	return fks, nil
}

// ---------------- Type Conversion (IMPROVED) ----------------

func mysqlToDgraphType(mysqlType string) string {
	mysqlType = strings.ToLower(mysqlType)
	switch {
	case strings.Contains(mysqlType, "tinyint(1)") || strings.Contains(mysqlType, "bool"):
		return "bool"
	case strings.Contains(mysqlType, "int") || strings.Contains(mysqlType, "bigint") ||
		strings.Contains(mysqlType, "smallint") || strings.Contains(mysqlType, "mediumint"):
		return "int"
	case strings.Contains(mysqlType, "float") || strings.Contains(mysqlType, "double") ||
		strings.Contains(mysqlType, "decimal") || strings.Contains(mysqlType, "numeric"):
		return "float"
	case mysqlType == "date":
		return "datetime" // Dgraph uses datetime for dates
	case strings.Contains(mysqlType, "datetime") || strings.Contains(mysqlType, "timestamp"):
		return "datetime"
	case strings.Contains(mysqlType, "text") || strings.Contains(mysqlType, "varchar") ||
		strings.Contains(mysqlType, "char") || strings.Contains(mysqlType, "json"):
		return "string"
	default:
		return "string"
	}
}

func convertValue(value string, mysqlType string) (interface{}, error) {
	if value == "" || strings.ToLower(value) == "null" {
		return nil, nil
	}

	dgraphType := mysqlToDgraphType(mysqlType)

	switch dgraphType {
	case "int":
		return strconv.ParseInt(strings.TrimSpace(value), 10, 64)
	case "float":
		return strconv.ParseFloat(strings.TrimSpace(value), 64)
	case "bool":
		// Handle various boolean representations
		val := strings.ToLower(strings.TrimSpace(value))
		switch val {
		case "1", "true", "yes", "on":
			return true, nil
		case "0", "false", "no", "off":
			return false, nil
		default:
			return strconv.ParseBool(val)
		}
	case "datetime":
		// Try multiple datetime formats
		formats := []string{
			"2006-01-02 15:04:05",
			"2006-01-02T15:04:05Z",
			"2006-01-02T15:04:05",
			"2006/01/02 15:04:05",
			"2006-01-02",
		}
		for _, format := range formats {
			if t, err := time.Parse(format, strings.TrimSpace(value)); err == nil {
				return t.Format(time.RFC3339), nil
			}
		}
		return value, nil // Return as string if parsing fails
	default:
		// Clean string value
		cleaned := strings.TrimSpace(value)
		// Escape special characters for JSON
		cleaned = strings.ReplaceAll(cleaned, "\\", "\\\\")
		cleaned = strings.ReplaceAll(cleaned, "\"", "\\\"")
		cleaned = strings.ReplaceAll(cleaned, "\n", "\\n")
		cleaned = strings.ReplaceAll(cleaned, "\r", "\\r")
		cleaned = strings.ReplaceAll(cleaned, "\t", "\\t")
		return cleaned, nil
	}
}

// ---------------- UID Generation (IMPROVED) ----------------

type UIDManager struct {
	uidMap  map[string]map[string]string // table -> original_id -> uid
	counter int64
}

func NewUIDManager() *UIDManager {
	return &UIDManager{
		uidMap:  make(map[string]map[string]string),
		counter: 1000,
	}
}

func (u *UIDManager) GetUID(tableName, originalID string) string {
	if u.uidMap[tableName] == nil {
		u.uidMap[tableName] = make(map[string]string)
	}

	if uid, exists := u.uidMap[tableName][originalID]; exists {
		return uid
	}

	u.counter++
	// Use blank node syntax for Dgraph
	uid := fmt.Sprintf("_:%s_%s_%d", tableName, originalID, u.counter)
	u.uidMap[tableName][originalID] = uid
	return uid
}

func (u *UIDManager) SaveMapping(filename string) error {
	f, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer f.Close()

	encoder := json.NewEncoder(f)
	encoder.SetIndent("", "  ")
	return encoder.Encode(u.uidMap)
}

// ---------------- Dgraph JSON Export (IMPROVED) ----------------

func getRowCount(db *sql.DB, tableName string) (int64, error) {
	var count int64
	query := fmt.Sprintf("SELECT COUNT(*) FROM `%s`", tableName)
	err := db.QueryRow(query).Scan(&count)
	return count, err
}

func exportToDgraphJSON(db *sql.DB, schema Schema, batchSize int, outputDir string) error {
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return err
	}

	uidManager := NewUIDManager()

	// First pass: Create UID mappings for all primary keys
	fmt.Println("Creating UID mappings...")
	for _, table := range schema.Tables {
		if strings.Contains(table.Name, ".sql") || strings.TrimSpace(table.Name) == "" {
			fmt.Printf("Skipping invalid table: %s\n", table.Name)
			continue
		}

		fmt.Printf("Creating UIDs for table: %s\n", table.Name)

		// Find primary key column
		query := fmt.Sprintf("SELECT * FROM `%s` LIMIT 1", table.Name)
		rows, err := db.Query(query)
		if err != nil {
			fmt.Printf("Error querying table %s: %v\n", table.Name, err)
			continue
		}

		cols, err := rows.Columns()
		if err != nil {
			rows.Close()
			fmt.Printf("Error getting columns for table %s: %v\n", table.Name, err)
			continue
		}
		rows.Close()

		// Get all records for UID mapping
		query = fmt.Sprintf("SELECT * FROM `%s`", table.Name)
		rows, err = db.Query(query)
		if err != nil {
			fmt.Printf("Error querying all records from table %s: %v\n", table.Name, err)
			continue
		}

		values := make([]sql.RawBytes, len(cols))
		scanArgs := make([]interface{}, len(cols))
		for i := range values {
			scanArgs[i] = &values[i]
		}

		recordCount := 0
		for rows.Next() {
			if err := rows.Scan(scanArgs...); err != nil {
				fmt.Printf("Error scanning row in table %s: %v\n", table.Name, err)
				continue
			}

			// Find primary key
			var pk string
			for i, col := range cols {
				if strings.ToLower(col) == "id" || strings.HasSuffix(strings.ToLower(col), "_id") {
					pk = string(values[i])
					break
				}
			}

			if pk == "" && len(values) > 0 {
				// Use first column as fallback
				pk = string(values[0])
			}

			if pk != "" && strings.TrimSpace(pk) != "" && strings.ToLower(pk) != "null" {
				uidManager.GetUID(table.Name, pk)
				recordCount++
			}
		}
		rows.Close()
		fmt.Printf("Created UIDs for %d records in table %s\n", recordCount, table.Name)
	}

	// Save UID mapping
	if err := uidManager.SaveMapping(fmt.Sprintf("%s/uid_mapping.json", outputDir)); err != nil {
		return err
	}

	// Second pass: Export data in batches
	fmt.Println("Exporting data in batches...")
	totalBatches := 0

	for _, table := range schema.Tables {
		if strings.Contains(table.Name, ".sql") || strings.TrimSpace(table.Name) == "" {
			fmt.Printf("Skipping invalid table: %s\n", table.Name)
			continue
		}

		fmt.Printf("Processing table: %s\n", table.Name)

		count, err := getRowCount(db, table.Name)
		if err != nil {
			fmt.Printf("Error getting row count for table %s: %v\n", table.Name, err)
			continue
		}

		if count == 0 {
			fmt.Printf("  Table %s is empty, skipping\n", table.Name)
			continue
		}

		fmt.Printf("  Total rows: %d\n", count)

		// Process in batches
		for offset := int64(0); offset < count; offset += int64(batchSize) {
			var batch []DgraphNode

			query := fmt.Sprintf("SELECT * FROM `%s` LIMIT %d OFFSET %d", table.Name, batchSize, offset)
			rows, err := db.Query(query)
			if err != nil {
				fmt.Printf("Error querying batch from table %s: %v\n", table.Name, err)
				break
			}

			cols, _ := rows.Columns()
			values := make([]sql.RawBytes, len(cols))
			scanArgs := make([]interface{}, len(cols))
			for i := range values {
				scanArgs[i] = &values[i]
			}

			for rows.Next() {
				if err := rows.Scan(scanArgs...); err != nil {
					rows.Close()
					return err
				}

				// Find primary key for UID
				var pk string
				for i, col := range cols {
					if strings.ToLower(col) == "id" || strings.HasSuffix(strings.ToLower(col), "_id") {
						pk = string(values[i])
						break
					}
				}

				if pk == "" && len(values) > 0 {
					pk = string(values[0])
				}

				if pk == "" || strings.ToLower(pk) == "null" {
					continue // Skip records without valid primary key
				}

				uid := uidManager.GetUID(table.Name, pk)

				node := DgraphNode{
					UID:        uid,
					DgraphType: []string{table.Name},
					Data:       make(map[string]interface{}),
				}

				// Process each column
				for i, col := range cols {
					val := string(values[i])
					if val == "" || strings.ToLower(val) == "null" {
						continue
					}

					mysqlType := table.Columns[col]

					// Use simple predicate names without table prefix
					predicate := col

					// Check if this is a foreign key
					isFK := false
					var refTable string
					for _, fk := range schema.Relationships {
						if fk.TableName == table.Name && fk.ColumnName == col {
							isFK = true
							refTable = fk.RefTableName
							break
						}
					}

					if isFK && val != "" && strings.ToLower(val) != "null" {
						// Reference to another node - make sure referenced UID exists
						refUID := uidManager.GetUID(refTable, val)
						node.Data[predicate] = map[string]string{"uid": refUID}
						fmt.Printf("    FK: %s.%s -> %s (UID: %s)\n", table.Name, col, refTable, refUID)
					} else {
						// Convert value based on type
						convertedVal, err := convertValue(val, mysqlType)
						if err != nil {
							fmt.Printf("    Warning: Failed to convert value '%s' for column '%s': %v\n", val, col, err)
							// Use original value as string
							node.Data[predicate] = val
						} else if convertedVal != nil {
							node.Data[predicate] = convertedVal
						}
					}
				}

				batch = append(batch, node)
			}
			rows.Close()

			// Save batch - Use proper format for Dgraph
			if len(batch) > 0 {
				batchNum := totalBatches + 1
				filename := fmt.Sprintf("%s/batch_%04d.json", outputDir, batchNum)

				f, err := os.Create(filename)
				if err != nil {
					return err
				}

				encoder := json.NewEncoder(f)
				encoder.SetIndent("", "  ")
				// Wrap in "set" for Dgraph format
				wrapper := map[string][]DgraphNode{"set": batch}
				if err := encoder.Encode(wrapper); err != nil {
					f.Close()
					return err
				}
				f.Close()

				fmt.Printf("  Batch %d saved: %s (%d records)\n", batchNum, filename, len(batch))
				totalBatches++
			}
		}
	}

	fmt.Printf("Export completed! Total batches: %d\n", totalBatches)
	return nil
}

// ---------------- Schema Export (IMPROVED RELATIONSHIPS) ----------------

func exportDgraphSchema(schema Schema, file string) error {
	f, err := os.Create(file)
	if err != nil {
		return err
	}
	defer f.Close()

	// Header comments
	fmt.Fprintf(f, "# Dgraph Schema Generated from MySQL Database: %s\n", schema.Database)
	fmt.Fprintf(f, "# Generated: %s\n", time.Now().Format(time.RFC3339))
	fmt.Fprintf(f, "# Total Tables: %d, Relationships: %d\n\n", len(schema.Tables), len(schema.Relationships))

	// Define types first
	for _, t := range schema.Tables {
		fmt.Fprintf(f, "type %s {\n", t.Name)

		// Add predicates for this type
		for col := range t.Columns {
			fmt.Fprintf(f, "    %s\n", col)
		}

		fmt.Fprintf(f, "}\n\n")
	}

	// Define predicates with proper syntax
	processedPredicates := make(map[string]bool)

	for _, t := range schema.Tables {
		for col, typ := range t.Columns {
			if processedPredicates[col] {
				continue // Skip duplicates
			}

			dgraphType := mysqlToDgraphType(typ)

			// Generate appropriate indexes
			var index string
			switch dgraphType {
			case "string":
				if strings.Contains(strings.ToLower(col), "email") {
					index = " @index(hash)"
				} else if strings.Contains(strings.ToLower(col), "name") ||
					strings.Contains(strings.ToLower(col), "title") {
					index = " @index(term, fulltext)"
				} else {
					index = " @index(exact)"
				}
			case "int":
				index = " @index(int)"
			case "float":
				index = " @index(float)"
			case "datetime":
				index = " @index(hour)"
			case "bool":
				index = " @index(bool)"
			}

			fmt.Fprintf(f, "%s: %s%s .\n", col, dgraphType, index)
			processedPredicates[col] = true
		}
	}

	// Foreign key predicates - IMPROVED
	if len(schema.Relationships) > 0 {
		fmt.Fprintf(f, "\n# Foreign key relationships\n")
		processedFKs := make(map[string]bool)

		for _, fk := range schema.Relationships {
			fkKey := fk.ColumnName
			if processedFKs[fkKey] {
				continue
			}

			// Add comment explaining the relationship
			fmt.Fprintf(f, "# %s.%s -> %s.%s\n",
				fk.TableName, fk.ColumnName, fk.RefTableName, fk.RefColumnName)

			// Check if this FK predicate was already defined as a regular predicate
			if !processedPredicates[fk.ColumnName] {
				fmt.Fprintf(f, "%s: uid @reverse .\n", fk.ColumnName)
			} else {
				// If it was already defined, just add a comment
				fmt.Fprintf(f, "# %s already defined above as uid reference\n", fk.ColumnName)
			}

			processedFKs[fkKey] = true
		}
	} else {
		fmt.Fprintf(f, "\n# No foreign key relationships detected\n")
		fmt.Fprintf(f, "# If you have relationships, they may need to be defined manually\n")
	}

	return nil
}

// ---------------- Main Function ----------------

func main() {
	// Database configuration
	user := "root"
	password := "root"
	host := "127.0.0.1"
	port := "3306"
	database := "dump" // CHANGE THIS

	// Batch configuration - smaller batches for better performance
	batchSize := 1000 // Reduced batch size
	outputDir := "dgraph_export"

	dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", user, password, host, port, database)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Fatal("Failed to connect to database:", err)
	}
	defer db.Close()

	// Test connection
	if err := db.Ping(); err != nil {
		log.Fatal("Failed to ping database:", err)
	}
	fmt.Println("Connected to MySQL database")

	// Extract schema
	fmt.Println("Extracting database schema...")
	schema := Schema{
		Database: database,
		Tables:   make(map[string]*Table),
	}

	tables, err := getTables(db, database)
	if err != nil {
		log.Fatal("Failed to get tables:", err)
	}

	fmt.Printf("Found tables: %v\n", tables)

	for _, tableName := range tables {
		if strings.Contains(tableName, ".sql") || strings.TrimSpace(tableName) == "" {
			fmt.Printf("Skipping invalid table name: %s\n", tableName)
			continue
		}

		fmt.Printf("Processing table: %s\n", tableName)
		cols, err := getColumns(db, database, tableName)
		if err != nil {
			fmt.Printf("Failed to get columns for table %s: %v\n", tableName, err)
			continue
		}
		schema.Tables[tableName] = &Table{Name: tableName, Columns: cols}
	}

	fks, err := getForeignKeys(db, database)
	if err != nil {
		log.Fatal("Failed to get foreign keys:", err)
	}
	schema.Relationships = fks

	// Debug: Print found relationships
	fmt.Printf("Schema extracted: %d tables, %d relationships\n", len(schema.Tables), len(schema.Relationships))
	if len(schema.Relationships) > 0 {
		fmt.Println("Found relationships:")
		for _, fk := range schema.Relationships {
			fmt.Printf("  %s.%s -> %s.%s\n", fk.TableName, fk.ColumnName, fk.RefTableName, fk.RefColumnName)
		}
	} else {
		fmt.Println("No foreign key relationships detected")
		fmt.Println("Will attempt to infer relationships from column naming patterns...")

		// Show potential relationships based on naming
		fmt.Println("Columns that might be foreign keys:")
		for tableName, table := range schema.Tables {
			for colName := range table.Columns {
				colLower := strings.ToLower(colName)
				if strings.HasSuffix(colLower, "_id") && colLower != "id" {
					fmt.Printf("  %s.%s (might reference %s table)\n",
						tableName, colName, strings.TrimSuffix(colLower, "_id"))
				}
			}
		}
	}

	// Create output directory
	os.MkdirAll(outputDir, 0755)

	// Save schema JSON
	schemaFile := fmt.Sprintf("%s/schema.json", outputDir)
	f, err := os.Create(schemaFile)
	if err != nil {
		log.Fatal("Failed to create schema file:", err)
	}
	encoder := json.NewEncoder(f)
	encoder.SetIndent("", "  ")
	encoder.Encode(schema)
	f.Close()
	fmt.Printf("MySQL schema saved to %s\n", schemaFile)

	// Export Dgraph schema - FIXED
	dgraphSchemaFile := fmt.Sprintf("%s/schema.dgraph", outputDir)
	if err := exportDgraphSchema(schema, dgraphSchemaFile); err != nil {
		log.Fatal("Failed to export Dgraph schema:", err)
	}
	fmt.Printf("Dgraph schema saved to %s\n", dgraphSchemaFile)

	// Export data to Dgraph JSON format
	if err := exportToDgraphJSON(db, schema, batchSize, outputDir); err != nil {
		log.Fatal("Failed to export data:", err)
	}

	fmt.Printf("\nExport completed successfully!\n")
	fmt.Printf("Output directory: %s\n", outputDir)
	fmt.Printf("Files generated:\n")
	fmt.Printf("  - schema.json (MySQL schema)\n")
	fmt.Printf("  - schema.dgraph (Dgraph schema)\n")
	fmt.Printf("  - uid_mapping.json (UID mappings)\n")
	fmt.Printf("  - batch_XXXX.json (data batches)\n")
	fmt.Printf("\nTo import into Dgraph:\n")
	fmt.Printf("1. Load schema: dgraph live -s schema.dgraph\n")
	fmt.Printf("2. Import data: dgraph live -f batch_XXXX.json\n")
}
