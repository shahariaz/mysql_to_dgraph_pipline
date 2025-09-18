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

type BatchData struct {
	Set []DgraphNode `json:"set"`
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
	query := `
		SELECT constraint_name, table_name, column_name,
		       referenced_table_name, referenced_column_name
		FROM information_schema.key_column_usage
		WHERE table_schema = ? AND referenced_table_name IS NOT NULL
		ORDER BY table_name, column_name
	`
	rows, err := db.Query(query, database)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var fks []ForeignKey
	for rows.Next() {
		var fk ForeignKey
		if err := rows.Scan(&fk.ConstraintName, &fk.TableName, &fk.ColumnName, &fk.RefTableName, &fk.RefColumnName); err != nil {
			return nil, err
		}
		fks = append(fks, fk)
	}
	return fks, nil
}

// ---------------- Type Conversion ----------------

func mysqlToDgraphType(mysqlType string) string {
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
		return "date"
	case strings.Contains(mysqlType, "datetime") || strings.Contains(mysqlType, "timestamp"):
		return "dateTime"
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
		return strconv.ParseBool(strings.TrimSpace(value))
	case "date":
		// Try multiple date formats
		formats := []string{"2006-01-02", "2006/01/02", "02-01-2006", "02/01/2006"}
		for _, format := range formats {
			if t, err := time.Parse(format, strings.TrimSpace(value)); err == nil {
				return t.Format("2006-01-02"), nil
			}
		}
		return value, nil // Return as string if parsing fails
	case "dateTime":
		// Try multiple datetime formats
		formats := []string{
			"2006-01-02 15:04:05",
			"2006-01-02T15:04:05Z",
			"2006-01-02T15:04:05",
			"2006/01/02 15:04:05",
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
		cleaned = strings.ReplaceAll(cleaned, "\"", "\\\"")
		cleaned = strings.ReplaceAll(cleaned, "\n", "\\n")
		cleaned = strings.ReplaceAll(cleaned, "\r", "\\r")
		cleaned = strings.ReplaceAll(cleaned, "\t", "\\t")
		return cleaned, nil
	}
}

// ---------------- UID Generation ----------------

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
	uid := fmt.Sprintf("_:%s_%s", tableName, originalID)
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

// ---------------- Dgraph JSON Export ----------------

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
	fmt.Println("🔍 Creating UID mappings...")
	for _, table := range schema.Tables {
		// Validate table name before querying
		if strings.Contains(table.Name, ".sql") || strings.TrimSpace(table.Name) == "" {
			fmt.Printf("⚠️  Skipping invalid table: %s\n", table.Name)
			continue
		}

		fmt.Printf("🔍 Creating UIDs for table: %s\n", table.Name)

		// Use backticks to handle table names with special characters
		query := fmt.Sprintf("SELECT * FROM `%s` LIMIT 1", table.Name)
		rows, err := db.Query(query)
		if err != nil {
			fmt.Printf("❌ Error querying table %s: %v\n", table.Name, err)
			continue
		}

		cols, err := rows.Columns()
		if err != nil {
			rows.Close()
			fmt.Printf("❌ Error getting columns for table %s: %v\n", table.Name, err)
			continue
		}
		rows.Close()

		// Now get all records for UID mapping
		query = fmt.Sprintf("SELECT * FROM `%s`", table.Name)
		rows, err = db.Query(query)
		if err != nil {
			fmt.Printf("❌ Error querying all records from table %s: %v\n", table.Name, err)
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
				fmt.Printf("❌ Error scanning row in table %s: %v\n", table.Name, err)
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

			if pk == "" {
				// Use first column as fallback
				pk = string(values[0])
			}

			if pk != "" && strings.TrimSpace(pk) != "" {
				uidManager.GetUID(table.Name, pk)
				recordCount++
			}
		}
		rows.Close()
		fmt.Printf("✅ Created UIDs for %d records in table %s\n", recordCount, table.Name)
	}

	// Save UID mapping
	if err := uidManager.SaveMapping(fmt.Sprintf("%s/uid_mapping.json", outputDir)); err != nil {
		return err
	}

	// Second pass: Export data in batches
	fmt.Println("📦 Exporting data in batches...")
	totalBatches := 0

	for _, table := range schema.Tables {
		// Skip invalid table names
		if strings.Contains(table.Name, ".sql") || strings.TrimSpace(table.Name) == "" {
			fmt.Printf("⚠️  Skipping invalid table: %s\n", table.Name)
			continue
		}

		fmt.Printf("Processing table: %s\n", table.Name)

		count, err := getRowCount(db, table.Name)
		if err != nil {
			fmt.Printf("❌ Error getting row count for table %s: %v\n", table.Name, err)
			continue
		}

		if count == 0 {
			fmt.Printf("  ⚠️  Table %s is empty, skipping\n", table.Name)
			continue
		}

		fmt.Printf("  📊 Total rows: %d\n", count)

		// Process in batches
		for offset := int64(0); offset < count; offset += int64(batchSize) {
			batch := BatchData{Set: []DgraphNode{}}

			query := fmt.Sprintf("SELECT * FROM `%s` LIMIT %d OFFSET %d", table.Name, batchSize, offset)
			rows, err := db.Query(query)
			if err != nil {
				fmt.Printf("❌ Error querying batch from table %s: %v\n", table.Name, err)
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

				if pk == "" {
					pk = string(values[0])
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
					predicate := fmt.Sprintf("%s.%s", table.Name, col)

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

					if isFK {
						// Reference to another node
						refUID := uidManager.GetUID(refTable, val)
						node.Data[predicate] = map[string]string{"uid": refUID}
					} else {
						// Convert value based on type
						convertedVal, err := convertValue(val, mysqlType)
						if err != nil {
							fmt.Printf("    ⚠️  Warning: Failed to convert value '%s' for column '%s': %v\n", val, col, err)
							// Use original value as string
							node.Data[predicate] = val
						} else if convertedVal != nil {
							node.Data[predicate] = convertedVal
						}
					}
				}

				batch.Set = append(batch.Set, node)
			}
			rows.Close()

			// Save batch
			if len(batch.Set) > 0 {
				batchNum := totalBatches + 1
				filename := fmt.Sprintf("%s/batch_%04d_%s.json", outputDir, batchNum, table.Name)

				f, err := os.Create(filename)
				if err != nil {
					return err
				}

				encoder := json.NewEncoder(f)
				encoder.SetIndent("", "  ")
				if err := encoder.Encode(batch); err != nil {
					f.Close()
					return err
				}
				f.Close()

				fmt.Printf("  ✅ Batch %d saved: %s (%d records)\n", batchNum, filename, len(batch.Set))
				totalBatches++
			}
		}
	}

	fmt.Printf("🎉 Export completed! Total batches: %d\n", totalBatches)
	return nil
}

// ---------------- Schema Export ----------------

func exportDgraphSchema(schema Schema, file string) error {
	f, err := os.Create(file)
	if err != nil {
		return err
	}
	defer f.Close()

	fmt.Fprintf(f, "# Dgraph Schema Generated from MySQL\n")
	fmt.Fprintf(f, "# Database: %s\n", schema.Database)
	fmt.Fprintf(f, "# Generated: %s\n\n", time.Now().Format(time.RFC3339))

	// Define types
	for _, t := range schema.Tables {
		fmt.Fprintf(f, "type %s {\n", t.Name)

		// Regular columns
		for col, typ := range t.Columns {
			dgraphType := mysqlToDgraphType(typ)
			fmt.Fprintf(f, "  %s.%s: %s\n", t.Name, col, dgraphType)
		}

		// Foreign key relationships
		for _, fk := range schema.Relationships {
			if fk.TableName == t.Name {
				fmt.Fprintf(f, "  %s.%s: uid\n", t.Name, fk.ColumnName)
			}
		}

		fmt.Fprintf(f, "}\n\n")
	}

	// Define predicates with indexes
	fmt.Fprintf(f, "# Predicates with indexes\n")
	for _, t := range schema.Tables {
		for col, typ := range t.Columns {
			dgraphType := mysqlToDgraphType(typ)
			predicate := fmt.Sprintf("%s.%s", t.Name, col)

			var index string
			switch dgraphType {
			case "string":
				index = " @index(exact, term)"
			case "int":
				index = " @index(int)"
			case "float":
				index = " @index(float)"
			case "dateTime":
				index = " @index(hour)"
			case "date":
				index = " @index(day)"
			case "bool":
				index = " @index(bool)"
			}

			fmt.Fprintf(f, "%s: %s%s .\n", predicate, dgraphType, index)
		}
	}

	// Foreign key predicates
	fmt.Fprintf(f, "\n# Foreign key relationships\n")
	for _, fk := range schema.Relationships {
		fmt.Fprintf(f, "%s.%s: uid @reverse .\n", fk.TableName, fk.ColumnName)
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

	// Batch configuration
	batchSize := 90000 // Records per batch
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
	fmt.Println("✅ Connected to MySQL database")

	// Extract schema
	fmt.Println("🔍 Extracting database schema...")
	schema := Schema{
		Database: database,
		Tables:   make(map[string]*Table),
	}

	tables, err := getTables(db, database)
	if err != nil {
		log.Fatal("Failed to get tables:", err)
	}

	fmt.Printf("📋 Found tables: %v\n", tables)

	for _, tableName := range tables {
		// Validate table name before processing
		if strings.Contains(tableName, ".sql") || strings.TrimSpace(tableName) == "" {
			fmt.Printf("⚠️  Skipping invalid table name: %s\n", tableName)
			continue
		}

		fmt.Printf("🔍 Processing table: %s\n", tableName)
		cols, err := getColumns(db, database, tableName)
		if err != nil {
			fmt.Printf("❌ Failed to get columns for table %s: %v\n", tableName, err)
			continue
		}
		schema.Tables[tableName] = &Table{Name: tableName, Columns: cols}
	}

	fks, err := getForeignKeys(db, database)
	if err != nil {
		log.Fatal("Failed to get foreign keys:", err)
	}
	schema.Relationships = fks

	fmt.Printf("✅ Schema extracted: %d tables, %d relationships\n", len(schema.Tables), len(schema.Relationships))

	// Save schema JSON
	schemaFile := fmt.Sprintf("%s/schema.json", outputDir)
	os.MkdirAll(outputDir, 0755)
	f, err := os.Create(schemaFile)
	if err != nil {
		log.Fatal("Failed to create schema file:", err)
	}
	encoder := json.NewEncoder(f)
	encoder.SetIndent("", "  ")
	encoder.Encode(schema)
	f.Close()
	fmt.Printf("✅ MySQL schema saved to %s\n", schemaFile)

	// Export Dgraph schema
	dgraphSchemaFile := fmt.Sprintf("%s/schema.dgraph", outputDir)
	if err := exportDgraphSchema(schema, dgraphSchemaFile); err != nil {
		log.Fatal("Failed to export Dgraph schema:", err)
	}
	fmt.Printf("✅ Dgraph schema saved to %s\n", dgraphSchemaFile)

	// Export data to Dgraph JSON format
	if err := exportToDgraphJSON(db, schema, batchSize, outputDir); err != nil {
		log.Fatal("Failed to export data:", err)
	}

	fmt.Printf("\n🎉 Export completed successfully!\n")
	fmt.Printf("📁 Output directory: %s\n", outputDir)
	fmt.Printf("📋 Files generated:\n")
	fmt.Printf("  - schema.json (MySQL schema)\n")
	fmt.Printf("  - schema.dgraph (Dgraph schema)\n")
	fmt.Printf("  - uid_mapping.json (UID mappings)\n")
	fmt.Printf("  - batch_XXXX_TABLENAME.json (data batches)\n")
	fmt.Printf("\n💡 To import into Dgraph:\n")
	fmt.Printf("1. First load the schema: dgraph live -s schema.dgraph\n")
	fmt.Printf("2. Then import each batch: dgraph live -f batch_XXXX_TABLENAME.json\n")
}
