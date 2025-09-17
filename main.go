package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"

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

// ---------------- Schema Extraction ----------------

func getTables(db *sql.DB, database string) ([]string, error) {
	query := `
		SELECT table_name
		FROM information_schema.tables
		WHERE table_schema = ? 
		AND table_type IN ('BASE TABLE', 'VIEW')
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
		tables = append(tables, table)
	}
	return tables, nil
}

func getColumns(db *sql.DB, database, table string) (map[string]string, error) {
	query := `
		SELECT column_name, data_type
		FROM information_schema.columns
		WHERE table_schema = ? AND table_name = ?
	`
	rows, err := db.Query(query, database, table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	columns := make(map[string]string)
	for rows.Next() {
		var colName, colType string
		if err := rows.Scan(&colName, &colType); err != nil {
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

// ---------------- Dgraph Schema Conversion ----------------

func mysqlToDgraphType(mysqlType string) string {
	switch strings.ToLower(mysqlType) {
	case "int", "bigint", "smallint", "mediumint":
		return "int"
	case "float", "double", "decimal":
		return "float"
	case "bool", "tinyint(1)":
		return "bool"
	case "date":
		return "date"
	case "datetime", "timestamp":
		return "dateTime"
	default:
		return "string"
	}
}

func exportDgraphSchema(schema Schema, file string) error {
	f, err := os.Create(file)
	if err != nil {
		return err
	}
	defer f.Close()

	for _, t := range schema.Tables {
		fmt.Fprintf(f, "type %s {\n", t.Name)
		for col, typ := range t.Columns {
			fmt.Fprintf(f, "  %s.%s: %s\n", fmt.Sprintf("%s.%s", t.Name, col), mysqlToDgraphType(typ))
		}
		fmt.Fprintln(f, "}")
		fmt.Fprintln(f)
	}
	return nil
}

// ---------------- Data Export ----------------

// Export rows as RDF with UID references for FKs
func exportDataRDF(db *sql.DB, schema Schema, file string) error {
	f, err := os.Create(file)
	if err != nil {
		return err
	}
	defer f.Close()

	for _, t := range schema.Tables {
		rows, err := db.Query("SELECT * FROM " + t.Name)
		if err != nil {
			return err
		}
		cols, _ := rows.Columns()
		values := make([]sql.RawBytes, len(cols))
		scanArgs := make([]interface{}, len(cols))
		for i := range values {
			scanArgs[i] = &values[i]
		}

		var uidCounter int64 = 1000

		for rows.Next() {
			err = rows.Scan(scanArgs...)
			if err != nil {
				return err
			}
			uidCounter++
			uid := fmt.Sprintf("_:%s_%d", t.Name, uidCounter)

			for i, col := range cols {
				val := string(values[i])
				if val != "" {
					predicate := fmt.Sprintf("<%s.%s>", t.Name, col)

					// Check if this column is a foreign key
					isFK := false
					var refTable string
					for _, fk := range schema.Relationships {
						if fk.TableName == t.Name && fk.ColumnName == col {
							isFK = true
							refTable = fk.RefTableName
							break
						}
					}

					if isFK {
						// Write UID reference
						refUID := fmt.Sprintf("_:%s_%s", refTable, val)
						fmt.Fprintf(f, "%s %s %s .\n", uid, predicate, refUID)
					} else {
						// Normal scalar value
						fmt.Fprintf(f, "%s %s \"%s\" .\n", uid, predicate, strings.ReplaceAll(val, "\"", "'"))
					}
				}
			}
			fmt.Fprintf(f, "%s <dgraph.type> \"%s\" .\n\n", uid, t.Name)
		}
		rows.Close()
	}
	return nil
}

// Export advanced Dgraph schema with edges
func exportDgraphAdvancedSchema(schema Schema, file string) error {
	f, err := os.Create(file)
	if err != nil {
		return err
	}
	defer f.Close()

	// Build reverse map
	reverseMap := make(map[string][]ForeignKey)
	for _, fk := range schema.Relationships {
		reverseMap[fk.RefTableName] = append(reverseMap[fk.RefTableName], fk)
	}

	// --- Types Section ---
	for _, t := range schema.Tables {
		fmt.Fprintf(f, "# -------------------\n")
		fmt.Fprintf(f, "# %s\n", t.Name)
		fmt.Fprintf(f, "# -------------------\n")
		fmt.Fprintf(f, "type %s {\n", t.Name)

		// Scalars
		for col, typ := range t.Columns {
			dType := mysqlToDgraphType(typ)
			fmt.Fprintf(f, "  %s.%s: %s\n", t.Name, col, dType)
		}

		// Outgoing FKs (many-to-one)
		for _, fk := range schema.Relationships {
			if fk.TableName == t.Name {
				fmt.Fprintf(f, "  %s.%s: %s\n", t.Name, fk.ColumnName, fk.RefTableName)
			}
		}

		// Incoming FKs (one-to-many)
		if fks, ok := reverseMap[t.Name]; ok {
			for _, fk := range fks {
				fmt.Fprintf(f, "  %s.%s: [%s]\n", fk.RefTableName, pluralize(fk.TableName), fk.TableName)
			}
		}

		fmt.Fprintln(f, "}\n")
	}

	// --- Predicates Section ---
	fmt.Fprintf(f, "# ===================\n")
	fmt.Fprintf(f, "# Predicates\n")
	fmt.Fprintf(f, "# ===================\n")

	// Scalars
	for _, t := range schema.Tables {
		for col, typ := range t.Columns {
			dType := mysqlToDgraphType(typ)
			predicate := fmt.Sprintf("%s.%s", t.Name, col)

			// Choose index defaults
			var index string
			switch dType {
			case "string":
				index = "@index(exact)"
			case "int":
				index = "@index(int)"
			case "float":
				index = "@index(float)"
			case "dateTime":
				index = "@index(hour)"
			case "date":
				index = "@index(day)"
			}

			if index != "" {
				fmt.Fprintf(f, "%s: %s %s .\n", predicate, dType, index)
			} else {
				fmt.Fprintf(f, "%s: %s .\n", predicate, dType)
			}
		}
	}

	// Outgoing FK → uid
	for _, fk := range schema.Relationships {
		fmt.Fprintf(f, "%s.%s: uid @reverse .\n", fk.TableName, fk.ColumnName)
	}

	// Incoming FK → [uid]
	for _, fk := range schema.Relationships {
		fmt.Fprintf(f, "%s.%s: [uid] @reverse .\n", fk.RefTableName, pluralize(fk.TableName))
	}

	return nil
}

// Very simple pluralizer
func pluralize(name string) string {
	if strings.HasSuffix(name, "s") {
		return name
	}
	return name + "s"
}

// Export rows as JSON
func exportDataJSON(db *sql.DB, schema Schema, file string) error {
	data := make(map[string][]map[string]string)

	for _, t := range schema.Tables {
		rows, err := db.Query("SELECT * FROM " + t.Name)
		if err != nil {
			return err
		}
		cols, _ := rows.Columns()
		values := make([]sql.RawBytes, len(cols))
		scanArgs := make([]interface{}, len(cols))
		for i := range values {
			scanArgs[i] = &values[i]
		}

		var tableData []map[string]string
		for rows.Next() {
			err = rows.Scan(scanArgs...)
			if err != nil {
				return err
			}
			rowData := make(map[string]string)
			for i, col := range cols {
				rowData[col] = string(values[i])
			}
			tableData = append(tableData, rowData)
		}
		rows.Close()
		data[t.Name] = tableData
	}

	f, err := os.Create(file)
	if err != nil {
		return err
	}
	defer f.Close()

	encoder := json.NewEncoder(f)
	encoder.SetIndent("", "  ")
	return encoder.Encode(data)
}

// ---------------- Main ----------------

func main() {
	user := "root"
	password := "root"
	host := "127.0.0.1"
	port := "3306"
	database := "dump" // CHANGE THIS

	dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", user, password, host, port, database)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Extract schema
	schema := Schema{
		Database: database,
		Tables:   make(map[string]*Table),
	}
	tables, _ := getTables(db, database)
	for _, t := range tables {
		cols, _ := getColumns(db, database, t)
		schema.Tables[t] = &Table{Name: t, Columns: cols}
	}
	fks, _ := getForeignKeys(db, database)
	schema.Relationships = fks

	// Save schema JSON
	jsonFile := "schema.json"
	f, _ := os.Create(jsonFile)
	encoder := json.NewEncoder(f)
	encoder.SetIndent("", "  ")
	encoder.Encode(schema)
	f.Close()
	fmt.Println("✅ MySQL schema exported to", jsonFile)

	// Save Dgraph schema
	exportDgraphSchema(schema, "schema.graphql")
	fmt.Println("✅ Dgraph schema exported to schema.graphql")

	// Save RDF data with UID references
	exportDataRDF(db, schema, "data.rdf")
	fmt.Println("✅ Data exported to data.rdf")

	// Save JSON data
	exportDataJSON(db, schema, "data.json")
	fmt.Println("✅ Data exported to data.json")

	// Save advanced Dgraph schema
	exportDgraphAdvancedSchema(schema, "schema.txt")
	fmt.Println("✅ Advanced Dgraph schema exported to schema.txt")
}
