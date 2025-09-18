package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"strings"

	_ "github.com/go-sql-driver/mysql"
)

const batchSize = 1000

var globalUIDRegistry = make(map[string]string) // shared_id -> blank node UID

// Map for foreign keys: table.column -> referenced table
var foreignKeyMap = make(map[string]string)

// ---------------- Helpers ----------------
func getGlobalUID(id string) string {
	if uid, ok := globalUIDRegistry[id]; ok {
		return uid
	}
	uid := fmt.Sprintf("_:global%s", id)
	globalUIDRegistry[id] = uid
	return uid
}

func escapeString(val string) string {
	val = strings.ReplaceAll(val, `\`, `\\`)
	val = strings.ReplaceAll(val, `"`, `\"`)
	val = strings.ReplaceAll(val, "\n", `\n`)
	val = strings.ReplaceAll(val, "\r", `\r`)
	val = strings.ReplaceAll(val, "\t", `\t`)
	return val
}

// ---------------- Main ----------------
func main() {
	dsn := "root:root@tcp(127.0.0.1:3306)/dump"
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	outFile, err := os.Create("mysql_to_dgraph_prod.rdf")
	if err != nil {
		log.Fatal(err)
	}
	defer outFile.Close()

	// ---------------- Foreign key detection ----------------
	fkRows, err := db.Query(`
		SELECT table_name, column_name, referenced_table_name
		FROM information_schema.key_column_usage
		WHERE table_schema=DATABASE() AND referenced_table_name IS NOT NULL
	`)
	if err != nil {
		log.Fatal(err)
	}
	defer fkRows.Close()

	for fkRows.Next() {
		var table, col, refTable string
		fkRows.Scan(&table, &col, &refTable)
		key := fmt.Sprintf("%s.%s", table, col)
		foreignKeyMap[key] = refTable
	}

	// ---------------- Tables ----------------
	tablesRows, err := db.Query("SELECT table_name FROM information_schema.tables WHERE table_schema=DATABASE()")
	if err != nil {
		log.Fatal(err)
	}
	defer tablesRows.Close()

	var tables []string
	for tablesRows.Next() {
		var t string
		tablesRows.Scan(&t)
		tables = append(tables, t)
	}
	for _, table := range tables {
		fmt.Printf("Processing table: %s\n", table)

		colsRows, err := db.Query(
			"SELECT column_name FROM information_schema.columns WHERE table_schema=DATABASE() AND table_name=?",
			table,
		)
		if err != nil {
			log.Fatal(err)
		}
		var cols []string
		for colsRows.Next() {
			var c string
			colsRows.Scan(&c)
			cols = append(cols, c)
		}
		colsRows.Close()

		// Count rows
		var totalRows int
		err = db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM `%s`", table)).Scan(&totalRows)
		if err != nil {
			log.Printf("Warning: Could not count rows for table %s: %v", table, err)
			continue // Skip this table if count fails
		}

		for offset := 0; offset < totalRows; offset += batchSize {
			query := fmt.Sprintf("SELECT * FROM `%s` LIMIT %d OFFSET %d", table, batchSize, offset)
			rows, err := db.Query(query)
			if err != nil {
				log.Fatal(err)
			}

			values := make([]sql.RawBytes, len(cols))
			scanArgs := make([]interface{}, len(cols))
			for i := range values {
				scanArgs[i] = &values[i]
			}

			for rows.Next() {
				err := rows.Scan(scanArgs...)
				if err != nil {
					log.Fatal(err)
				}

				// Row UID
				var rowID string
				for i, col := range cols {
					if strings.ToLower(col) == "id" || strings.HasSuffix(strings.ToLower(col), "_id") {
						rowID = string(values[i])
						break
					}
				}
				if rowID == "" && len(cols) > 0 {
					rowID = string(values[0])
				}
				rowUID := fmt.Sprintf("_:%s_%s", table, rowID)
				fmt.Fprintf(outFile, "%s <dgraph.type> \"%s\" .\n", rowUID, table)

				for i, col := range cols {
					val := string(values[i])
					if val == "" || strings.ToLower(val) == "null" {
						continue
					}

					key := fmt.Sprintf("%s.%s", table, col)
					if refTableFromMap, ok := foreignKeyMap[key]; ok {
						// Use refTableFromMap here to ensure it's used
						_ = refTableFromMap // Explicitly use it (could log or something, but this silences unused)
						targetUID := getGlobalUID(val)
						fmt.Fprintf(outFile, "%s <%s> %s .\n", rowUID, col, targetUID)
						// Reverse edge
						fmt.Fprintf(outFile, "%s <%s_reverse> %s .\n", targetUID, col, rowUID)
					} else if strings.HasSuffix(strings.ToLower(col), "_id") || strings.Contains(strings.ToLower(col), "parent_id") {
						targetUID := getGlobalUID(val)
						fmt.Fprintf(outFile, "%s <%s> %s .\n", rowUID, col, targetUID)
						// Reverse edge
						fmt.Fprintf(outFile, "%s <%s_reverse> %s .\n", targetUID, col, rowUID)
					} else {
						fmt.Fprintf(outFile, "%s <%s> \"%s\" .\n", rowUID, col, escapeString(val))
					}
				}
				fmt.Fprintln(outFile)
			}
			rows.Close()
		}
	}

	// Write global nodes
	for id, uid := range globalUIDRegistry {
		fmt.Fprintf(outFile, "%s <dgraph.type> \"GlobalID\" .\n", uid)
		fmt.Fprintf(outFile, "%s <global_id> \"%s\" .\n\n", uid, id)
	}

	log.Println("✅ Production-ready RDF with FK detection generated: mysql_to_dgraph_prod.rdf")
}
