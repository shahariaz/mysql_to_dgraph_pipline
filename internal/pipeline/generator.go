package pipeline

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/shahariaz/mysql_to_dgraph_pipeline/internal/config"
	"github.com/shahariaz/mysql_to_dgraph_pipeline/pkg/logger"
)

// SchemaGenerator generates Dgraph schema from MySQL schema
type SchemaGenerator struct {
	cfg    *config.Config
	logger *logger.Logger
}

// PredicateInfo holds information about a predicate
type PredicateInfo struct {
	Name    string
	Type    string
	Index   string
	Reverse bool
	List    bool
	Count   bool
	Upsert  bool
}

func NewSchemaGenerator(cfg *config.Config, logger *logger.Logger) *SchemaGenerator {
	return &SchemaGenerator{
		cfg:    cfg,
		logger: logger,
	}
}

func (sg *SchemaGenerator) Generate(schema *Schema) error {
	// Create output directory
	if err := os.MkdirAll(sg.cfg.Output.Directory, 0755); err != nil {
		return fmt.Errorf("failed to create output directory: %w", err)
	}

	// Generate predicates
	predicates := sg.generatePredicates(schema)

	// Generate types
	types := sg.generateTypes(schema, predicates)

	// Write schema file
	schemaPath := filepath.Join(sg.cfg.Output.Directory, sg.cfg.Output.SchemaFile)
	if err := sg.writeSchemaFile(schemaPath, predicates, types); err != nil {
		return fmt.Errorf("failed to write schema file: %w", err)
	}

	sg.logger.Info("Dgraph schema generated successfully",
		"predicates", len(predicates),
		"types", len(types),
		"file", schemaPath)

	return nil
}

func (sg *SchemaGenerator) generatePredicates(schema *Schema) map[string]*PredicateInfo {
	predicates := make(map[string]*PredicateInfo)

	// Generate predicates for table columns
	for tableName, table := range schema.Tables {
		for columnName, column := range table.Columns {
			predicateName := fmt.Sprintf("%s.%s", tableName, columnName)
			dgraphType := MySQLToDgraphType(column.Type)

			predicate := &PredicateInfo{
				Name: predicateName,
				Type: dgraphType,
			}

			// Add appropriate index
			predicate.Index = sg.getIndexType(dgraphType, column)

			// Check if it's a upsert candidate (unique columns)
			predicate.Upsert = sg.isUpsertCandidate(tableName, columnName, schema)

			predicates[predicateName] = predicate
		}
	}

	// Generate predicates for foreign key relationships
	for _, fk := range schema.Relationships {
		// Forward relationship
		fkPredicateName := fmt.Sprintf("%s.%s", fk.TableName, fk.ColumnName)
		if pred, exists := predicates[fkPredicateName]; exists {
			pred.Type = "uid"
			pred.Reverse = true
			pred.Index = "" // UID predicates don't need index specification
		} else {
			predicates[fkPredicateName] = &PredicateInfo{
				Name:    fkPredicateName,
				Type:    "uid",
				Reverse: true,
			}
		}

		// Reverse relationship (collection)
		reversePredicateName := fmt.Sprintf("%s.%s_reverse", fk.TableName, fk.ColumnName)
		predicates[reversePredicateName] = &PredicateInfo{
			Name:    reversePredicateName,
			Type:    "uid",
			List:    true,
			Reverse: true,
		}

		// Also create a semantic reverse relationship
		semanticReverseName := fmt.Sprintf("%s.%s", fk.RefTableName, sg.pluralize(fk.TableName))
		if _, exists := predicates[semanticReverseName]; !exists {
			predicates[semanticReverseName] = &PredicateInfo{
				Name:    semanticReverseName,
				Type:    "uid",
				List:    true,
				Reverse: true,
			}
		}
	}

	return predicates
}

func (sg *SchemaGenerator) generateTypes(schema *Schema, predicates map[string]*PredicateInfo) map[string][]string {
	types := make(map[string][]string)

	for tableName, table := range schema.Tables {
		var typePredicates []string

		// Add column predicates
		for columnName := range table.Columns {
			predicateName := fmt.Sprintf("%s.%s", tableName, columnName)
			typePredicates = append(typePredicates, predicateName)
		}

		// Add outgoing foreign key predicates
		for _, fk := range schema.Relationships {
			if fk.TableName == tableName {
				predicateName := fmt.Sprintf("%s.%s", fk.TableName, fk.ColumnName)
				if !sg.containsString(typePredicates, predicateName) {
					typePredicates = append(typePredicates, predicateName)
				}
			}
		}

		// Add incoming foreign key predicates (reverse relationships)
		for _, fk := range schema.Relationships {
			if fk.RefTableName == tableName {
				// Add reverse predicates
				reversePredicateName := fmt.Sprintf("%s.%s_reverse", fk.TableName, fk.ColumnName)
				if !sg.containsString(typePredicates, reversePredicateName) {
					typePredicates = append(typePredicates, reversePredicateName)
				}

				// Add semantic reverse relationship
				semanticReverseName := fmt.Sprintf("%s.%s", tableName, sg.pluralize(fk.TableName))
				if !sg.containsString(typePredicates, semanticReverseName) {
					typePredicates = append(typePredicates, semanticReverseName)
				}
			}
		}

		sort.Strings(typePredicates)
		types[tableName] = typePredicates
	}

	return types
}

func (sg *SchemaGenerator) writeSchemaFile(filePath string, predicates map[string]*PredicateInfo, types map[string][]string) error {
	file, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := bufio.NewWriter(file)
	defer writer.Flush()

	// Write header
	sg.writeHeader(writer)

	// Write predicates
	sg.writePredicates(writer, predicates)

	// Write types
	sg.writeTypes(writer, types)

	return nil
}

func (sg *SchemaGenerator) writeHeader(writer *bufio.Writer) {
	fmt.Fprintln(writer, "# ==============================================")
	fmt.Fprintln(writer, "# Dgraph Schema Generated from MySQL Database")
	fmt.Fprintln(writer, "# ==============================================")
	fmt.Fprintln(writer, "# Generated automatically by mysql-to-dgraph pipeline")
	fmt.Fprintln(writer, "# Do not edit this file manually")
	fmt.Fprintln(writer, "#")
	fmt.Fprintln(writer, "# This schema includes:")
	fmt.Fprintln(writer, "# - All table columns as predicates")
	fmt.Fprintln(writer, "# - Foreign key relationships with @reverse")
	fmt.Fprintln(writer, "# - Appropriate indexes for performance")
	fmt.Fprintln(writer, "# - Type definitions for all tables")
	fmt.Fprintln(writer, "# ==============================================")
	fmt.Fprintln(writer)
}

func (sg *SchemaGenerator) writePredicates(writer *bufio.Writer, predicates map[string]*PredicateInfo) {
	fmt.Fprintln(writer, "# ==============================================")
	fmt.Fprintln(writer, "# PREDICATES")
	fmt.Fprintln(writer, "# ==============================================")
	fmt.Fprintln(writer)

	// Sort predicates for consistent output
	var sortedPredicates []*PredicateInfo
	for _, pred := range predicates {
		sortedPredicates = append(sortedPredicates, pred)
	}
	sort.Slice(sortedPredicates, func(i, j int) bool {
		return sortedPredicates[i].Name < sortedPredicates[j].Name
	})

	for _, pred := range sortedPredicates {
		var line strings.Builder
		line.WriteString(pred.Name)
		line.WriteString(": ")

		// Handle list types
		if pred.List {
			line.WriteString("[")
			line.WriteString(pred.Type)
			line.WriteString("]")
		} else {
			line.WriteString(pred.Type)
		}

		// Add directives
		var directives []string

		if pred.Index != "" {
			directives = append(directives, pred.Index)
		}

		if pred.Reverse {
			directives = append(directives, "@reverse")
		}

		if pred.Count {
			directives = append(directives, "@count")
		}

		if pred.Upsert {
			directives = append(directives, "@upsert")
		}

		if len(directives) > 0 {
			line.WriteString(" ")
			line.WriteString(strings.Join(directives, " "))
		}

		line.WriteString(" .")
		fmt.Fprintln(writer, line.String())
	}
	fmt.Fprintln(writer)
}

func (sg *SchemaGenerator) writeTypes(writer *bufio.Writer, types map[string][]string) {
	fmt.Fprintln(writer, "# ==============================================")
	fmt.Fprintln(writer, "# TYPES")
	fmt.Fprintln(writer, "# ==============================================")
	fmt.Fprintln(writer)

	// Sort types for consistent output
	var sortedTypeNames []string
	for typeName := range types {
		sortedTypeNames = append(sortedTypeNames, typeName)
	}
	sort.Strings(sortedTypeNames)

	for _, typeName := range sortedTypeNames {
		predicateList := types[typeName]

		fmt.Fprintf(writer, "type %s {\n", typeName)
		fmt.Fprintln(writer, "  dgraph.type")

		for _, predicate := range predicateList {
			fmt.Fprintf(writer, "  %s\n", predicate)
		}

		fmt.Fprintln(writer, "}")
		fmt.Fprintln(writer)
	}
}

func (sg *SchemaGenerator) getIndexType(dgraphType string, column *Column) string {
	switch dgraphType {
	case "string":
		// Use term index for most strings, exact for IDs and unique fields
		if strings.Contains(strings.ToLower(column.Name), "id") ||
			strings.Contains(strings.ToLower(column.Name), "email") ||
			strings.Contains(strings.ToLower(column.Name), "username") {
			return "@index(exact)"
		}
		return "@index(term)"
	case "int":
		return "@index(int)"
	case "float":
		return "@index(float)"
	case "bool":
		return "@index(bool)"
	case "dateTime", "datetime":
		return "@index(hour)"
	default:
		return ""
	}
}

func (sg *SchemaGenerator) isUpsertCandidate(tableName, columnName string, schema *Schema) bool {
	// Primary keys and unique columns are upsert candidates
	table := schema.Tables[tableName]
	if table == nil {
		return false
	}

	// Check if it's a primary key
	for _, pk := range table.PrimaryKeys {
		if pk == columnName {
			return true
		}
	}

	// Check for common unique columns
	columnLower := strings.ToLower(columnName)
	uniquePatterns := []string{"email", "username", "slug", "code", "uuid"}

	for _, pattern := range uniquePatterns {
		if strings.Contains(columnLower, pattern) {
			return true
		}
	}

	return false
}

func (sg *SchemaGenerator) pluralize(name string) string {
	name = strings.ToLower(name)

	// Simple pluralization rules
	if strings.HasSuffix(name, "s") || strings.HasSuffix(name, "x") ||
		strings.HasSuffix(name, "z") || strings.HasSuffix(name, "ch") ||
		strings.HasSuffix(name, "sh") {
		return name + "es"
	}

	if strings.HasSuffix(name, "y") && len(name) > 1 {
		secondLast := name[len(name)-2]
		if secondLast != 'a' && secondLast != 'e' && secondLast != 'i' &&
			secondLast != 'o' && secondLast != 'u' {
			return name[:len(name)-1] + "ies"
		}
	}

	if strings.HasSuffix(name, "f") {
		return name[:len(name)-1] + "ves"
	}

	if strings.HasSuffix(name, "fe") {
		return name[:len(name)-2] + "ves"
	}

	return name + "s"
}

func (sg *SchemaGenerator) containsString(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}
