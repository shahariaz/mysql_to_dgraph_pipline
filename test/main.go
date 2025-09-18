package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"strings"
)

func main() {
	// Open the RDF file
	file, err := os.Open("mysql_to_dgraph_prod.rdf")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	// Maps to store predicates and types
	predicates := make(map[string]string) // predicate -> type
	types := make(map[string][]string)    // dgraph.type -> list of predicates
	seenTypes := make(map[string]bool)    // Track unique dgraph.type values

	// Parse RDF file
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		// Split RDF triple (assuming format: subject predicate object .)
		parts := strings.Split(line, " ")
		if len(parts) < 4 {
			continue
		}
		subject := parts[0]
		predicate := strings.Trim(parts[1], "<>")
		object := strings.Join(parts[2:len(parts)-1], " ")

		// Infer predicate type
		if predicate == "dgraph.type" {
			// Extract type name (remove quotes)
			typeName := strings.Trim(object, "\"")
			seenTypes[typeName] = true
			if _, ok := types[typeName]; !ok {
				types[typeName] = []string{}
			}
			continue
		}

		// Determine predicate type
		if strings.HasPrefix(object, "_:") || strings.HasPrefix(object, "<0x") {
			predicates[predicate] = "[uid]"
			if strings.HasSuffix(predicate, "_reverse") {
				predicates[predicate] += " @reverse"
			}
		} else if strings.HasPrefix(object, "\"") {
			// Assume string for now (could add logic for int, float, etc.)
			predicates[predicate] = "string @index(term)"
			if predicate == "global_id" {
				predicates[predicate] = "string @index(exact)"
			}
		}

		// Associate predicate with type
		for typeName := range seenTypes {
			if strings.Contains(subject, "_:"+typeName+"_") {
				if !contains(types[typeName], predicate) {
					types[typeName] = append(types[typeName], predicate)
				}
			}
		}
	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}

	// Generate schema file
	outFile, err := os.Create("dgraph_schema.graphql")
	if err != nil {
		log.Fatal(err)
	}
	defer outFile.Close()

	// Write predicates
	fmt.Fprintln(outFile, "# Predicates")
	for pred, predType := range predicates {
		fmt.Fprintf(outFile, "%s: %s .\n", pred, predType)
	}

	// Write types
	fmt.Fprintln(outFile, "\n# Types")
	for typeName, preds := range types {
		fmt.Fprintf(outFile, "type %s {\n", typeName)
		fmt.Fprintln(outFile, "    dgraph.type")
		for _, pred := range preds {
			fmt.Fprintf(outFile, "    %s\n", pred)
		}
		fmt.Fprintln(outFile, "}")
	}

	log.Println("✅ Dgraph schema generated: dgraph_schema.graphql")
}

// Helper to check if a slice contains a string
func contains(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}
