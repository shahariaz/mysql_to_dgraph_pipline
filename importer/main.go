package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/dgraph-io/dgo/v230"
	"github.com/dgraph-io/dgo/v230/protos/api"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Config for Dgraph connection
type DgraphConfig struct {
	Host    string `json:"host"`
	Port    string `json:"port"`
	Alpha   string `json:"alpha"`    // Alpha server address
	Schema  string `json:"schema"`   // Schema file path
	DataDir string `json:"data_dir"` // Directory containing batch files
}

// Default configuration with intelligent path detection
func getDefaultConfig() DgraphConfig {
	// Get current working directory
	wd, _ := os.Getwd()
	fmt.Printf("🗂️  Current working directory: %s\n", wd)

	// Try different possible paths for dgraph_export
	possiblePaths := []string{
		"dgraph_export",       // Same directory
		"../dgraph_export",    // Parent directory
		"../../dgraph_export", // Grandparent directory
	}

	var exportDir, schemaFile string

	// Find the correct path
	for _, path := range possiblePaths {
		if _, err := os.Stat(path); err == nil {
			exportDir = path
			schemaFile = filepath.Join(path, "schema.dgraph")
			fmt.Printf("✅ Found dgraph_export directory at: %s\n", exportDir)
			break
		}
	}

	// If not found, use default and let validation catch it
	if exportDir == "" {
		exportDir = "dgraph_export"
		schemaFile = "dgraph_export/schema.dgraph"
		fmt.Printf("⚠️  dgraph_export directory not auto-detected, using default path\n")
	}

	return DgraphConfig{
		Host:    "localhost",
		Port:    "9080",
		Alpha:   "localhost:9080",
		Schema:  schemaFile,
		DataDir: exportDir,
	}
}

// Validate configuration paths and provide helpful error messages
func validateConfig(config DgraphConfig) error {
	wd, _ := os.Getwd()

	// Check schema file
	if _, err := os.Stat(config.Schema); os.IsNotExist(err) {
		fmt.Printf("❌ Schema file validation failed\n")
		fmt.Printf("   Looking for: %s\n", config.Schema)
		fmt.Printf("   From directory: %s\n", wd)

		// List available files in current and parent directories
		fmt.Printf("\n📁 Available files/directories in current location:\n")
		listDirectoryContents(".")

		if wd != filepath.Dir(wd) { // Not at root
			fmt.Printf("\n📁 Available files/directories in parent location:\n")
			listDirectoryContents("..")
		}

		return fmt.Errorf("schema file not found at %s", config.Schema)
	}

	// Check data directory
	if _, err := os.Stat(config.DataDir); os.IsNotExist(err) {
		return fmt.Errorf("data directory not found at %s", config.DataDir)
	}

	// Check if data directory has batch files
	batchFiles, err := getBatchFiles(config.DataDir)
	if err != nil {
		return fmt.Errorf("error reading batch files: %v", err)
	}

	if len(batchFiles) == 0 {
		return fmt.Errorf("no batch files found in %s", config.DataDir)
	}

	fmt.Printf("✅ Configuration validated successfully\n")
	fmt.Printf("   Schema file: %s ✓\n", config.Schema)
	fmt.Printf("   Data directory: %s ✓\n", config.DataDir)
	fmt.Printf("   Batch files found: %d\n", len(batchFiles))

	return nil
}

// Helper function to list directory contents
func listDirectoryContents(dir string) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		fmt.Printf("   Error reading directory %s: %v\n", dir, err)
		return
	}

	for _, entry := range entries {
		if entry.IsDir() {
			fmt.Printf("   📁 %s/\n", entry.Name())
		} else {
			fmt.Printf("   📄 %s\n", entry.Name())
		}
	}
}

// Enhanced connection with better error handling
func connectDgraph(config DgraphConfig) (*dgo.Dgraph, *grpc.ClientConn, error) {
	fmt.Printf("🔌 Connecting to Dgraph at %s...\n", config.Alpha)

	// Set connection timeout
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conn, err := grpc.DialContext(ctx, config.Alpha,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(), // Wait for connection to be ready
	)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to connect to Dgraph at %s: %v\nPlease ensure Dgraph is running", config.Alpha, err)
	}

	dgraphClient := dgo.NewDgraphClient(api.NewDgraphClient(conn))

	// Test connection with a simple query
	testCtx, testCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer testCancel()

	_, err = dgraphClient.NewTxn().Query(testCtx, "{ q(func: uid(0x1)) { uid } }")
	if err != nil && !strings.Contains(err.Error(), "uid 0x1 not found") {
		conn.Close()
		return nil, nil, fmt.Errorf("failed to test Dgraph connection: %v", err)
	}

	fmt.Printf("✅ Connected to Dgraph successfully!\n")
	return dgraphClient, conn, nil
}

// Enhanced schema validation and auto-correction
func validateAndCleanSchema(schemaFile string) (string, error) {
	fmt.Printf("🔍 Validating schema file: %s\n", schemaFile)

	schemaBytes, err := os.ReadFile(schemaFile)
	if err != nil {
		return "", fmt.Errorf("failed to read schema file: %v", err)
	}

	schemaContent := string(schemaBytes)

	// Check if file contains Go code (common mistake)
	if strings.Contains(schemaContent, "package main") ||
		strings.Contains(schemaContent, "import (") ||
		strings.Contains(schemaContent, "func main()") {
		fmt.Printf("❌ Schema file contains Go code instead of Dgraph schema!\n")
		fmt.Printf("📄 First few lines of the file:\n")
		lines := strings.Split(schemaContent, "\n")
		for i, line := range lines {
			if i >= 10 {
				break
			}
			fmt.Printf("   %d: %s\n", i+1, line)
		}
		return "", fmt.Errorf("invalid schema file: contains Go code instead of Dgraph schema")
	}

	// Check for common schema patterns
	hasTypes := strings.Contains(schemaContent, "type ")
	hasPredicates := strings.Contains(schemaContent, ":")

	if !hasTypes && !hasPredicates {
		return "", fmt.Errorf("schema file appears to be empty or invalid - no types or predicates found")
	}

	// Clean and validate schema
	cleanSchema := cleanSchemaContent(schemaContent)

	fmt.Printf("✅ Schema validation passed\n")
	fmt.Printf("📊 Schema size: %d bytes\n", len(cleanSchema))

	return cleanSchema, nil
}

// Clean schema content by removing comments and empty lines
func cleanSchemaContent(content string) string {
	lines := strings.Split(content, "\n")
	var cleanLines []string

	for _, line := range lines {
		line = strings.TrimSpace(line)

		// Skip empty lines and comments
		if line == "" || strings.HasPrefix(line, "#") || strings.HasPrefix(line, "//") {
			continue
		}

		cleanLines = append(cleanLines, line)
	}

	return strings.Join(cleanLines, "\n")
}

// Generate a sample schema if the current one is invalid
func generateSampleSchema(outputPath string) error {
	sampleSchema := `# Dgraph Schema for MySQL Export
# Types
type Person {
    name: string @index(term, fulltext) .
    email: string @index(hash) .
    age: int .
    created_at: datetime .
    updated_at: datetime .
}

type Company {
    name: string @index(term, fulltext) .
    website: string .
    industry: string @index(term) .
    founded: datetime .
    employees: [Person] .
}

type Product {
    name: string @index(term, fulltext) .
    description: string @index(fulltext) .
    price: float .
    category: Category .
    created_at: datetime .
}

type Category {
    name: string @index(term) .
    description: string .
    products: [Product] .
}

type Order {
    order_id: string @index(hash) .
    total: float .
    status: string @index(term) .
    customer: Person .
    products: [Product] .
    created_at: datetime .
}

# Predicates (if not using types)
name: string @index(term, fulltext) .
email: string @index(hash) .
age: int .
price: float .
description: string @index(fulltext) .
created_at: datetime .
updated_at: datetime .
`

	if err := os.WriteFile(outputPath, []byte(sampleSchema), 0644); err != nil {
		return fmt.Errorf("failed to write sample schema: %v", err)
	}

	fmt.Printf("✅ Sample schema generated at: %s\n", outputPath)
	return nil
}

// Enhanced schema loading with validation
func loadSchemaWithValidation(client *dgo.Dgraph, schemaFile string) error {
	fmt.Printf("📋 Loading and validating schema from %s...\n", schemaFile)

	// Validate and clean schema
	schemaContent, err := validateAndCleanSchema(schemaFile)
	if err != nil {
		fmt.Printf("❌ Schema validation failed: %v\n", err)

		// Offer to generate a sample schema
		fmt.Printf("\n💡 Would you like me to generate a sample schema? (y/n): ")
		var response string
		fmt.Scanln(&response)

		if strings.ToLower(strings.TrimSpace(response)) == "y" {
			samplePath := filepath.Join(filepath.Dir(schemaFile), "sample_schema.dgraph")
			if genErr := generateSampleSchema(samplePath); genErr != nil {
				return fmt.Errorf("failed to generate sample schema: %v", genErr)
			}

			fmt.Printf("\n📝 Please:\n")
			fmt.Printf("   1. Review the sample schema at: %s\n", samplePath)
			fmt.Printf("   2. Customize it based on your MySQL table structure\n")
			fmt.Printf("   3. Replace your current schema file or update the path\n")
			fmt.Printf("   4. Run the importer again\n")
		}

		return err
	}

	// Count schema elements
	lines := strings.Split(schemaContent, "\n")
	typeCount := 0
	predicateCount := 0

	for _, line := range lines {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "type ") {
			typeCount++
		} else if strings.Contains(line, ":") && !strings.HasPrefix(line, "#") && !strings.HasPrefix(line, "//") {
			predicateCount++
		}
	}

	fmt.Printf("📋 Schema contains: %d types, %d predicates\n", typeCount, predicateCount)

	// Apply schema to Dgraph
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	op := &api.Operation{Schema: schemaContent}

	if err := client.Alter(ctx, op); err != nil {
		// More detailed error handling
		if strings.Contains(err.Error(), "lexing") {
			return fmt.Errorf("schema syntax error: %v\n\n💡 Common issues:\n- Check for typos in type definitions\n- Ensure proper predicate syntax (name: type @index(...))\n- Remove any non-schema content", err)
		}
		return fmt.Errorf("failed to apply schema: %v", err)
	}

	fmt.Printf("✅ Schema loaded successfully!\n")
	return nil
}

// Check what files are in the dgraph_export directory
func inspectExportDirectory(dataDir string) error {
	fmt.Printf("🗂️  Inspecting export directory: %s\n", dataDir)

	entries, err := os.ReadDir(dataDir)
	if err != nil {
		return fmt.Errorf("failed to read directory: %v", err)
	}

	fmt.Printf("📁 Found %d files/directories:\n", len(entries))

	for _, entry := range entries {
		if entry.IsDir() {
			fmt.Printf("   📁 %s/ (directory)\n", entry.Name())
		} else {
			info, _ := entry.Info()
			fmt.Printf("   📄 %s (%d bytes)\n", entry.Name(), info.Size())

			// Show first few lines of schema files
			if strings.HasSuffix(entry.Name(), ".dgraph") || strings.HasSuffix(entry.Name(), ".schema") {
				filePath := filepath.Join(dataDir, entry.Name())
				showFilePreview(filePath)
			}
		}
	}

	return nil
}

// Show preview of a file
func showFilePreview(filePath string) {
	fmt.Printf("   👀 Preview of %s:\n", filepath.Base(filePath))

	content, err := os.ReadFile(filePath)
	if err != nil {
		fmt.Printf("      ❌ Error reading file: %v\n", err)
		return
	}

	lines := strings.Split(string(content), "\n")
	maxLines := 5
	if len(lines) < maxLines {
		maxLines = len(lines)
	}

	for i := 0; i < maxLines; i++ {
		line := strings.TrimSpace(lines[i])
		if len(line) > 80 {
			line = line[:80] + "..."
		}
		fmt.Printf("      %d: %s\n", i+1, line)
	}

	if len(lines) > maxLines {
		fmt.Printf("      ... (%d more lines)\n", len(lines)-maxLines)
	}
}

// Get all batch files sorted by name with better validation
func getBatchFiles(dataDir string) ([]string, error) {
	pattern := filepath.Join(dataDir, "batch_*.json")
	files, err := filepath.Glob(pattern)
	if err != nil {
		return nil, fmt.Errorf("error finding batch files with pattern %s: %v", pattern, err)
	}

	// Validate each file
	var validFiles []string
	for _, file := range files {
		if info, err := os.Stat(file); err == nil && info.Size() > 0 {
			validFiles = append(validFiles, file)
		} else if err != nil {
			fmt.Printf("⚠️  Skipping file %s: %v\n", filepath.Base(file), err)
		} else {
			fmt.Printf("⚠️  Skipping empty file %s\n", filepath.Base(file))
		}
	}

	sort.Strings(validFiles)
	return validFiles, nil
}

// Enhanced batch import with better error handling and progress
func importBatch(client *dgo.Dgraph, batchFile string) error {
	fileName := filepath.Base(batchFile)
	fmt.Printf("📦 Importing batch: %s", fileName)

	// Check file size
	fileInfo, err := os.Stat(batchFile)
	if err != nil {
		return fmt.Errorf("failed to stat file: %v", err)
	}
	fmt.Printf(" (%.2f KB)", float64(fileInfo.Size())/1024)

	// Read batch file
	data, err := os.ReadFile(batchFile)
	if err != nil {
		return fmt.Errorf("failed to read batch file: %v", err)
	}

	// Validate JSON before sending
	var batchData interface{}
	if err := json.Unmarshal(data, &batchData); err != nil {
		return fmt.Errorf("invalid JSON in batch file: %v", err)
	}

	// Count expected nodes (rough estimate)
	var nodeCount int
	switch v := batchData.(type) {
	case []interface{}:
		nodeCount = len(v)
	case map[string]interface{}:
		nodeCount = 1
	}

	// Create mutation with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	mu := &api.Mutation{
		SetJson:   data,
		CommitNow: true,
	}

	// Execute mutation
	start := time.Now()
	assigned, err := client.NewTxn().Mutate(ctx, mu)
	if err != nil {
		return fmt.Errorf("failed to mutate: %v", err)
	}

	duration := time.Since(start)

	fmt.Printf("\n  ✅ Imported %d nodes (expected ~%d) in %v\n", len(assigned.Uids), nodeCount, duration)
	return nil
}

// Enhanced import with better progress tracking and statistics
func importAllBatches(client *dgo.Dgraph, dataDir string) error {
	batchFiles, err := getBatchFiles(dataDir)
	if err != nil {
		return fmt.Errorf("failed to get batch files: %v", err)
	}

	if len(batchFiles) == 0 {
		return fmt.Errorf("no valid batch files found in %s", dataDir)
	}

	fmt.Printf("📁 Found %d valid batch files to import\n", len(batchFiles))

	// Calculate total size
	var totalSize int64
	for _, file := range batchFiles {
		if info, err := os.Stat(file); err == nil {
			totalSize += info.Size()
		}
	}
	fmt.Printf("📊 Total data size: %.2f MB\n", float64(totalSize)/(1024*1024))

	totalStart := time.Now()
	successCount := 0
	var failedFiles []string

	for i, batchFile := range batchFiles {
		fmt.Printf("\n[%d/%d] ", i+1, len(batchFiles))

		if err := importBatch(client, batchFile); err != nil {
			fileName := filepath.Base(batchFile)
			fmt.Printf("❌ Failed to import %s: %v\n", fileName, err)
			failedFiles = append(failedFiles, fileName)
			continue
		}

		successCount++

		// Show progress
		progress := float64(i+1) / float64(len(batchFiles)) * 100
		elapsed := time.Since(totalStart)
		fmt.Printf("  📈 Progress: %.1f%% (Elapsed: %v)\n", progress, elapsed.Round(time.Second))
	}

	totalDuration := time.Since(totalStart)
	fmt.Printf("\n🎉 Import completed!\n")
	fmt.Printf("✅ Successfully imported: %d/%d batches\n", successCount, len(batchFiles))
	fmt.Printf("⏱️  Total time: %v\n", totalDuration)
	fmt.Printf("📊 Average rate: %.2f batches/minute\n", float64(successCount)/totalDuration.Minutes())

	if len(failedFiles) > 0 {
		fmt.Printf("⚠️  %d batches failed to import:\n", len(failedFiles))
		for _, file := range failedFiles {
			fmt.Printf("   - %s\n", file)
		}
	}

	return nil
}

// Enhanced verification with more detailed statistics
func verifyImport(client *dgo.Dgraph, expectedTypes []string) error {
	fmt.Printf("\n🔍 Verifying import...\n")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	totalNodes := 0

	for _, nodeType := range expectedTypes {
		query := fmt.Sprintf(`
		{
			count(func: type(%s)) {
				count(uid)
			}
		}`, nodeType)

		resp, err := client.NewTxn().Query(ctx, query)
		if err != nil {
			fmt.Printf("❌ Failed to query %s: %v\n", nodeType, err)
			continue
		}

		var result map[string][]map[string]int
		if err := json.Unmarshal(resp.Json, &result); err != nil {
			fmt.Printf("❌ Failed to parse response for %s: %v\n", nodeType, err)
			continue
		}

		if count, ok := result["count"]; ok && len(count) > 0 {
			if c, ok := count[0]["count"]; ok {
				fmt.Printf("  ✅ %s: %,d records\n", nodeType, c)
				totalNodes += c
			}
		}
	}

	if totalNodes > 0 {
		fmt.Printf("\n📊 Total nodes imported: %,d\n", totalNodes)
	}

	// Additional verification - check for any orphaned nodes
	orphanQuery := `
	{
		orphans(func: has(uid)) @filter(NOT type(Person) AND NOT type(Company) AND NOT type(Order) AND NOT type(Product) AND NOT type(Category) AND NOT type(Review)) {
			count(uid)
		}
	}`

	if resp, err := client.NewTxn().Query(ctx, orphanQuery); err == nil {
		var result map[string][]map[string]int
		if json.Unmarshal(resp.Json, &result) == nil {
			if orphans, ok := result["orphans"]; ok && len(orphans) > 0 {
				if count, ok := orphans[0]["count"]; ok && count > 0 {
					fmt.Printf("⚠️  Found %d nodes without recognized types\n", count)
				}
			}
		}
	}

	return nil
}

// Enhanced type extraction with better parsing
func getExpectedTypes(schemaFile string) ([]string, error) {
	data, err := os.ReadFile(schemaFile)
	if err != nil {
		return nil, err
	}

	var types []string
	lines := strings.Split(string(data), "\n")

	for _, line := range lines {
		line = strings.TrimSpace(line)
		// More robust type detection
		if strings.HasPrefix(line, "type ") && strings.Contains(line, "{") {
			// Extract type name more carefully
			parts := strings.Fields(line)
			if len(parts) >= 2 {
				typeName := strings.TrimSpace(parts[1])
				// Remove any trailing characters like '{'
				typeName = strings.Split(typeName, "{")[0]
				typeName = strings.TrimSpace(typeName)
				if typeName != "" && !contains(types, typeName) {
					types = append(types, typeName)
				}
			}
		}
	}

	return types, nil
}

// Helper function to check if slice contains string
func contains(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}

// Enhanced health check with more detailed diagnostics
func checkDgraphHealth(host, port string) error {
	fmt.Printf("🏥 Checking Dgraph health at %s:%s...\n", host, port)

	url := fmt.Sprintf("http://%s:%s/health", host, port)

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Get(url)
	if err != nil {
		return fmt.Errorf("Dgraph health check failed - server not responding: %v", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("Dgraph health check failed with status %d: %s", resp.StatusCode, string(body))
	}

	// Try to parse health response for more details
	var healthData map[string]interface{}
	if json.Unmarshal(body, &healthData) == nil {
		if status, ok := healthData["status"].(string); ok {
			fmt.Printf("📊 Dgraph status: %s\n", status)
		}
	}

	return nil
}

// Enhanced interactive configuration with validation
func getConfig() DgraphConfig {
	config := getDefaultConfig()

	fmt.Printf("\n🔧 Dgraph Import Configuration\n")
	fmt.Printf("================================\n")
	fmt.Printf("Current settings:\n")
	fmt.Printf("  Dgraph Alpha: %s\n", config.Alpha)
	fmt.Printf("  Schema file: %s\n", config.Schema)
	fmt.Printf("  Data directory: %s\n", config.DataDir)
	fmt.Printf("\nPress Enter to use defaults or type 'custom' to customize: ")

	var input string
	fmt.Scanln(&input)

	if strings.ToLower(strings.TrimSpace(input)) == "custom" {
		fmt.Printf("\n🎛️  Custom Configuration\n")
		fmt.Printf("======================\n")

		fmt.Printf("Dgraph Alpha address [%s]: ", config.Alpha)
		fmt.Scanln(&input)
		if strings.TrimSpace(input) != "" {
			config.Alpha = strings.TrimSpace(input)
		}

		fmt.Printf("Schema file path [%s]: ", config.Schema)
		fmt.Scanln(&input)
		if strings.TrimSpace(input) != "" {
			config.Schema = strings.TrimSpace(input)
		}

		fmt.Printf("Data directory [%s]: ", config.DataDir)
		fmt.Scanln(&input)
		if strings.TrimSpace(input) != "" {
			config.DataDir = strings.TrimSpace(input)
		}

		fmt.Printf("\n📝 Updated configuration:\n")
		fmt.Printf("  Dgraph Alpha: %s\n", config.Alpha)
		fmt.Printf("  Schema file: %s\n", config.Schema)
		fmt.Printf("  Data directory: %s\n", config.DataDir)
	}

	return config
}

// Show helpful startup information
func showStartupInfo() {
	fmt.Printf("🚀 Dgraph Data Importer\n")
	fmt.Printf("========================\n")
	fmt.Printf("Version: 2.0 Enhanced\n")
	fmt.Printf("Features: Auto-path detection, Enhanced validation, Progress tracking\n")
	fmt.Printf("Time: %s\n\n", time.Now().Format("2006-01-02 15:04:05"))
}

// Show completion information
func showCompletionInfo() {
	fmt.Printf("\n🎉 All done! Your MySQL data is now in Dgraph!\n")
	fmt.Printf("🌐 You can explore your data at: http://localhost:8000\n")
	fmt.Printf("📖 Dgraph documentation: https://dgraph.io/docs/\n")
	fmt.Printf("🔍 Try some queries in Ratel (the Dgraph UI)\n")
	fmt.Printf("\n💡 Sample queries to try:\n")
	fmt.Printf("   Query all types: { q(func: has(dgraph.type)) { dgraph.type } }\n")
	fmt.Printf("   Count by type: { q(func: type(YourType)) { count(uid) } }\n")
}

func main() {
	showStartupInfo()

	// Get configuration with auto-detection
	config := getConfig()

	// Validate configuration paths
	fmt.Printf("\n🔍 Validating configuration...\n")
	if err := validateConfig(config); err != nil {
		fmt.Printf("❌ Configuration validation failed: %v\n", err)

		// Show directory inspection
		if _, statErr := os.Stat(config.DataDir); statErr == nil {
			inspectExportDirectory(config.DataDir)
		}

		fmt.Printf("\n💡 Suggestions:\n")
		fmt.Printf("   1. Run from the correct directory (where dgraph_export exists)\n")
		fmt.Printf("   2. Use 'custom' configuration to set correct paths\n")
		fmt.Printf("   3. Ensure your export files are in the expected location\n")
		fmt.Printf("\n📁 Current directory structure should look like:\n")
		fmt.Printf("   your-project/\n")
		fmt.Printf("   ├── dgraph_export/\n")
		fmt.Printf("   │   ├── schema.dgraph\n")
		fmt.Printf("   │   ├── batch_001.json\n")
		fmt.Printf("   │   └── batch_002.json\n")
		fmt.Printf("   └── importer/\n")
		fmt.Printf("       └── main.go\n")
		return
	}

	// Check if Dgraph is running
	if err := checkDgraphHealth(config.Host, "8080"); err != nil {
		fmt.Printf("❌ %v\n", err)
		fmt.Printf("\n💡 Make sure Dgraph is running. Here are the commands:\n")
		fmt.Printf("\n🐳 Using Docker (Recommended):\n")
		fmt.Printf("   docker-compose up -d\n")
		fmt.Printf("\n📦 Or manually:\n")
		fmt.Printf("   # Terminal 1:\n")
		fmt.Printf("   dgraph zero --my=localhost:5080\n")
		fmt.Printf("   # Terminal 2:\n")
		fmt.Printf("   dgraph alpha --my=localhost:7080 --zero=localhost:5080\n")
		fmt.Printf("\n🔗 Dgraph UI will be available at: http://localhost:8000\n")
		return
	}
	fmt.Printf("✅ Dgraph is healthy and ready!\n")

	// Connect to Dgraph
	client, conn, err := connectDgraph(config)
	if err != nil {
		log.Fatalf("❌ Connection failed: %v", err)
	}
	defer conn.Close()

	// Load schema with enhanced validation
	if err := loadSchemaWithValidation(client, config.Schema); err != nil {
		log.Fatalf("❌ Schema loading failed: %v", err)
	}

	// Import all batches
	fmt.Printf("\n🚛 Starting batch import process...\n")
	if err := importAllBatches(client, config.DataDir); err != nil {
		log.Fatalf("❌ Import failed: %v", err)
	}

	// Verify import
	fmt.Printf("\n🔍 Running post-import verification...\n")
	if expectedTypes, err := getExpectedTypes(config.Schema); err == nil && len(expectedTypes) > 0 {
		verifyImport(client, expectedTypes)
	} else {
		fmt.Printf("⚠️  Could not extract types from schema for verification\n")
	}

	showCompletionInfo()
}
