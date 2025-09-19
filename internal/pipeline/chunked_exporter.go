package pipeline

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/shahariaz/mysql_to_dgraph_pipeline/internal/config"
	"github.com/shahariaz/mysql_to_dgraph_pipeline/pkg/logger"
)

// ChunkedExporter handles large-scale data export in chunks
type ChunkedExporter struct {
	cfg          *config.Config
	logger       *logger.Logger
	outputDir    string
	chunkSize    int64
	currentChunk int
	mu           sync.Mutex
}

// ChunkInfo contains information about an export chunk
type ChunkInfo struct {
	Index    int
	Filename string
	Size     int64
	Records  int64
}

func NewChunkedExporter(cfg *config.Config, logger *logger.Logger, outputDir string, chunkSize int64) *ChunkedExporter {
	return &ChunkedExporter{
		cfg:          cfg,
		logger:       logger,
		outputDir:    outputDir,
		chunkSize:    chunkSize,
		currentChunk: 0,
	}
}

// CreateChunk creates a new chunk file for export
func (ce *ChunkedExporter) CreateChunk(format string) (*os.File, string, error) {
	ce.mu.Lock()
	defer ce.mu.Unlock()

	ce.currentChunk++
	filename := fmt.Sprintf("data_chunk_%d.%s", ce.currentChunk, format)
	filepath := filepath.Join(ce.outputDir, filename)

	file, err := os.Create(filepath)
	if err != nil {
		return nil, "", fmt.Errorf("failed to create chunk file %s: %w", filepath, err)
	}

	ce.logger.Info("Created new chunk file", "file", filename, "chunk", ce.currentChunk)
	return file, filename, nil
}

// ExportInChunks exports data in manageable chunks
func (ce *ChunkedExporter) ExportInChunks(ctx context.Context, processor *DataProcessor, schema *Schema, tables []string) ([]ChunkInfo, error) {
	var chunks []ChunkInfo
	totalRecords := int64(0)

	// Estimate total records first
	ce.logger.Info("Estimating total records to process...")
	for _, tableName := range tables {
		count, err := processor.getTableRowCount(tableName)
		if err != nil {
			ce.logger.Warn("Failed to get row count for table", "table", tableName, "error", err)
			continue
		}
		totalRecords += count
		ce.logger.Info("Table row count", "table", tableName, "rows", count)
	}

	processor.metrics.TotalRows = totalRecords
	processor.metrics.TablesCount = len(tables)

	ce.logger.Info("Starting chunked export", "total_records", totalRecords, "chunk_size", ce.chunkSize)

	// Process tables in chunks
	currentRecords := int64(0)
	chunkRecords := int64(0)

	var currentFile *os.File
	var currentWriter *bufio.Writer
	var currentFilename string
	var err error

	// Create first chunk
	currentFile, currentFilename, err = ce.CreateChunk("rdf")
	if err != nil {
		return nil, err
	}
	defer currentFile.Close()

	currentWriter = bufio.NewWriterSize(currentFile, 1024*1024) // 1MB buffer
	defer currentWriter.Flush()

	for tableIndex, tableName := range tables {
		processor.metrics.ProcessedTables = tableIndex
		processor.metrics.CurrentTable = tableName

		table := schema.Tables[tableName]
		if table == nil {
			ce.logger.Warn("Table not found in schema", "table", tableName)
			continue
		}

		// Process table in batches
		offset := int64(0)
		batchSize := int64(ce.cfg.Pipeline.BatchSize)

		for {
			select {
			case <-ctx.Done():
				return chunks, ctx.Err()
			default:
			}

			// Check if we need a new chunk
			if chunkRecords >= ce.chunkSize {
				// Finalize current chunk
				currentWriter.Flush()
				currentFile.Close()

				chunks = append(chunks, ChunkInfo{
					Index:    ce.currentChunk,
					Filename: currentFilename,
					Records:  chunkRecords,
				})

				// Create new chunk
				currentFile, currentFilename, err = ce.CreateChunk("rdf")
				if err != nil {
					return chunks, err
				}
				defer currentFile.Close()

				currentWriter = bufio.NewWriterSize(currentFile, 1024*1024)
				defer currentWriter.Flush()
				chunkRecords = 0
			}

			// Process batch from table
			batchProcessed, err := processor.processTableBatchToWriter(ctx, tableName, table, offset, batchSize, currentWriter, schema)
			if err != nil {
				ce.logger.Error("Failed to process batch", "table", tableName, "offset", offset, "error", err)
				break
			}

			if batchProcessed == 0 {
				break // No more data
			}

			currentRecords += batchProcessed
			chunkRecords += batchProcessed
			offset += batchSize

			// Update metrics
			processor.metrics.UpdateProgress(currentRecords, tableName)

			// Log progress every 10k records
			if currentRecords%10000 == 0 {
				processed, rps, memMB, _ := processor.metrics.GetStats()
				eta := processor.metrics.EstimateCompletion()

				ce.logger.Info("Export progress",
					"processed", processed,
					"total", totalRecords,
					"progress_pct", fmt.Sprintf("%.2f%%", float64(processed)/float64(totalRecords)*100),
					"records_per_sec", fmt.Sprintf("%.2f", rps),
					"memory_mb", fmt.Sprintf("%.2f", memMB),
					"eta", eta.String(),
				)
			}
		}
	}

	// Finalize last chunk
	if chunkRecords > 0 {
		currentWriter.Flush()
		currentFile.Close()

		chunks = append(chunks, ChunkInfo{
			Index:    ce.currentChunk,
			Filename: currentFilename,
			Records:  chunkRecords,
		})
	}

	ce.logger.Info("Chunked export completed",
		"total_chunks", len(chunks),
		"total_records", currentRecords,
		"duration", time.Since(processor.metrics.StartTime),
	)

	return chunks, nil
}
