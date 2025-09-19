# Large-Scale Performance Testing Guide

## 🚀 Ready for 500K+ Records!

The pipeline has been optimized for high-performance processing of large datasets. Here's what's been implemented for the 0.5 million record test:

## 📊 Performance Optimizations

### 1. **Enhanced Configuration** ✅
- **Batch Size**: 5,000 records per batch (optimized for memory vs. speed)
- **Workers**: 8 parallel workers for concurrent processing
- **Chunk Size**: 50,000 records per output chunk
- **Memory Buffer**: 100MB buffer for efficient I/O
- **Connection Pool**: 20 MySQL connections with optimized timeouts

### 2. **Performance Monitoring** ✅
- **Real-time Metrics**: Records/second, memory usage, ETA
- **Progress Tracking**: Current table, processed rows, completion percentage
- **Memory Monitoring**: GC-optimized memory tracking
- **Performance Logging**: Every 10k records with detailed stats

### 3. **Chunked Processing** ✅
- **Streaming Export**: Process data in chunks to avoid memory issues
- **Chunked RDF Files**: Split output into manageable 50k record chunks
- **Parallel Import**: Import multiple chunks simultaneously to Dgraph
- **Retry Logic**: Automatic retry with exponential backoff

### 4. **Optimized Scripts** ✅
- **`run-large-scale-test.sh`**: Complete end-to-end pipeline runner
- **`import-chunked-to-dgraph.sh`**: Parallel import with rate limiting
- **Error Handling**: Comprehensive error recovery and logging

## 🎯 Quick Start for Large-Scale Test

### Step 1: Prepare Your Environment
```bash
# Ensure Docker containers are running
docker-compose up -d

# Check services
docker-compose ps
```

### Step 2: Load Your 0.5M Dataset
```bash
# Your 0.5M dataset should be loaded into MySQL
# Verify the data count
mysql -h localhost -P 3306 -u root -ppassword company_db -e "
SELECT 
    table_name, 
    table_rows 
FROM information_schema.tables 
WHERE table_schema = 'company_db'"
```

### Step 3: Run Large-Scale Pipeline
```bash
# Run the complete pipeline with optimized settings
./scripts/run-large-scale-test.sh

# Or run specific phases:
./scripts/run-large-scale-test.sh --extract-only  # Just extract data
./scripts/run-large-scale-test.sh --import-only   # Just import to Dgraph
./scripts/run-large-scale-test.sh --validate-only # Just validate results
```

## 📈 Expected Performance Metrics

Based on the optimizations, here's what to expect:

### **Processing Speed**
- **Small datasets (< 10K)**: ~1,000-2,000 records/sec
- **Medium datasets (10K-100K)**: ~800-1,500 records/sec  
- **Large datasets (100K+)**: ~500-1,000 records/sec

### **Memory Usage**
- **Baseline**: ~50-100MB
- **Peak Processing**: ~200-500MB
- **Chunked Export**: Memory stays under 1GB

### **Time Estimates for 500K Records**
- **Schema Extraction**: ~10-30 seconds
- **Data Processing**: ~8-15 minutes
- **RDF Generation**: ~5-10 minutes
- **Dgraph Import**: ~5-15 minutes
- **Total Pipeline**: ~20-35 minutes

## 🔧 Configuration Tuning

### For Even Larger Datasets (1M+):
```yaml
# config/production.yaml adjustments
pipeline:
  workers: 12              # More workers
  batch_size: 10000        # Larger batches
  memory_limit_mb: 8192    # More memory
  chunk_size: 100000       # Larger chunks

dgraph:
  batch_size: 100000       # Larger Dgraph batches
  max_retries: 10          # More retries
```

### For Faster Processing:
```bash
# Environment variables for maximum speed
export BATCH_SIZE=10000
export WORKERS=12
export CHUNK_SIZE=100000
export MAX_PARALLEL=5     # More parallel imports
```

## 📊 Monitoring During Processing

### Real-time Progress
The pipeline provides detailed progress information:
```
[2025-09-18 17:30:45] Performance metrics processed_rows=125000 records_per_second=892.34 memory_mb=245.67 current_table=users eta=8m45s
[2025-09-18 17:30:55] Export progress processed=150000 total=500000 progress_pct=30.00% records_per_sec=890.12 memory_mb=267.43 eta=6m32s
```

### Memory Monitoring
```bash
# Monitor system resources during processing
watch -n 5 'free -h && ps aux | grep mysql-dgraph-pipeline'
```

### Dgraph Monitoring
```bash
# Monitor Dgraph during import
watch -n 10 'curl -s http://localhost:8080/health | jq .'
```

## 🎉 Success Criteria

After the 0.5M record test, you should see:

### ✅ **Complete Data Transfer**
- All 500,000+ records exported to RDF
- All foreign key relationships preserved
- Schema correctly generated with all predicates

### ✅ **Performance Benchmarks**
- Processing rate > 500 records/second
- Memory usage < 1GB peak
- Total time < 45 minutes
- Zero data loss

### ✅ **Dgraph Import Success**
- All chunks imported without errors
- Query validation returns correct counts
- Relationships are traversable in both directions

### ✅ **Data Integrity**
- MySQL row count = Dgraph node count
- All foreign keys become graph edges
- Sample queries return expected results

## 🚨 Troubleshooting Large-Scale Issues

### High Memory Usage
```bash
# Reduce batch size and workers
export BATCH_SIZE=2500
export WORKERS=4
export CHUNK_SIZE=25000
```

### Slow Processing
```bash
# Increase parallelism
export WORKERS=16
export MAX_PARALLEL=8
```

### Import Failures
```bash
# Check Dgraph logs
docker-compose logs dgraph-alpha

# Retry failed chunks manually
./scripts/import-chunked-to-dgraph.sh --retry-failed
```

## 📋 Post-Test Validation

### 1. **Data Count Verification**
```bash
# Compare MySQL vs Dgraph counts
mysql -e "SELECT COUNT(*) as mysql_users FROM company_db.users"
curl -X POST http://localhost:8080/query -d '{"query": "{ users(func: type(users)) { count(uid) } }"}'
```

### 2. **Relationship Validation**
```bash
# Test complex relationship queries
curl -X POST http://localhost:8080/query -d '{
  "query": "{ 
    companies(func: type(companies), first: 5) { 
      companies.name 
      ~users.company_id { 
        count(uid) 
        users.name 
      } 
    } 
  }"
}'
```

### 3. **Performance Benchmarks**
```bash
# Query performance test
time curl -X POST http://localhost:8080/query -d '{
  "query": "{ all(func: has(dgraph.type)) { count(uid) } }"
}'
```

---

🎯 **You're now ready to test with 0.5 million records!** 

Just run `./scripts/run-large-scale-test.sh` and watch the optimized pipeline handle your large dataset with ease. The system will provide real-time feedback on progress, performance metrics, and any issues that arise.

The pipeline is designed to be production-ready and can scale to handle even larger datasets with the configuration adjustments provided above.
