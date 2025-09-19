# MySQL to Dgraph Pipeline - Complete Commands Reference

## 🚀 Complete End-to-End Pipeline Process

This document contains all the commands needed to run the complete MySQL to Dgraph migration pipeline from scratch.

---

## 📋 Prerequisites

1. Docker containers running (MySQL, Dgraph Zero, Alpha, Ratel)
2. MySQL database populated with data
3. Pipeline binary built (`go build -o mysql-dgraph-pipeline ./cmd/main.go`)

---

## 🔄 Step 1: Verify MySQL Data

```bash
# Connect to MySQL and check available data
mysql -h localhost -P 3306 -u root -p dump

# Check table counts
SELECT 
    table_name, 
    table_rows 
FROM information_schema.tables 
WHERE table_schema = 'dump' 
ORDER BY table_rows DESC;

# Check specific table data
SELECT COUNT(*) as total_customers FROM chorki_customers;
SELECT COUNT(*) as total_series FROM chorki_series;
SELECT COUNT(*) as total_seasons FROM chorki_seasons;
SELECT COUNT(*) as total_videos FROM chorki_videos;

# Exit MySQL
exit
```

---

## 🧹 Step 2: Clear Dgraph (Fresh Start)

```bash
# Drop all existing data in Dgraph
curl -X POST localhost:8080/alter -d '{"drop_all": true}'

# Verify Dgraph is empty
curl -X POST localhost:8080/query -H "Content-Type: application/json" -d '{"query": "{ q(func: has(dgraph.type)) { count(uid) } }"}'
```

---

## ⚙️ Step 3: Run Complete Pipeline

```bash
# Navigate to project directory
cd /d/codes/A_GO_PROJECTS/mysql_to_dgraph_pipline

# Run the complete pipeline (extracts all data including customers)
./mysql-dgraph-pipeline -config config/large_scale_test.yaml

# Wait for completion - you should see output like:
# ✅ Processed X tables
# ✅ Generated X records  
# ✅ Wrote X UID mappings
# ✅ Schema file written with X predicates
# ✅ RDF file written: XXX MB
```

---

## 📁 Step 4: Verify Output Files

```bash
# Check generated files
ls -lh output/

# Check RDF file size
ls -lh output/large_scale_data.rdf

# Check schema file
head -20 output/large_scale_schema.txt

# Verify customer data in RDF (should show customer nodes)
grep -c "chorki_customers" output/large_scale_data.rdf

# Verify customer data in schema (should show customer predicates)
grep "chorki_customers" output/large_scale_schema.txt
```

---

## 📤 Step 5: Copy Files to Dgraph Container

```bash
# Copy RDF data file to container
cat /d/codes/A_GO_PROJECTS/mysql_to_dgraph_pipline/output/large_scale_data.rdf | docker exec -i mysql_to_dgraph_pipline-alpha-1 tee /tmp/fresh_data.rdf > /dev/null

# Copy schema file to container  
cat /d/codes/A_GO_PROJECTS/mysql_to_dgraph_pipline/output/large_scale_schema.txt | docker exec -i mysql_to_dgraph_pipline-alpha-1 tee /tmp/fresh_schema.txt > /dev/null

# Verify files copied successfully
docker exec mysql_to_dgraph_pipline-alpha-1 ls -lh /tmp/fresh_*
```

---

## 🚀 Step 6: Import Data Using Dgraph Live Loader

```bash
# Import schema and data in single command
docker exec mysql_to_dgraph_pipline-alpha-1 dgraph live -f /tmp/fresh_data.rdf -s /tmp/fresh_schema.txt --alpha localhost:9080 --zero mysql_to_dgraph_pipline-zero-1:5080

# Alternative: Import schema first, then data
# docker exec mysql_to_dgraph_pipline-alpha-1 dgraph live -s /tmp/fresh_schema.txt --alpha localhost:9080 --zero mysql_to_dgraph_pipline-zero-1:5080
# docker exec mysql_to_dgraph_pipline-alpha-1 dgraph live -f /tmp/fresh_data.rdf --alpha localhost:9080 --zero mysql_to_dgraph_pipline-zero-1:5080
```

**Expected Output:**
```
Processing schema file...
Processed schema file...
Found 1 data file(s) to process
Processing data file...
[timestamp] Elapsed: 05s Txns: XXX N-Quads: XXX N-Quads/s [last 5s]: XXX Aborts: 0
[timestamp] Elapsed: 10s Txns: XXX N-Quads: XXX N-Quads/s [last 5s]: XXX Aborts: 0
...
[timestamp] Total: XXXs. Processed XXX MB. XXX RDF processed.
```

---

## ✅ Step 7: Verify Import Success

### 7.1 Check Total Node Count
```bash
curl -X POST localhost:8080/query -H "Content-Type: application/json" -d '{"query": "{ q(func: has(dgraph.type)) { count(uid) } }"}'
```

### 7.2 Check Data Distribution  
```bash
curl -X POST localhost:8080/query -H "Content-Type: application/json" -d '{"query": "{ total_series(func: eq(dgraph.type, \"chorki_series\")) { count(uid) } total_seasons(func: eq(dgraph.type, \"chorki_seasons\")) { count(uid) } total_videos(func: eq(dgraph.type, \"chorki_videos\")) { count(uid) } total_customers(func: eq(dgraph.type, \"chorki_customers\")) { count(uid) } }"}'
```

### 7.3 Test Relationships
```bash
# Test Series → Seasons → Videos relationship
curl -X POST localhost:8080/query -H "Content-Type: application/json" -d '{"query": "{ guti_series(func: eq(chorki_series.title, \"Guti\")) { uid chorki_series.title ~chorki_seasons.series_id { uid chorki_seasons.title ~chorki_videos.season_id { uid chorki_videos.title } } } }"}'
```

### 7.4 Test Customer Data
```bash
# Check customer data exists
curl -X POST localhost:8080/query -H "Content-Type: application/json" -d '{"query": "{ customers(func: eq(dgraph.type, \"chorki_customers\"), first: 5) { uid chorki_customers.email chorki_customers.name chorki_customers.phone } }"}'
```

---

## 🔧 Step 8: Advanced Testing Queries

### 8.1 Performance Test
```bash
curl -X POST localhost:8080/query -H "Content-Type: application/json" -d '{"query": "{ performance_test(func: eq(dgraph.type, \"chorki_videos\"), first: 100) { uid chorki_videos.title chorki_videos.views season: ~chorki_videos.season_id { uid chorki_seasons.title series: chorki_seasons.series_id { uid chorki_series.title } } } }"}'
```

### 8.2 Analytics Query
```bash
curl -X POST localhost:8080/query -H "Content-Type: application/json" -d '{"query": "{ top_videos(func: eq(dgraph.type, \"chorki_videos\"), orderdesc: chorki_videos.views, first: 10) @filter(gt(chorki_videos.views, 1000)) { uid chorki_videos.title chorki_videos.views } bengali_series(func: eq(chorki_series.language_code, \"bn\")) { count(uid) } english_series(func: eq(chorki_series.language_code, \"en\")) { count(uid) } }"}'
```

### 8.3 Customer Analytics
```bash
curl -X POST localhost:8080/query -H "Content-Type: application/json" -d '{"query": "{ customer_stats(func: eq(dgraph.type, \"chorki_customers\"), first: 10) { uid chorki_customers.email chorki_customers.created_at chorki_customers.status } }"}'
```

---

## 🐛 Troubleshooting

### Container Issues
```bash
# Check running containers
docker ps

# Check container logs
docker logs mysql_to_dgraph_pipline-alpha-1
docker logs mysql_to_dgraph_pipline-zero-1

# Restart containers if needed
docker-compose down && docker-compose up -d
```

### MySQL Connection Issues
```bash
# Test MySQL connection
mysql -h localhost -P 3306 -u root -p dump -e "SELECT 1"

# Check MySQL container status
docker logs mysql_to_dgraph_pipline-mysql-1
```

### Dgraph Issues
```bash
# Check Dgraph health
curl localhost:8080/health

# Check Dgraph state
curl localhost:8080/state
```

### File Issues
```bash
# Check file permissions
ls -la output/

# Check disk space
df -h

# Verify file contents
head output/large_scale_data.rdf
head output/large_scale_schema.txt
```

---

## 📊 Expected Results

After successful completion, you should have:

- **Total Nodes**: ~1M+ (varies by dataset)
- **Series**: ~250+
- **Seasons**: ~270+  
- **Videos**: ~8K+
- **Customers**: ~69K+ (NEW!)
- **Working Relationships**: Series ↔ Seasons ↔ Videos
- **Customer Data**: Accessible and queryable

---

## 🎯 Quick Reference Commands

```bash
# Complete pipeline in one go (after containers are running)
cd /d/codes/A_GO_PROJECTS/mysql_to_dgraph_pipline
curl -X POST localhost:8080/alter -d '{"drop_all": true}'
./mysql-dgraph-pipeline -config config/large_scale_test.yaml
cat output/large_scale_data.rdf | docker exec -i mysql_to_dgraph_pipline-alpha-1 tee /tmp/fresh_data.rdf > /dev/null
cat output/large_scale_schema.txt | docker exec -i mysql_to_dgraph_pipline-alpha-1 tee /tmp/fresh_schema.txt > /dev/null
docker exec mysql_to_dgraph_pipline-alpha-1 dgraph live -f /tmp/fresh_data.rdf -s /tmp/fresh_schema.txt --alpha localhost:9080 --zero mysql_to_dgraph_pipline-zero-1:5080
curl -X POST localhost:8080/query -H "Content-Type: application/json" -d '{"query": "{ q(func: has(dgraph.type)) { count(uid) } }"}'
```

---

## 📝 Notes

1. **Single File Import**: Always use the complete RDF file (not chunks) to preserve relationships
2. **Customer Data**: Ensure customer tables are included in the pipeline config
3. **Performance**: Live loader can handle 500MB+ files efficiently
4. **Relationships**: Foreign keys are automatically mapped to Dgraph relationships
5. **Validation**: Always verify data counts and test relationships after import

---

**Created**: September 19, 2025  
**Pipeline Version**: Production Ready  
**Last Updated**: After successful customer data integration
