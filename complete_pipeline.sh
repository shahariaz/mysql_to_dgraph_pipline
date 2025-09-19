#!/bin/bash
# Complete MySQL to Dgraph Pipeline Script
# Usage: ./complete_pipeline.sh

set -e  # Exit on any error

echo "🚀 Starting Complete MySQL to Dgraph Pipeline..."

# Step 1: Clear Dgraph
echo "🧹 Step 1: Clearing Dgraph..."
curl -X POST localhost:8080/alter -d '{"drop_all": true}'
echo "✅ Dgraph cleared"

# Step 2: Run Pipeline
echo "⚙️ Step 2: Running pipeline from MySQL..."
./mysql-dgraph-pipeline -config config/large_scale_test.yaml
echo "✅ Pipeline completed"

# Step 3: Copy files to container
echo "📤 Step 3: Copying files to Dgraph container..."
cat output/large_scale_data.rdf | docker exec -i mysql_to_dgraph_pipline-alpha-1 tee /tmp/fresh_data.rdf > /dev/null
cat output/large_scale_schema.txt | docker exec -i mysql_to_dgraph_pipline-alpha-1 tee /tmp/fresh_schema.txt > /dev/null
echo "✅ Files copied to container"

# Step 4: Import to Dgraph
echo "🚀 Step 4: Importing data using live loader..."
docker exec mysql_to_dgraph_pipline-alpha-1 dgraph live -f /tmp/fresh_data.rdf -s /tmp/fresh_schema.txt --alpha localhost:9080 --zero mysql_to_dgraph_pipline-zero-1:5080
echo "✅ Data imported to Dgraph"

# Step 5: Verify import
echo "✅ Step 5: Verifying import..."
echo "Total nodes imported:"
curl -s -X POST localhost:8080/query -H "Content-Type: application/json" -d '{"query": "{ q(func: has(dgraph.type)) { count(uid) } }"}' | jq -r '.data.q[0].count'

echo "Data distribution:"
curl -s -X POST localhost:8080/query -H "Content-Type: application/json" -d '{"query": "{ total_series(func: eq(dgraph.type, \"chorki_series\")) { count(uid) } total_seasons(func: eq(dgraph.type, \"chorki_seasons\")) { count(uid) } total_videos(func: eq(dgraph.type, \"chorki_videos\")) { count(uid) } total_customers(func: eq(dgraph.type, \"chorki_customers\")) { count(uid) } }"}' | jq -r '
"Series: " + (.data.total_series[0].count | tostring) + 
", Seasons: " + (.data.total_seasons[0].count | tostring) + 
", Videos: " + (.data.total_videos[0].count | tostring) + 
", Customers: " + (.data.total_customers[0].count | tostring)'

echo "🎉 Pipeline completed successfully!"
echo "📊 You can now test queries in Ratel UI at http://localhost:8000"
