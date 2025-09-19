#!/bin/bash
# Quick Import Script - Copy and Import existing RDF files to Dgraph
# Usage: ./quick_import.sh

set -e

echo "📤 Copying files to Dgraph container..."

# Copy RDF file
echo "  → Copying RDF data file..."
cat output/large_scale_data.rdf | docker exec -i mysql_to_dgraph_pipline-alpha-1 tee /tmp/fresh_data.rdf > /dev/null

# Copy schema file  
echo "  → Copying schema file..."
cat output/large_scale_schema.txt | docker exec -i mysql_to_dgraph_pipline-alpha-1 tee /tmp/fresh_schema.txt > /dev/null

echo "✅ Files copied successfully"

echo "🚀 Importing to Dgraph using live loader..."
docker exec mysql_to_dgraph_pipline-alpha-1 dgraph live -f /tmp/fresh_data.rdf -s /tmp/fresh_schema.txt --alpha localhost:9080 --zero mysql_to_dgraph_pipline-zero-1:5080

echo "✅ Import completed!"

echo "📊 Quick verification:"
curl -s -X POST localhost:8080/query -H "Content-Type: application/json" -d '{"query": "{ q(func: has(dgraph.type)) { count(uid) } }"}' | jq -r '"Total nodes: " + .data.q[0].count'
