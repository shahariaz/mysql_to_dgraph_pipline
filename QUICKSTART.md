# Quick Start Guide

## 🚀 Get Started in 5 Minutes

### Prerequisites
- Go 1.21+
- Docker & Docker Compose
- MySQL database with data

### 1. Setup
```bash
# Clone and setup
git clone <repo>
cd mysql_to_dgraph_pipeline

# Run setup script
chmod +x scripts/setup.sh
./scripts/setup.sh setup
```

### 2. Configure
```bash
# Copy environment file
cp .env.example .env

# Edit with your MySQL credentials
nano .env
```

### 3. Start Infrastructure
```bash
# Start Dgraph and MySQL
docker-compose up -d mysql zero alpha ratel

# Or use make
make infra-start
```

### 4. Run Pipeline
```bash
# Full pipeline (schema + data)
make run

# Or step by step
make run-schema    # Extract and generate schema
make run-data      # Convert and export data
make run-validate  # Validate results
```

### 5. Import to Dgraph
```bash
# Import schema and data
make import

# Or manually
./scripts/import-to-dgraph.sh full
```

## 🎯 Quick Commands

```bash
# Development
make setup         # Initial setup
make build         # Build application
make run-dry       # Dry run test
make clean         # Clean artifacts

# Docker
make docker-run    # Start with Docker
make docker-stop   # Stop services
make docker-logs   # View logs

# Production
make prod-build    # Production build
make benchmark     # Performance test
make health-check  # Check services
```

## 📊 Monitor Progress

- **Dgraph UI**: http://localhost:8000
- **Pipeline Metrics**: http://localhost:8081/metrics
- **Logs**: `docker-compose logs -f pipeline`

## 🔧 Common Issues

**Memory Issues**: Reduce batch size
```bash
./pipeline -batch-size 500 -parallel 2
```

**Connection Issues**: Check Docker services
```bash
docker-compose ps
make health-check
```

**Large Tables**: Process individually
```bash
./pipeline -tables "large_table" -batch-size 100
```

## 📈 Performance Tips

For 1B+ rows:
```yaml
# config/production.yaml
pipeline:
  workers: 8
  batch_size: 5000
  memory_limit_mb: 4096
```

## 🆘 Need Help?

- Check logs: `make docker-logs`
- Health check: `make health-check`
- Documentation: `make docs`
- Issues: GitHub Issues
