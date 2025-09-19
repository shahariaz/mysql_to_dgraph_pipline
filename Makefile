# Makefile for MySQL to Dgraph Pipeline
# =====================================

.PHONY: help build test clean run deps docker-build docker-run setup

# Variables
BINARY_NAME=pipeline
OUTPUT_DIR=output
CONFIG_FILE=config/config.yaml

# Default target
help:
	@echo "MySQL to Dgraph Pipeline"
	@echo "========================"
	@echo ""
	@echo "Available targets:"
	@echo "  build         Build the application"
	@echo "  test          Run tests"
	@echo "  clean         Clean build artifacts"
	@echo "  run           Run the pipeline"
	@echo "  run-schema    Run schema extraction only"
	@echo "  run-data      Run data migration only"
	@echo "  run-validate  Run validation only"
	@echo "  deps          Install dependencies"
	@echo "  setup         Complete setup"
	@echo "  docker-build  Build Docker image"
	@echo "  docker-run    Run with Docker Compose"
	@echo "  docker-stop   Stop Docker services"
	@echo "  import        Import data to Dgraph"
	@echo "  monitor       Start monitoring stack"
	@echo "  lint          Run linters"
	@echo "  fmt           Format code"

# Build the application
build:
	@echo "Building $(BINARY_NAME)..."
	go build -o $(BINARY_NAME) cmd/main.go
	@echo "Build completed: $(BINARY_NAME)"

# Install dependencies
deps:
	@echo "Installing dependencies..."
	go mod tidy
	go mod download

# Run tests
test:
	@echo "Running tests..."
	go test -v ./...

# Format code
fmt:
	@echo "Formatting code..."
	go fmt ./...

# Lint code
lint: fmt
	@echo "Running linters..."
	golangci-lint run

# Clean build artifacts
clean:
	@echo "Cleaning..."
	rm -f $(BINARY_NAME)
	rm -rf $(OUTPUT_DIR)/*
	go clean

# Setup directories and config
setup:
	@echo "Setting up project..."
	mkdir -p $(OUTPUT_DIR)
	mkdir -p logs
	@if [ ! -f .env ]; then \
		echo "Creating default .env file..."; \
		cp .env.example .env 2>/dev/null || \
		echo "MYSQL_HOST=localhost\nMYSQL_USER=root\nMYSQL_PASSWORD=root\nMYSQL_DATABASE=dump" > .env; \
	fi
	@echo "Setup completed"

# Run pipeline modes
run: build
	./$(BINARY_NAME) -config $(CONFIG_FILE) -mode full

run-schema: build
	./$(BINARY_NAME) -config $(CONFIG_FILE) -mode schema

run-data: build
	./$(BINARY_NAME) -config $(CONFIG_FILE) -mode data

run-validate: build
	./$(BINARY_NAME) -config $(CONFIG_FILE) -mode validate

run-dry: build
	./$(BINARY_NAME) -config $(CONFIG_FILE) -mode full -dry-run

# Docker targets
docker-build:
	@echo "Building Docker image..."
	docker build -t mysql-dgraph-pipeline .

docker-run:
	@echo "Starting services with Docker Compose..."
	docker-compose up -d

docker-stop:
	@echo "Stopping Docker services..."
	docker-compose down

docker-logs:
	docker-compose logs -f pipeline

# Start infrastructure only
infra-start:
	@echo "Starting infrastructure..."
	docker-compose up -d mysql zero alpha ratel

infra-stop:
	@echo "Stopping infrastructure..."
	docker-compose stop mysql zero alpha ratel

# Import data to Dgraph
import:
	@echo "Importing data to Dgraph..."
	./scripts/import-to-dgraph.sh full

import-schema:
	./scripts/import-to-dgraph.sh schema

import-data:
	./scripts/import-to-dgraph.sh data

# Monitoring
monitor:
	@echo "Starting monitoring stack..."
	docker-compose --profile monitoring up -d

monitor-stop:
	docker-compose --profile monitoring down

# Development helpers
dev-reset: clean docker-stop
	@echo "Resetting development environment..."
	docker-compose down -v
	rm -rf $(OUTPUT_DIR)/*

dev-full: setup build infra-start
	@echo "Waiting for infrastructure..."
	sleep 30
	@$(MAKE) run

# Production targets
prod-build:
	@echo "Building for production..."
	CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -ldflags '-w' -o $(BINARY_NAME) cmd/main.go

prod-deploy: prod-build docker-build
	@echo "Deploying to production..."
	docker-compose -f docker-compose.prod.yml up -d

# Backup and restore
backup:
	@echo "Creating backup..."
	mkdir -p backups/$(shell date +%Y%m%d_%H%M%S)
	cp -r $(OUTPUT_DIR)/* backups/$(shell date +%Y%m%d_%H%M%S)/

# Database operations
db-dump:
	@echo "Dumping MySQL database..."
	docker-compose exec mysql mysqldump -u root -proot dump > backup_$(shell date +%Y%m%d_%H%M%S).sql

db-restore:
	@echo "Please specify backup file: make db-restore FILE=backup.sql"
	@if [ -n "$(FILE)" ]; then \
		docker-compose exec -T mysql mysql -u root -proot dump < $(FILE); \
	fi

# Benchmarking
benchmark:
	@echo "Running performance benchmark..."
	./$(BINARY_NAME) -config config/production.yaml -mode data -tables users -batch-size 5000

# Health checks
health-check:
	@echo "Checking service health..."
	@curl -f http://localhost:3306 2>/dev/null && echo "MySQL: OK" || echo "MySQL: FAIL"
	@curl -f http://localhost:8080/health 2>/dev/null && echo "Dgraph: OK" || echo "Dgraph: FAIL"
	@curl -f http://localhost:8081/metrics 2>/dev/null && echo "Pipeline: OK" || echo "Pipeline: FAIL"

# Documentation
docs:
	@echo "Generating documentation..."
	godoc -http=:6060 &
	@echo "Documentation available at http://localhost:6060"

# Install tools
install-tools:
	@echo "Installing development tools..."
	go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest
	go install golang.org/x/tools/cmd/godoc@latest
