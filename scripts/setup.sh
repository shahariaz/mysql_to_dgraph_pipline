#!/bin/bash

# MySQL to Dgraph Pipeline Setup Script
# =====================================

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUTPUT_DIR="$PROJECT_DIR/output"

# Functions
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check prerequisites
check_prerequisites() {
    log_info "Checking prerequisites..."
    
    # Check Go
    if ! command -v go &> /dev/null; then
        log_error "Go is not installed. Please install Go 1.21 or higher."
        exit 1
    fi
    
    GO_VERSION=$(go version | grep -oE '[0-9]+\.[0-9]+' | head -1)
    log_info "Found Go version: $GO_VERSION"
    
    # Check Docker
    if ! command -v docker &> /dev/null; then
        log_warning "Docker is not installed. Docker features will be unavailable."
    else
        log_info "Found Docker: $(docker --version)"
    fi
    
    # Check Docker Compose
    if ! command -v docker-compose &> /dev/null; then
        log_warning "Docker Compose is not installed. Docker features will be unavailable."
    else
        log_info "Found Docker Compose: $(docker-compose --version)"
    fi
}

# Setup directories
setup_directories() {
    log_info "Setting up directories..."
    
    mkdir -p "$OUTPUT_DIR"
    mkdir -p "$PROJECT_DIR/logs"
    mkdir -p "$PROJECT_DIR/config"
    
    log_success "Directories created"
}

# Install dependencies
install_dependencies() {
    log_info "Installing Go dependencies..."
    
    cd "$PROJECT_DIR"
    go mod tidy
    
    log_success "Dependencies installed"
}

# Build application
build_application() {
    log_info "Building application..."
    
    cd "$PROJECT_DIR"
    go build -o pipeline cmd/main.go
    
    if [ -f "pipeline" ]; then
        log_success "Application built successfully"
        chmod +x pipeline
    else
        log_error "Build failed"
        exit 1
    fi
}

# Setup configuration
setup_configuration() {
    log_info "Setting up configuration..."
    
    # Create default .env if it doesn't exist
    if [ ! -f "$PROJECT_DIR/.env" ]; then
        cat > "$PROJECT_DIR/.env" << EOF
# MySQL Configuration
MYSQL_ROOT_PASSWORD=root
MYSQL_DATABASE=dump
MYSQL_USER=user
MYSQL_PASSWORD=password

# Pipeline Configuration
PIPELINE_WORKERS=4
PIPELINE_BATCH_SIZE=1000
LOG_LEVEL=info
OUTPUT_DIR=output
EOF
        log_success "Created default .env file"
    fi
}

# Test MySQL connection
test_mysql_connection() {
    log_info "Testing MySQL connection..."
    
    # Load environment variables
    if [ -f "$PROJECT_DIR/.env" ]; then
        source "$PROJECT_DIR/.env"
    fi
    
    MYSQL_HOST=${MYSQL_HOST:-localhost}
    MYSQL_PORT=${MYSQL_PORT:-3306}
    MYSQL_USER=${MYSQL_USER:-user}
    MYSQL_PASSWORD=${MYSQL_PASSWORD:-password}
    
    # Try to connect to MySQL
    if command -v mysql &> /dev/null; then
        if mysql -h "$MYSQL_HOST" -P "$MYSQL_PORT" -u "$MYSQL_USER" -p"$MYSQL_PASSWORD" -e "SELECT 1" &> /dev/null; then
            log_success "MySQL connection successful"
        else
            log_warning "MySQL connection failed. Make sure MySQL is running and credentials are correct."
        fi
    else
        log_warning "MySQL client not found. Cannot test connection."
    fi
}

# Start infrastructure with Docker
start_infrastructure() {
    log_info "Starting infrastructure with Docker..."
    
    if command -v docker-compose &> /dev/null; then
        cd "$PROJECT_DIR"
        docker-compose up -d mysql zero alpha ratel
        
        log_info "Waiting for services to be ready..."
        sleep 30
        
        log_success "Infrastructure started"
        log_info "Services available at:"
        echo "  - MySQL: localhost:3306"
        echo "  - Dgraph Alpha: localhost:8080"
        echo "  - Dgraph Ratel: http://localhost:8000"
    else
        log_error "Docker Compose not available"
        exit 1
    fi
}

# Run pipeline test
run_test() {
    log_info "Running pipeline test..."
    
    cd "$PROJECT_DIR"
    ./pipeline -config config/config.yaml -mode schema -dry-run
    
    if [ $? -eq 0 ]; then
        log_success "Pipeline test completed successfully"
    else
        log_error "Pipeline test failed"
        exit 1
    fi
}

# Show usage
show_usage() {
    echo "Usage: $0 [OPTION]"
    echo ""
    echo "Options:"
    echo "  setup        Complete setup (default)"
    echo "  deps         Install dependencies only"
    echo "  build        Build application only"
    echo "  config       Setup configuration only"
    echo "  test         Test MySQL connection"
    echo "  start        Start infrastructure"
    echo "  run-test     Run pipeline test"
    echo "  help         Show this help"
    echo ""
    echo "Examples:"
    echo "  $0 setup      # Complete setup"
    echo "  $0 start      # Start Docker infrastructure"
    echo "  $0 test       # Test MySQL connection"
}

# Main execution
main() {
    case "${1:-setup}" in
        "setup")
            log_info "Starting complete setup..."
            check_prerequisites
            setup_directories
            setup_configuration
            install_dependencies
            build_application
            log_success "Setup completed successfully!"
            echo ""
            echo "Next steps:"
            echo "1. Edit .env file with your MySQL credentials"
            echo "2. Run: ./scripts/setup.sh start (to start infrastructure)"
            echo "3. Run: ./pipeline -config config/config.yaml -mode full"
            ;;
        "deps")
            install_dependencies
            ;;
        "build")
            build_application
            ;;
        "config")
            setup_configuration
            ;;
        "test")
            test_mysql_connection
            ;;
        "start")
            start_infrastructure
            ;;
        "run-test")
            run_test
            ;;
        "help")
            show_usage
            ;;
        *)
            log_error "Unknown option: $1"
            show_usage
            exit 1
            ;;
    esac
}

# Run main function
main "$@"
