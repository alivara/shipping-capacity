#!/bin/bash

################################################################################
# Shipping Capacity API - Deployment Script
################################################################################

set -e  # Exit on error

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

################################################################################
# Helper Functions
################################################################################

print_header() { echo -e "${BLUE}========================================\n$1\n========================================${NC}"; }
print_success() { echo -e "${GREEN}✓ $1${NC}"; }
print_error() { echo -e "${RED}✗ $1${NC}"; }
print_warning() { echo -e "${YELLOW}⚠ $1${NC}"; }
print_info() { echo -e "${BLUE}ℹ $1${NC}"; }

check_poetry() {
    if ! command -v poetry &> /dev/null; then
        print_error "Poetry not installed! Install: curl -sSL https://install.python-poetry.org | python3 -"
        exit 1
    fi
}

check_docker_compose() {
    if ! docker compose version &> /dev/null; then
        print_error "Docker Compose not installed!"
        exit 1
    fi
}

wait_for_db() {
    print_info "Waiting for database..."
    for i in {1..30}; do
        if docker compose -f docker-compose.local.yml exec -T postgres pg_isready -U admin &> /dev/null; then
            print_success "Database ready!"
            return 0
        fi
        echo -n "."
        sleep 1
    done
    print_error "Database timeout"
    return 1
}

################################################################################
# Development Commands
################################################################################

cmd_install() {
    print_header "Installing Dependencies"
    check_poetry
    poetry install
    print_success "Dependencies installed!"
}

cmd_run() {
    print_header "Starting Application (Local)"
    check_poetry
    check_docker_compose

    docker compose -f docker-compose.local.yml up -d postgres || true
    wait_for_db

    print_info "Starting FastAPI at http://localhost:8000"
    print_info "Docs: http://localhost:8000/docs"
    poetry run uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
}

################################################################################
# Database Commands
################################################################################

cmd_db_start() {
    print_header "Starting Database"
    check_docker_compose
    docker compose -f docker-compose.local.yml up -d postgres
    wait_for_db
    print_success "Database running at localhost:5432"
}

cmd_db_stop() {
    print_header "Stopping Database"
    check_docker_compose
    docker compose -f docker-compose.local.yml stop postgres
    print_success "Database stopped!"
}

cmd_db_restart() {
    cmd_db_stop
    sleep 2
    cmd_db_start
}

cmd_db_reset() {
    print_header "Resetting Database"
    print_warning "This will DELETE all data!"
    read -p "Continue? (y/N): " -n 1 -r
    echo
    [[ ! $REPLY =~ ^[Yy]$ ]] && exit 0

    check_docker_compose
    docker compose -f docker-compose.local.yml down -v postgres
    cmd_db_start
    print_success "Database reset!"
}

cmd_db_shell() {
    print_header "Database Shell"
    check_docker_compose
    docker compose -f docker-compose.local.yml exec postgres psql -U admin -d local
}

cmd_db_logs() {
    check_docker_compose
    docker compose -f docker-compose.local.yml logs -f postgres
}

################################################################################
# Testing Commands
################################################################################

cmd_test() {
    print_header "Running Tests (Docker - No Poetry Required)"
    check_docker_compose

    print_info "Building app image..."
    docker compose -f docker-compose.local.yml build app-fastapi

    print_info "Starting database..."
    docker compose -f docker-compose.local.yml up -d postgres
    wait_for_db

    print_info "Running tests in Docker..."
    docker compose -f docker-compose.local.yml run --rm \
        -e DATABASE_POSTGRESQL_DB=test_db \
        -e APP_ENV=test \
        -v "$(pwd)/htmlcov:/app/htmlcov" \
        app-fastapi \
        pytest app/tests/ -v

    local EXIT_CODE=$?

    [ $EXIT_CODE -eq 0 ] && print_success "All tests passed!" || print_error "Tests failed!"
    exit $EXIT_CODE
}

cmd_test_local() {
    print_header "Running Tests Locally (Poetry Required)"
    check_poetry
    check_docker_compose

    docker compose -f docker-compose.local.yml up -d postgres || true
    wait_for_db

    print_info "Running tests locally..."
    poetry run pytest app/tests/ -v

    local EXIT_CODE=$?
    [ $EXIT_CODE -eq 0 ] && print_success "All tests passed!" || print_error "Tests failed!"
    exit $EXIT_CODE
}

cmd_test_unit() {
    print_header "Running Unit Tests (Docker)"
    check_docker_compose

    print_info "Building app image..."
    docker compose -f docker-compose.local.yml build app-fastapi

    print_info "Running unit tests (no DB)..."
    docker compose -f docker-compose.local.yml run --rm --no-deps \
        -e APP_ENV=test \
        app-fastapi \
        pytest app/tests/ -m unit -v

    [ $? -eq 0 ] && print_success "Unit tests passed!" || print_error "Tests failed!"
}

cmd_test_coverage() {
    print_header "Running Tests with Coverage (Docker)"
    check_docker_compose

    print_info "Building app image..."
    docker compose -f docker-compose.local.yml build app-fastapi

    print_info "Starting database..."
    docker compose -f docker-compose.local.yml up -d postgres
    wait_for_db

    print_info "Running tests with coverage..."
    docker compose -f docker-compose.local.yml run --rm \
        -e DATABASE_POSTGRESQL_DB=test_db \
        -e APP_ENV=test \
        -v "$(pwd)/htmlcov:/app/htmlcov" \
        app-fastapi \
        pytest app/tests/ --cov=app --cov-report=html --cov-report=term

    [ $? -eq 0 ] && print_success "Coverage: htmlcov/index.html" || print_error "Tests failed!"
}

################################################################################
# Docker Commands
################################################################################

cmd_docker_build() {
    print_header "Building Docker Image"
    docker compose -f docker-compose.local.yml build app-fastapi
    print_success "Image built!"
}

cmd_docker_up() {
    print_header "Starting All Services"
    check_docker_compose

    print_info "Building image..."
    docker compose -f docker-compose.local.yml build app-fastapi

    print_info "Starting services..."
    docker compose -f docker-compose.local.yml up -d
    wait_for_db

    print_success "Services started!"
    print_info "API: http://localhost:80"
    print_info "Docs: http://localhost:80/docs"
    print_info "PgAdmin: http://localhost:15432"
}

cmd_docker_down() {
    print_header "Stopping Services"
    check_docker_compose
    docker compose -f docker-compose.local.yml down
    print_success "Services stopped!"
}

cmd_docker_logs() {
    check_docker_compose
    docker compose -f docker-compose.local.yml logs -f "$@"
}

################################################################################
# Utility Commands
################################################################################

cmd_clean() {
    print_header "Cleaning Project"
    find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
    find . -type f -name "*.pyc" -delete 2>/dev/null || true
    find . -type d -name "*.egg-info" -exec rm -rf {} + 2>/dev/null || true
    rm -rf .pytest_cache htmlcov .coverage build dist 2>/dev/null || true
    print_success "Project cleaned!"
}

cmd_logs() {
    [ -f "logs/app.log" ] && tail -f logs/app.log || print_error "Log file not found"
}

cmd_status() {
    print_header "Service Status"
    echo -e "\n${BLUE}Docker Services:${NC}"
    docker compose -f docker-compose.local.yml ps 2>/dev/null || echo "  No services running"
}

cmd_lint() {
    print_header "Running Linters"
    check_poetry
    poetry run ruff check app/ || true
    poetry run mypy app/ || true
}

cmd_format() {
    print_header "Formatting Code"
    check_poetry
    poetry run black app/ app/tests/
    poetry run isort app/ app/tests/
    print_success "Code formatted!"
}

################################################################################
# Help
################################################################################

cmd_help() {
    cat << EOF

${BLUE}Shipping Capacity API - Deployment Script${NC}

${GREEN}Development:${NC}
  install           Install dependencies
  run               Start local dev server (hot reload)
  shell             Open Poetry shell

${GREEN}Database:${NC}
  db:start          Start PostgreSQL
  db:stop           Stop PostgreSQL
  db:restart        Restart PostgreSQL
  db:reset          Reset database (deletes all data)
  db:shell          Open psql shell
  db:logs           View database logs

${GREEN}Testing:${NC}
  test              Run all tests ⚡
  test:unit         Run unit tests only (no DB)
  test:coverage     Run with coverage report

${GREEN}Docker:${NC}
  docker:build      Build image
  docker:up         Build + start all services
  docker:down       Stop all services
  docker:logs       View logs

${GREEN}Utilities:${NC}
  clean             Remove cache files
  lint              Run linters
  format            Format code
  status            Show service status
  help              Show this help

${GREEN}Examples:${NC}
  ./deploy.sh db:start       # Start database
  ./deploy.sh test           # Run all tests
  ./deploy.sh test:coverage  # Generate coverage
  ./deploy.sh docker:up      # Start all services
  ./deploy.sh run            # Local dev server

${YELLOW}Tip:${NC} Use ${GREEN}poetry run pytest -v${NC} for fastest tests during development

EOF
}

################################################################################
# Main Router
################################################################################

main() {
    case "${1:-help}" in
        # Development
        install) cmd_install ;;
        run|start) cmd_run ;;
        shell) cmd_shell ;;

        # Database
        db:start) cmd_db_start ;;
        db:stop) cmd_db_stop ;;
        db:restart) cmd_db_restart ;;
        db:reset) cmd_db_reset ;;
        db:shell|db:psql) cmd_db_shell ;;
        db:logs) cmd_db_logs ;;

        # Testing
        test|test:all) cmd_test ;;
        test:local|test:dev) cmd_test_local ;;
        test:unit) cmd_test_unit ;;
        test:coverage|coverage) cmd_test_coverage ;;

        # Docker
        docker:build|build) cmd_docker_build ;;
        docker:up|up) cmd_docker_up ;;
        docker:down|down) cmd_docker_down ;;
        docker:logs) shift; cmd_docker_logs "$@" ;;

        # Utilities
        clean) cmd_clean ;;
        lint) cmd_lint ;;
        format) cmd_format ;;
        logs) cmd_logs ;;
        status) cmd_status ;;

        # Help
        help|--help|-h) cmd_help ;;

        *)
            print_error "Unknown command: $1"
            echo "Run './deploy.sh help' for usage"
            exit 1
            ;;
    esac
}

main "$@"
