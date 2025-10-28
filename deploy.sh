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

cmd_run() {
    print_header "Starting Application (Local)"
    check_docker_compose

    # Start database if not running
    if ! docker compose -f docker-compose.local.yml ps postgres | grep -q "Up"; then
        print_info "Starting database..."
        docker compose -f docker-compose.local.yml up -d postgres
        wait_for_db
    else
        print_info "Database already running"
    fi

    # Start app
    print_info "Starting FastAPI at http://localhost"
    print_info "Docs: http://localhost/docs"
    docker compose -f docker-compose.local.yml up -d app-fastapi

    print_info "Waiting for app to be healthy..."
    # Wait for health endpoint with retry (30 second timeout)
    for i in {1..30}; do
        if curl -f -s http://localhost/health > /dev/null 2>&1; then
            print_success "App is healthy!"
            break
        fi
        if [ $i -eq 30 ]; then
            print_error "App health check timeout after 30 seconds!"
            print_info "Check logs: docker compose -f docker-compose.local.yml logs app-fastapi"
            exit 1
        fi
        echo -n "."
        sleep 1
    done

    # Add ETL data loading
    print_info "Loading data..."
    cmd_etl_docker_load "data/sailing_level_raw.csv"
    print_success "Data loaded!"

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
    print_warning "This will DELETE all data and volumes!"
    print_warning "All sailing data will be lost!"
    echo
    read -p "Type 'yes' to confirm: " -r
    echo
    [[ ! $REPLY == "yes" ]] && { print_info "Reset cancelled"; exit 0; }

    check_docker_compose
    print_info "Stopping and removing database..."
    docker compose -f docker-compose.local.yml down -v postgres

    print_info "Starting fresh database..."
    cmd_db_start

    print_success "Database reset complete!"
    print_info ""
    print_info "Next steps:"
    print_info "  1. Run migrations: ${GREEN}alembic upgrade head${NC}"
    print_info "  2. Load data:      ${GREEN}./deploy.sh etl:load${NC}"
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
    if ! docker compose -f docker-compose.local.yml build app-fastapi; then
        print_error "Build failed!"
        exit 1
    fi

    print_info "Starting services..."
    if ! docker compose -f docker-compose.local.yml up -d; then
        print_error "Failed to start services!"
        exit 1
    fi

    wait_for_db
    sleep 3  # Give app time to start

    # Verify services are running
    print_info "Verifying services..."
    RUNNING=$(docker compose -f docker-compose.local.yml ps --status running | grep -c "Up" || echo "0")

    if [ "$RUNNING" -gt 0 ]; then
        print_success "Services started! ($RUNNING containers running)"
        print_info ""
        print_info "Available endpoints:"
        print_info "  API:     ${GREEN}http://localhost:80${NC}"
        print_info "  Docs:    ${GREEN}http://localhost:80/docs${NC}"
        print_info "  PgAdmin: ${GREEN}http://localhost:15432${NC}"
        print_info ""
        print_info "Next steps:"
        print_info "  1. Check status: ${GREEN}./deploy.sh status${NC}"
        print_info "  2. Check data:   ${GREEN}./deploy.sh etl:status${NC}"
        print_info "  3. Load data:    ${GREEN}./deploy.sh etl:load${NC}"
        print_info "  4. View logs:    ${GREEN}./deploy.sh docker:logs${NC}"
    else
        print_error "Services failed to start!"
        print_info "Check logs with: ./deploy.sh docker:logs"
        exit 1
    fi
}

cmd_docker_down() {
    print_header "Stopping Services"
    check_docker_compose
    docker compose -f docker-compose.local.yml down
    print_success "Services stopped!"
}


################################################################################
# ETL Commands
################################################################################
cmd_etl_status() {
    print_header "ETL Status"
    check_docker_compose

    docker compose -f docker-compose.local.yml run --rm app-fastapi \
        python scripts/etl_manager.py status
}

cmd_etl_docker_load() {
    print_header "ETL Load Data (Docker)"
    check_docker_compose

    local CSV_PATH="${1:-data/sailing_level_raw.csv}"

    print_info "Loading from: $CSV_PATH (inside container)"

    # Run ETL load in Docker container
    if docker compose -f docker-compose.local.yml run --rm app-fastapi \
        python scripts/etl_manager.py load --csv-path "$CSV_PATH"; then
        print_success "Data loaded successfully!"
        print_info "Check status with: ${GREEN}./deploy.sh etl:status${NC}"
    else
        print_error "Data loading failed!"
        print_info "Possible causes:"
        print_info "  - CSV file not found in container at: $CSV_PATH"
        print_info "  - Data already exists (use --force or clear first)"
        print_info "  - Database connection issue"
        print_info ""
        print_info "Check logs: docker compose -f docker-compose.local.yml logs app-fastapi"
        exit 1
    fi
}

cmd_etl_docker_refresh() {
    print_header "ETL Refresh Data (Docker)"
    check_docker_compose

    local CSV_PATH="${1:-data/sailing_level_raw.csv}"

    # Check if CSV file exists
    if [ ! -f "$CSV_PATH" ]; then
        print_error "CSV file not found: $CSV_PATH"
        exit 1
    fi

    print_warning "This will CLEAR and RELOAD all data!"
    print_info "CSV file: $CSV_PATH"
    print_info "File size: $(du -h "$CSV_PATH" | cut -f1)"
    echo

    read -p "Continue? (y/N): " -n 1 -r
    echo
    [[ ! $REPLY =~ ^[Yy]$ ]] && { print_info "Refresh cancelled"; exit 0; }

    if docker compose -f docker-compose.local.yml run --rm app-fastapi \
        python scripts/etl_manager.py refresh --csv-path "$CSV_PATH"; then
        print_success "Data refreshed successfully!"
        print_info "Check status with: ${GREEN}./deploy.sh etl:status${NC}"
    else
        print_error "Data refresh failed!"
        print_warning "Table may be in an inconsistent state"
        print_info "Try: ${GREEN}./deploy.sh etl:load${NC}"
        exit 1
    fi
}

################################################################################
# Help
################################################################################

cmd_help() {
    cat << EOF

${BLUE}╔════════════════════════════════════════╗
║  Shipping Capacity API - Deploy Script  ║
╚════════════════════════════════════════╝${NC}

${GREEN}🚀 Quick Start (First Time Users):${NC}
  quickstart        One-command setup (Docker + Data + Services)

${GREEN}Development:${NC}
  run               Start dev server (port 80)
  shell             Open Poetry shell

${GREEN}Database:${NC}
  db:start          Start PostgreSQL
  db:stop           Stop PostgreSQL
  db:restart        Restart PostgreSQL
  db:reset          Reset database (⚠️  deletes all data)


${GREEN}ETL (Data Management):${NC}
  etl:status        Check data status (rows, size, dates)
  etl:load [path]   Load CSV data (default: data/sailing_level_raw.csv)
  etl:refresh       Clear + reload all data

${GREEN}Testing:${NC}
  test              Run all tests ⚡
  test:unit         Run unit tests only (no DB)
  test:coverage     Generate coverage report

${GREEN}Docker:${NC}
  docker:build      Build application image
  docker:up         Build + start all services (port 80)
  docker:down       Stop all services
  docker:logs       View service logs

${BLUE}═══════════════════════════════════════${NC}
${GREEN}Common Workflows:${NC}

  ${YELLOW}First Time Setup:${NC}
    ${GREEN}./deploy.sh quickstart${NC}

  ${YELLOW}Daily Development:${NC}
    1. ${GREEN}./deploy.sh run${NC}              # Start app (fast!)
    2. ${GREEN}./deploy.sh etl:status${NC}       # Check data
    3. Make changes, app auto-reloads ⚡
    4. ${GREEN}./deploy.sh test${NC}             # Run tests

  ${YELLOW}Data Refresh:${NC}
    ${GREEN}./deploy.sh etl:refresh${NC}         # While app runs!

  ${YELLOW}Production-Like:${NC}
    1. ${GREEN}./deploy.sh docker:up${NC}        # All services
    2. ${GREEN}./deploy.sh etl:load${NC}         # Load data
    3. API at ${GREEN}http://localhost:80${NC}

${BLUE}═══════════════════════════════════════${NC}
${YELLOW}💡 Tips:${NC}
  • Data loading is ${GREEN}separate${NC} from app startup
  • Use ${GREEN}status${NC} to check what's running
  • ${GREEN}etl:refresh${NC} works while app is running!
  • Check ${GREEN}etl:status${NC} before loading data

EOF
}

################################################################################
# Main Router
################################################################################

main() {
    case "${1:-help}" in
        # Development
        run|start) cmd_run ;;
        shell) cmd_shell ;;

        # Database
        db:start) cmd_db_start ;;
        db:stop) cmd_db_stop ;;
        db:restart) cmd_db_restart ;;
        db:reset) cmd_db_reset ;;

        # ETL
        etl:status) cmd_etl_status ;;
        etl:load) shift; cmd_etl_docker_load "$@" ;;
        etl:refresh) shift; cmd_etl_docker_refresh "$@" ;;

        # Testing
        test|test:all) cmd_test ;;
        test:unit) cmd_test_unit ;;
        test:coverage|coverage) cmd_test_coverage ;;

        # Docker
        docker:build|build) cmd_docker_build ;;
        docker:up|up) cmd_docker_up ;;
        docker:down|down) cmd_docker_down ;;

        # Help
        help|--help|-h|"") cmd_help ;;

        *)
            print_error "Unknown command: $1"
            echo ""
            echo "Run '${GREEN}./deploy.sh help${NC}' for usage"
            exit 1
            ;;
    esac
}

main "$@"
