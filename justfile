# Display available commands
default:
    @just --list

# Setup database
db-setup:
    #!/usr/bin/env bash
    set -euo pipefail

    DB_HOST="${DB_HOST:-localhost}"
    DB_PORT="${DB_PORT:-5432}"
    DB_USER="${DB_USER:-postgres}"
    DB_PASS="${DB_PASS:-password}"

    echo "Setting up fusillade database..."

    if ! pg_isready -h "$DB_HOST" -p "$DB_PORT" >/dev/null 2>&1; then
        echo "❌ PostgreSQL is not running on $DB_HOST:$DB_PORT"
        exit 1
    fi

    echo "Creating fusillade database..."
    PGPASSWORD="$DB_PASS" psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d postgres -c "CREATE DATABASE fusillade;" 2>/dev/null || echo "  - database already exists"

    echo "Writing .env file..."
    echo "DATABASE_URL=postgres://$DB_USER:$DB_PASS@$DB_HOST:$DB_PORT/fusillade" > .env

    echo "Running migrations..."
    sqlx migrate run

    echo "✅ Database setup complete!"

# Run tests
test *args="":
    cargo test {{args}}

# Lint
lint:
    cargo fmt --check
    cargo clippy -- -D warnings
    cargo sqlx prepare --check

# Format
fmt:
    cargo fmt

# CI pipeline
ci:
    just db-setup
    cargo test
    cargo llvm-cov --lcov --output-path lcov.info
    just lint

# Start Docker PostgreSQL for testing
db-start:
    #!/usr/bin/env bash
    set -euo pipefail

    if docker ps -a --format '{{{{.Names}}}}' | grep -q "^test-postgres$"; then
        if docker ps --format '{{{{.Names}}}}' | grep -q "^test-postgres$"; then
            echo "✅ test-postgres container is already running"
        else
            echo "Starting existing test-postgres container..."
            docker start test-postgres
        fi
    else
        echo "Creating new test-postgres container..."
        docker volume create test-postgres-data >/dev/null 2>&1 || true
        docker run --name test-postgres \
          -e POSTGRES_PASSWORD=password \
          -e POSTGRES_HOST_AUTH_METHOD=trust \
          -p 5432:5432 \
          -v test-postgres-data:/var/lib/postgresql/ \
          -d postgres:latest \
          postgres -c fsync=off
    fi

    echo "Waiting for postgres to be ready..."
    sleep 3

    if pg_isready -h localhost -p 5432 >/dev/null 2>&1; then
        echo "✅ PostgreSQL is ready on localhost:5432"
    else
        echo "❌ PostgreSQL not responding"
        exit 1
    fi

# Stop Docker PostgreSQL
db-stop *args="":
    #!/usr/bin/env bash
    set -euo pipefail

    if docker ps --format '{{{{.Names}}}}' | grep -q "^test-postgres$"; then
        echo "Stopping test-postgres container..."
        docker stop test-postgres

        if [[ "{{args}}" == *"--remove"* ]]; then
            echo "Removing test-postgres container..."
            docker rm test-postgres
            echo "Removing test-postgres-data volume..."
            docker volume rm test-postgres-data 2>/dev/null || echo "  (volume already removed)"
        fi
        echo "✅ Done"
    else
        echo "ℹ️  test-postgres container is not running"
    fi
