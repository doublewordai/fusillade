# Fusillade Development Guide

## Overview

Fusillade is a Rust library for batching HTTP requests with retry logic and per-model concurrency control. Requests are stored persistently in PostgreSQL and processed by a background daemon.

**Key Concepts:**
- **Files** - Collections of request templates
- **Request Templates** - Define HTTP requests (endpoint, method, body, API key)
- **Batches** - Snapshots of all templates in a file, triggering execution
- **Requests** - Created from templates, one per batch, progressing through states

## Repository Structure

```
fusillade/
├── src/
│   ├── lib.rs              # Library entry point
│   ├── error.rs            # Error types
│   ├── http.rs             # HTTP request execution (~27k lines)
│   ├── batch/              # Batch management
│   ├── daemon/             # Background daemon logic
│   ├── manager/            # Request manager implementations
│   └── request/            # Request state machine
├── migrations/             # SQLx database migrations
├── tests/
│   └── integration.rs      # Integration tests
├── justfile               # Build commands and workflows
└── Cargo.toml
```

## Development Setup

### Prerequisites

- Rust (2024 edition)
- PostgreSQL
- Docker (optional, for test database)
- `just` command runner
- `sqlx-cli` for migrations

### Initial Setup

1. **Start PostgreSQL** (using Docker):
   ```bash
   just db-start
   ```

2. **Create database and run migrations**:
   ```bash
   just db-setup
   ```

   This creates the `fusillade` database and writes connection string to `.env`:
   ```
   DATABASE_URL=postgres://postgres:password@localhost:5432/fusillade
   ```

3. **Run tests**:
   ```bash
   just test
   ```

### Common Workflows

#### Running Tests
```bash
# All tests
just test

# Specific test
just test integration_test_name

# With test output
cargo test -- --nocapture
```

#### Code Quality
```bash
# Format code
just fmt

# Run linters
just lint

# Full CI pipeline (setup DB, test, coverage, lint)
just ci
```

#### Database Management
```bash
# Start test database (Docker)
just db-start

# Stop database (preserves data)
just db-stop

# Stop and remove database + volume
just db-stop --remove

# Setup/reset database
just db-setup
```

## Architecture Notes

### Database-Backed State
- All request states are persisted in PostgreSQL (see `migrations/` for schema)
- Uses `sqlx` for compile-time checked queries
- `.sqlx/` contains cached query metadata for offline builds

### Concurrency Control
- Per-model concurrency limits prevent overwhelming specific endpoints
- Configurable via `DaemonConfig`
- Example: limit GPT-4 to 5 concurrent requests while allowing 20 for GPT-3.5-turbo

### Request Lifecycle
1. Create file with request templates
2. Create batch from file (snapshots templates)
3. Daemon processes requests with retries and backoff
4. Query batch status and individual request results

## Testing

### Integration Tests
Located in `tests/integration.rs` (~136k lines). These tests:
- Require a running PostgreSQL database
- Test end-to-end workflows
- Use the test database configured in `.env`

### Running Tests
The tests expect `DATABASE_URL` environment variable to be set (done by `just db-setup`).

### Testing Standards

#### Required Test Coverage
- **Never add functionality without unit test coverage**
- Every new feature must include tests
- Bug fixes should include regression tests

#### Test Performance
- **Never use `sleep()` in tests**
- Always favor polling/waiting behavior for asynchronous operations
- Use proper synchronization primitives or polling loops with timeouts
- Keep tests as fast as possible

Example of good async testing:
```rust
// Good - polling with timeout
let start = Instant::now();
loop {
    if condition_met().await {
        break;
    }
    if start.elapsed() > Duration::from_secs(5) {
        panic!("Timeout waiting for condition");
    }
    tokio::time::sleep(Duration::from_millis(10)).await;
}

// Bad - fixed sleep
tokio::time::sleep(Duration::from_secs(2)).await; // Don't do this!
```

## Important Behaviors

### Git Workflow

#### Branch Naming
- Always work on sensibly named branches (e.g., `feat/add-retry-logic`, `fix/database-connection`)
- Never commit directly to `main`

#### Commit Conventions
Use Conventional Commits format:

- `feat:` - New feature
- `fix:` - Bug fix
- `feat!:` - Breaking change (new feature)
- `fix!:` - Breaking change (bug fix)

**Important:** The `!` denotes breaking changes. Evaluate breaking changes relative to `main`, not between commits on your branch, because commits are squashed when merged.

Examples:
```bash
git commit -m "feat: add per-model rate limiting"
git commit -m "fix: prevent duplicate batch creation"
git commit -m "feat!: change RequestTemplate API to use builder pattern"
```

#### Merging
- Always wait for CI to pass before merging
- PRs should be reviewed and approved
- Commits are squashed on merge, so the PR title should follow conventional commit format

### Never Commit Without Permission
- Always wait for explicit user approval before committing changes
- Use `git add` to stage changes, but don't run `git commit` without permission

### Code Style
- Follow Rust 2024 edition conventions
- Run `just fmt` before committing
- Ensure `just lint` passes (no warnings allowed)

### Database Migrations
- All schema changes go in `migrations/`
- Use `sqlx migrate add <name>` to create new migrations
- Run `cargo sqlx prepare` after migration changes to update `.sqlx/` cache

### Dependencies
- Uses `sqlx` with compile-time query checking
- Tokio async runtime (full features)
- Optional postgres feature (enabled by default)

## Development Tips

### Working with SQLx
- Queries are checked at compile time against the database schema
- Keep `.sqlx/` directory committed for offline builds
- Run `cargo sqlx prepare` to regenerate query metadata after schema changes

### Feature Flags
```toml
default = ["postgres"]
postgres = ["sqlx"]
```

The `postgres` feature can be disabled for minimal builds without database support.

### Environment Variables
The `.env` file (gitignored) contains:
```
DATABASE_URL=postgres://postgres:password@localhost:5432/fusillade
```

### Version Management
- Current version: 2.0.1 (see `Cargo.toml`)
- Uses release-please for automated releases
- Changelog maintained in `CHANGELOG.md`
