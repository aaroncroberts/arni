# Local Database Development Guide

This guide explains how to set up and use local database containers for Arni development and integration testing.

## Overview

Arni provides a Docker Compose configuration that spins up **5 database systems** with pre-populated test data:

- **PostgreSQL 16** (alpine)
- **MySQL 8.0**
- **Azure SQL Edge** (SQL Server)
- **Oracle 23ai Free**
- **MongoDB 7**

Each container:
- Runs with health checks
- Initializes with a test schema and sample data
- Stores data persistently in `~/.arni/data/{database}/`
- Exposes standard ports on localhost

## Prerequisites

### Required Software

**Option 1: Podman (Recommended for macOS/Linux)**
```bash
# macOS (with Homebrew)
brew install podman podman-compose

# Initialize podman machine (macOS only)
podman machine init
podman machine start
```

**Option 2: Docker**
```bash
# macOS (with Homebrew)
brew install docker docker-compose

# Or download Docker Desktop from https://www.docker.com/products/docker-desktop
```

### System Resources

- **Disk Space**: ~10GB for all container images
- **Memory**: 8GB RAM recommended (Oracle requires 2GB shared memory)
- **Available Ports**: 5432, 3306, 1433, 1521, 5500, 27017

## Quick Start

### Start All Databases

```bash
# From project root
podman-compose up -d

# Or with Docker
docker-compose up -d
```

This will:
1. Download container images (first run only, ~5-10 minutes)
2. Create data directories in `~/.arni/data/`
3. Start all 5 database containers
4. Run initialization scripts
5. Wait for health checks to pass

### Verify Containers Are Running

```bash
# Check container status
podman ps

# Expected output: 5 containers with "healthy" status
# arni-dev-postgres
# arni-dev-mysql
# arni-dev-mssql
# arni-dev-oracle
# arni-dev-mongodb
```

### Stop Databases

```bash
# Stop all containers (data persists)
podman-compose down

# Stop and remove volumes (DESTROYS DATA)
podman-compose down -v
```

## Connection Details

All databases are pre-configured with test credentials and sample data.

### PostgreSQL

| Property      | Value                  |
|---------------|------------------------|
| **Host**      | `localhost`            |
| **Port**      | `5432`                 |
| **Database**  | `test_db`              |
| **Username**  | `test_user`            |
| **Password**  | `test_password`        |
| **Schema**    | `public`               |

**Connection String:**
```
postgresql://test_user:test_password@localhost:5432/test_db
```

**Test Connection:**
```bash
psql -h localhost -U test_user -d test_db -c "SELECT COUNT(*) FROM users;"
# Expected output: 5 rows
```

### MySQL

| Property      | Value                  |
|---------------|------------------------|
| **Host**      | `localhost`            |
| **Port**      | `3306`                 |
| **Database**  | `test_db`              |
| **Username**  | `test_user`            |
| **Password**  | `test_password`        |

**Connection String:**
```
mysql://test_user:test_password@localhost:3306/test_db
```

**Test Connection:**
```bash
mysql -h localhost -u test_user -ptest_password test_db -e "SELECT COUNT(*) FROM users;"
# Expected output: 5 rows
```

### SQL Server (Azure SQL Edge)

| Property      | Value                  |
|---------------|------------------------|
| **Host**      | `localhost`            |
| **Port**      | `1433`                 |
| **Database**  | `test_db`              |
| **Username**  | `sa`                   |
| **Password**  | `Test_Password123!`    |

**Connection String:**
```
sqlserver://sa:Test_Password123!@localhost:1433/test_db
```

**Test Connection:**
```bash
# Using sqlcmd (install separately)
sqlcmd -S localhost -U sa -P 'Test_Password123!' -Q "SELECT COUNT(*) FROM test_db.dbo.users;"
# Expected output: 5 rows
```

**Note:** SQL Server uses strong password requirements.

### Oracle 23ai Free

| Property      | Value                  |
|---------------|------------------------|
| **Host**      | `localhost`            |
| **Port**      | `1521` (TNS), `5500` (Enterprise Manager) |
| **SID**       | `FREE`                 |
| **Username**  | `system`               |
| **Password**  | `test_password`        |

**Connection String:**
```
oracle://system:test_password@localhost:1521/FREE
```

**Test Connection:**
```bash
# Using sqlplus (included in Oracle container)
sqlplus system/test_password@localhost:1521/FREE <<EOF
SELECT COUNT(*) FROM users;
EXIT;
EOF
# Expected output: 5 rows
```

**Note:** Oracle container takes 2-3 minutes to fully initialize on first start.

### MongoDB

| Property      | Value                  |
|---------------|------------------------|
| **Host**      | `localhost`            |
| **Port**      | `27017`                |
| **Database**  | `test_db`              |
| **Username**  | `test_user`            |
| **Password**  | `test_password`        |

**Connection String:**
```
mongodb://test_user:test_password@localhost:27017/test_db?authSource=admin
```

**Test Connection:**
```bash
mongosh -u test_user -p test_password --authenticationDatabase admin --eval 'db.getSiblingDB("test_db").users.countDocuments()'
# Expected output: 5
```

## Test Data Schema

All databases are initialized with identical test data:

### Users Table/Collection

| Column      | Type                | Description                  |
|-------------|---------------------|------------------------------|
| `id`        | Integer (PK)        | Auto-incrementing primary key|
| `name`      | String (100 chars)  | User full name               |
| `email`     | String (255 chars)  | Email address (unique)       |
| `active`    | Boolean             | Account active status        |
| `created_at`| Timestamp           | Account creation time        |

### Sample Data (5 Users)

| id | name          | email                    | active | created_at   |
|----|---------------|--------------------------|--------|--------------|
| 1  | Alice Johnson | alice.johnson@example.com| true   | timestamp    |
| 2  | Bob Smith     | bob.smith@example.com    | true   | timestamp    |
| 3  | Charlie Brown | charlie.brown@example.com| false  | timestamp    |
| 4  | Diana Prince  | diana.prince@example.com | true   | timestamp    |
| 5  | Eve Adams     | eve.adams@example.com    | true   | timestamp    |

### Indexes

- **Unique Index**: `email` column
- **Non-Unique Index**: `active` column

## Running Integration Tests

### Prerequisites

Set environment variables to enable integration tests:

```bash
# Enable all database integration tests
export TEST_POSTGRES_AVAILABLE=true
export TEST_MYSQL_AVAILABLE=true
export TEST_MSSQL_AVAILABLE=true
export TEST_ORACLE_AVAILABLE=true
export TEST_MONGODB_AVAILABLE=true

# Connection details (defaults to localhost)
export TEST_POSTGRES_HOST=localhost
export TEST_POSTGRES_PORT=5432
export TEST_MYSQL_HOST=localhost
export TEST_MYSQL_PORT=3306
# ... (add others as needed)
```

**Tip:** Create a `.env.test` file in the project root:
```bash
# .env.test
TEST_POSTGRES_AVAILABLE=true
TEST_MYSQL_AVAILABLE=true
TEST_MSSQL_AVAILABLE=true
TEST_ORACLE_AVAILABLE=true
TEST_MONGODB_AVAILABLE=true
```

Then source it before running tests:
```bash
source .env.test
cargo test --test '*' -- --ignored
```

### Run Integration Tests

```bash
# Start databases first
podman-compose up -d

# Wait for health checks (2-3 minutes for Oracle)
podman ps  # Verify all show "healthy" status

# Run all integration tests (currently ignored by default)
cargo test --test '*' -- --ignored

# Run specific database integration tests
cargo test --test postgres -- --ignored
cargo test --test mysql -- --ignored
cargo test --test mssql -- --ignored
cargo test --test oracle -- --ignored
cargo test --test mongodb -- --ignored
```

### Clean Test Data

If tests pollute the database:

```bash
# Stop and remove containers + volumes
podman-compose down -v

# Restart fresh
podman-compose up -d
```

## Troubleshooting

### Port Already in Use

**Symptom:**
```
Error: bind: address already in use
```

**Solution:**
```bash
# Find process using the port (example: 5432)
lsof -i :5432

# Kill the conflicting process
kill -9 <PID>

# Or change ports in compose.yml
ports:
  - "15432:5432"  # Use port 15432 on host
```

### Container Unhealthy

**Symptom:**
```bash
podman ps
# Shows "unhealthy" or "starting" status for >5 minutes
```

**Solution:**
```bash
# Check container logs
podman logs arni-dev-postgres
podman logs arni-dev-oracle

# Common issues:
# - PostgreSQL: Check password in compose.yml
# - MySQL: Verify init script syntax
# - SQL Server: Ensure password meets complexity requirements
# - Oracle: May need 2-3 minutes on first start
# - MongoDB: Check authentication database

# Restart specific container
podman restart arni-dev-postgres
```

### Permission Denied on Data Directories

**Symptom:**
```
Error: mkdir ~/.arni/data: permission denied
```

**Solution:**
```bash
# Create directories manually with correct permissions
mkdir -p ~/.arni/data/{postgres,mysql,mssql,oracle,mongodb}
chmod 755 ~/.arni/data/*

# For SELinux systems (Fedora/RHEL)
chcon -Rt svirt_sandbox_file_t ~/.arni/data/
```

### Oracle Container Slow to Start

**Symptom:**
Oracle container stays in "starting" status for 5+ minutes.

**Solution:**
This is **normal** for Oracle 23ai Free on first run:
- First start: 2-3 minutes (database initialization)
- Subsequent starts: 30-60 seconds

```bash
# Monitor Oracle logs
podman logs -f arni-dev-oracle

# Look for: "DATABASE IS READY TO USE!"
```

### SQL Server Connection Refused

**Symptom:**
```
sqlcmd: Sqlcmd: Error: Microsoft ODBC Driver: TCP Provider: Error code 0x2749
```

**Solution:**
SQL Server takes ~30 seconds to initialize:
```bash
# Wait for health check
podman ps  # Wait for "healthy" status

# Retry connection after 30 seconds
sleep 30
sqlcmd -S localhost -U sa -P 'Test_Password123!'
```

### MongoDB Authentication Failed

**Symptom:**
```
MongoServerError: Authentication failed
```

**Solution:**
Ensure using correct connection string:
```bash
# Correct: Specify authSource=admin
mongosh -u test_user -p test_password --authenticationDatabase admin

# Connection string format:
mongodb://test_user:test_password@localhost:27017/test_db?authSource=admin
```

### Podman Machine Not Running (macOS)

**Symptom:**
```
Error: unable to connect to Podman socket
```

**Solution:**
```bash
# Check podman machine status
podman machine list

# Start the machine
podman machine start

# Set socket environment variable (if needed)
export DOCKER_HOST="unix://$HOME/.local/share/containers/podman/machine/podman-machine-default/podman.sock"
```

## Advanced Usage

### Running Subset of Databases

Start only specific databases:

```bash
# PostgreSQL and MySQL only
podman-compose up -d postgres mysql

# All except Oracle (to save resources)
podman-compose up -d postgres mysql mssql mongodb
```

### Custom Initialization Scripts

Add additional setup to existing init scripts:

```bash
# Edit initialization script
vim scripts/init-postgres.sql

# Add custom tables, indexes, functions, etc.

# Recreate container to re-run init script
podman-compose down -v
podman-compose up -d postgres
```

### Accessing Container Shells

```bash
# PostgreSQL
podman exec -it arni-dev-postgres psql -U test_user test_db

# MySQL
podman exec -it arni-dev-mysql mysql -u test_user -ptest_password test_db

# SQL Server
podman exec -it arni-dev-mssql /opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P 'Test_Password123!'

# Oracle
podman exec -it arni-dev-oracle sqlplus system/test_password@FREE

# MongoDB
podman exec -it arni-dev-mongodb mongosh -u test_user -p test_password --authenticationDatabase admin
```

### Backup and Restore

**Backup:**
```bash
# Data is stored in ~/.arni/data/{database}/
# Simply copy these directories to back up

tar -czf arni-db-backup.tar.gz ~/.arni/data/
```

**Restore:**
```bash
# Extract backup
tar -xzf arni-db-backup.tar.gz -C ~/

# Restart containers
podman-compose down
podman-compose up -d
```

## CI/CD Integration

### GitHub Actions Example

```yaml
name: Integration Tests

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    services:
      postgres:
        image: postgres:16-alpine
        env:
          POSTGRES_USER: test_user
          POSTGRES_PASSWORD: test_password
          POSTGRES_DB: test_db
        ports:
          - 5432:5432
        options: >-
          --health-cmd pg_isready
          --health-interval 10s
          --health-timeout 5s
          --health-retries 5

    steps:
      - uses: actions/checkout@v3
      - name: Run integration tests
        env:
          TEST_POSTGRES_AVAILABLE: true
          TEST_POSTGRES_HOST: localhost
          TEST_POSTGRES_PORT: 5432
        run: cargo test --test postgres -- --ignored
```

## Resource Usage

Typical resource consumption with all containers running:

| Container | Memory | Disk  | CPU (idle) |
|-----------|--------|-------|------------|
| PostgreSQL| 50 MB  | 200 MB| <1%        |
| MySQL     | 350 MB | 400 MB| <1%        |
| SQL Server| 600 MB | 1 GB  | 1-2%       |
| Oracle    | 1.5 GB | 4 GB  | 2-3%       |
| MongoDB   | 150 MB | 300 MB| <1%        |
| **Total** | ~2.6GB | ~6 GB | ~5%        |

**Optimization Tips:**
- Run only databases you need for current work
- Stop Oracle when not actively testing (largest resource user)
- Use `podman-compose down` when not developing to free resources

## Next Steps

- **Run Integration Tests**: See [tests/README.md](../tests/README.md)
- **Develop Adapters**: See [.claude/CLAUDE.md](../.claude/CLAUDE.md)
- **Contribute**: See [CONTRIBUTING.md](../CONTRIBUTING.md) (coming soon)

---

**Questions or Issues?**
- Check [GitHub Issues](https://github.com/yourusername/arni/issues)
- See [docs/](../docs/) for more guides
