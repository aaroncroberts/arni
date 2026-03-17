# Configuration Guide

Arni uses configuration files to manage database connections. You can use either YAML or TOML format.

## Configuration Files

Configuration files define one or more **profiles**, where each profile represents a database connection.

### YAML Format

```yaml
profiles:
  my_postgres:
    type: postgres
    host: localhost
    port: 5432
    database: mydb
    username: postgres
    password: secret

  my_mongo:
    type: mongodb
    uri: mongodb://localhost:27017
    database: mydb
```

### TOML Format

```toml
[profiles.my_postgres]
type = "postgres"
host = "localhost"
port = 5432
database = "mydb"
username = "postgres"
password = "secret"

[profiles.my_mongo]
type = "mongodb"
uri = "mongodb://localhost:27017"
database = "mydb"
```

## Supported Database Types

### PostgreSQL

```yaml
profiles:
  postgres_example:
    type: postgres
    host: localhost
    port: 5432
    database: mydb
    username: postgres
    password: secret
```

### MongoDB

```yaml
profiles:
  mongodb_example:
    type: mongodb
    uri: mongodb://localhost:27017
    database: mydb
```

### Oracle

```yaml
profiles:
  oracle_example:
    type: oracle
    connection_string: "user/pass@//host:1521/service"
```

### SQL Server

```yaml
profiles:
  mssql_example:
    type: mssql
    host: localhost
    port: 1433
    database: mydb
    username: sa
    password: YourStrong@Passw0rd
```

### DuckDB

```yaml
profiles:
  # File-based
  duckdb_file:
    type: duckdb
    path: ./data/analytics.duckdb
  
  # In-memory
  duckdb_memory:
    type: duckdb
    path: ":memory:"
```

## Loading Configuration

### From File (Auto-detect Format)

```rust
use arni::config::Config;

// Auto-detects format from extension (.yaml, .yml, or .toml)
let config = Config::from_file("config.yaml")?;
```

### From Specific Format

```rust
// Load YAML explicitly
let config = Config::from_yaml_file("config.yaml")?;

// Load TOML explicitly
let config = Config::from_toml_file("config.toml")?;
```

## Using Profiles

```rust
use arni::config::Config;

let config = Config::from_file("config.yaml")?;

// Get a specific profile
if let Some(profile) = config.get_profile("my_postgres") {
    println!("Database type: {}", profile.db_type());
}

// Access profile fields
match profile {
    Profile::Postgres { host, port, database, .. } => {
        println!("Connecting to {}:{}/{}", host, port, database);
    }
    _ => {}
}
```

## Validation

Configuration files are automatically validated when loaded:

- All required fields must be present
- Host and database names cannot be empty
- Port numbers must be non-zero
- Connection strings/URIs cannot be empty
- At least one profile must be defined

Invalid configurations will return an error with a descriptive message.

## Environment Variables

You can reference environment variables in password fields:

```yaml
profiles:
  prod_db:
    type: postgres
    host: prod.example.com
    port: 5432
    database: production
    username: app_user
    password: ${DB_PASSWORD}
```

**Note:** Environment variable substitution is not yet implemented but is planned for a future release.

## Examples

See the `examples/` directory for complete configuration file examples:
- `examples/config.yaml` - YAML format with all database types
- `examples/config.toml` - TOML format with all database types
