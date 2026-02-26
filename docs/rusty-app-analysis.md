# Rusty-App Architecture Analysis for Arni Adaptation

**Analysis Date**: February 26, 2026  
**Source Project**: `/Users/aaron/Repos/github/rusty-app/`  
**Target Project**: Arni - Unified Database Access with Polars DataFrames

## Executive Summary

This document provides a comprehensive analysis of the `rusty-data` and `rusty-logging` crates from the rusty-app project, with recommendations for adapting them to the arni project. The key difference is that arni uses **Polars DataFrames** as the data interchange format instead of custom `QueryResult` structures.

---

## Part 1: rusty-data Architecture

### Overview

`rusty-data` is a production-grade database abstraction library providing unified access to 6 database systems through a consistent adapter pattern. It handles configuration, credential encryption, connection pooling, and comprehensive schema introspection.

### Core Architecture

#### 1. DatabaseAdapter Trait

**Location**: `src/adapter.rs`

The `DatabaseAdapter` trait defines the contract all database adapters must implement:

```rust
#[async_trait]
pub trait DatabaseAdapter: Send + Sync {
    // Core Connection Methods
    async fn connect(&mut self, config: &ConnectionConfig, password: Option<&str>) -> Result<()>;
    async fn disconnect(&mut self) -> Result<()>;
    fn is_connected(&self) -> bool;
    
    // Query Execution
    async fn execute_query(&self, query: &str) -> Result<QueryResult>;
    
    // Schema Discovery
    async fn list_databases(&self) -> Result<Vec<String>>;
    async fn list_tables(&self, schema: Option<&str>) -> Result<Vec<String>>;
    async fn describe_table(&self, table_name: &str, schema: Option<&str>) -> Result<TableInfo>;
    async fn test_connection(&self, config: &ConnectionConfig, password: Option<&str>) -> Result<bool>;
    
    // Server & Database Introspection (with default implementations)
    async fn get_server_info(&self) -> Result<ServerInfo>;
    async fn get_database_metadata(&self, database_name: &str) -> Result<DatabaseMetadata>;
    async fn get_table_metadata(&self, table_name: &str, schema: Option<&str>) -> Result<TableMetadata>;
    async fn get_indexes(&self, table_name: &str, schema: Option<&str>) -> Result<Vec<IndexInfo>>;
    async fn get_foreign_keys(&self, table_name: &str, schema: Option<&str>) -> Result<Vec<ForeignKeyInfo>>;
    async fn get_views(&self, schema: Option<&str>) -> Result<Vec<ViewInfo>>;
    async fn get_view_definition(&self, view_name: &str, schema: Option<&str>) -> Result<Option<String>>;
    async fn list_stored_procedures(&self, schema: Option<&str>) -> Result<Vec<ProcedureInfo>>;
    
    fn database_type(&self) -> DatabaseType;
}
```

**Key Design Principles**:
- **Async-first**: All I/O operations are async using `tokio`
- **Default implementations**: Complex introspection methods have defaults (return empty/minimal data)
- **Type safety**: `DatabaseType` enum prevents runtime errors
- **Error context**: Custom error types with detailed categorization

#### 2. Data Types

**QueryResult Structure** (key difference from arni):
```rust
pub struct QueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<QueryValue>>,  // Row-major: Vec of rows, each row is Vec of values
    pub rows_affected: Option<u64>,
}

pub enum QueryValue {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Text(String),
    Bytes(Vec<u8>),
}
```

**Schema Types**:
```rust
pub struct TableInfo {
    pub name: String,
    pub schema: Option<String>,
    pub columns: Vec<ColumnInfo>,
}

pub struct ColumnInfo {
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
    pub default_value: Option<String>,
    pub is_primary_key: bool,
}

pub struct ConnectionConfig {
    pub id: String,
    pub name: String,
    pub db_type: DatabaseType,
    pub host: Option<String>,
    pub port: Option<u16>,
    pub database: String,
    pub username: Option<String>,
    pub use_ssl: bool,
    pub parameters: HashMap<String, String>,
}
```

#### 3. Implemented Adapters

All adapters follow the same pattern with database-specific implementations:

##### PostgreSQL Adapter (`src/adapters/postgres.rs`)
- **Driver**: `sqlx` with `postgres` feature
- **Pool**: `PgPool` with 5 max connections
- **Type Mapping**: Comprehensive support for PG types (BOOL, INT2/4/8, FLOAT4/8, TEXT, TIMESTAMP, BYTEA)
- **Features**:
  - Full schema introspection (tables, columns, indexes, FKs, views, procedures)
  - Server info extraction (version, settings)
  - Database metadata (size, owner, encoding)
  - Categorized error handling (syntax, permission, object_not_found, constraint)
- **Lines**: ~1816 (most comprehensive)

##### MySQL Adapter (`src/adapters/mysql.rs`)
- **Driver**: `sqlx` with `mysql` feature
- **Pool**: `MySqlPool` with 5 max connections
- **Type Mapping**: MySQL-specific types (TINYINT, MEDIUMINT, DECIMAL, DATETIME, BLOB variants)
- **Special Handling**: DDL statements (CREATE/DROP PROCEDURE) use simple execution
- **Lines**: ~1657

##### SQLite Adapter (`src/adapters/sqlite.rs`)
- **Driver**: `sqlx` with `sqlite` feature
- **Pool**: `SqlitePool` with 5 max connections
- **Type Mapping**: Simplified type system (NULL, INTEGER, REAL, TEXT, BLOB)
- **Features**:
  - File-based database (`:memory:` support)
  - Foreign keys enabled via PRAGMA
  - Relative and absolute path support
- **Lines**: ~1696

##### MongoDB Adapter (`src/adapters/mongodb.rs`)
- **Driver**: `mongodb` crate (official driver)
- **Connection**: Direct client (no pooling needed)
- **Query Format**: JSON commands (custom DSL)
  ```json
  {
    "collection": "users",
    "filter": {"age": {"$gt": 18}},
    "limit": 10
  }
  ```
- **Type Mapping**: BSON to QueryValue conversion
- **Special Operations**: createView, insertMany, etc.
- **Lines**: ~1807

##### SQL Server Adapter (`src/adapters/mssql.rs`)
- **Driver**: `tiberius` with `tokio-util` compat layer
- **Connection**: `TcpStream` + `Client<Compat<TcpStream>>`
- **Pooling**: Custom `Pool<T>` wrapper (see pool.rs)
- **Authentication**: SQL Server authentication (username/password)
- **Special Handling**: DDL statements use `simple_query`
- **Lines**: ~1711

##### Oracle Adapter (`src/adapters/oracle.rs`)
- **Driver**: `oracle` crate (synchronous)
- **Async Bridge**: `tokio::task::spawn_blocking` for blocking operations
- **Pooling**: Custom `Pool<Connection>`
- **Connection String**: `host:port/service_name` format
- **Type Mapping**: Limited (primarily string with fallback to i64/f64)
- **Error Handling**: ORA-XXXXX error code categorization
- **Lines**: ~1542

#### 4. Connection Pooling

**Pool Module** (`src/pool.rs`):
```rust
pub struct Pool<T> {
    inner: Arc<Mutex<T>>,
}

impl<T> Pool<T> {
    pub fn new(client: T) -> Self;
    pub async fn lock(&self) -> PoolGuard<'_, T>;
}
```

**Purpose**: Provides interior mutability for database clients requiring mutable access while maintaining a `&self` API for the trait.

**Used By**: MSSQL (tiberius) and Oracle adapters

#### 5. Configuration Management

**ConfigManager** (`src/config.rs`):
- **Directory**: Configurable config directory (default: `~/.config/rusty-app`)
- **File Format**: TOML (`connections.toml`)
- **Encryption**:
  - Algorithm: AES-256-GCM
  - Key Derivation: Argon2 with salt
  - Credential Storage: Separate encrypted files per connection
- **Validation**: Comprehensive config validation (host, port, database name length limits)

**Key Methods**:
```rust
impl ConfigManager {
    pub fn new<P: AsRef<Path>>(config_dir: P) -> Result<Self>;
    pub fn load_connections(&self) -> Result<Vec<ConnectionConfig>>;
    pub fn save_connections(&self, connections: &[ConnectionConfig]) -> Result<()>;
    pub fn encrypt_password(password: &str, master_password: &str) -> Result<EncryptedCredentials>;
    pub fn decrypt_password(encrypted: &EncryptedCredentials, master_password: &str) -> Result<String>;
}
```

#### 6. Error Handling

**DataError** (`src/error.rs`):
```rust
pub enum DataError {
    Config(String),           // Configuration errors
    Connection(String),       // Connection failures
    Query(String),            // Query execution errors
    Encryption(String),       // Encryption/decryption errors
    Io(#[from] std::io::Error),
    Serialization(String),    // TOML/JSON errors
    AdapterNotFound(String),
    Authentication(String),
    NotSupported(String),
    Other(#[from] anyhow::Error),
}
```

**Error Categorization**: Each adapter categorizes errors for better debugging:
- PostgreSQL: `authentication`, `network`, `database_not_found`, `syntax_error`, `permission_denied`
- MongoDB: `authentication`, `network`, `replica_set`, `unauthorized`
- Oracle: `authentication`, `tns_resolution`, `timeout`, `no_listener`, `instance_unavailable`

#### 7. Module Structure

```
rusty-data/
├── src/
│   ├── lib.rs              # Re-exports, module declarations
│   ├── adapter.rs          # DatabaseAdapter trait, types (630 lines)
│   ├── config.rs           # ConfigManager, encryption (598 lines)
│   ├── error.rs            # DataError enum (193 lines)
│   ├── pool.rs             # Pool<T> for interior mutability (117 lines)
│   └── adapters/
│       ├── mod.rs          # Feature-gated module exports
│       ├── postgres.rs     # PostgreSQL adapter (1816 lines)
│       ├── mysql.rs        # MySQL adapter (1657 lines)
│       ├── sqlite.rs       # SQLite adapter (1696 lines)
│       ├── mongodb.rs      # MongoDB adapter (1807 lines)
│       ├── mssql.rs        # SQL Server adapter (1711 lines)
│       └── oracle.rs       # Oracle adapter (1542 lines)
├── tests/
│   ├── postgres_integration_tests.rs     # 1178 lines
│   ├── mysql_integration_tests.rs
│   ├── sqlite_integration_tests.rs
│   ├── mongodb_integration_tests.rs      # 850 lines
│   ├── mssql_integration_tests.rs
│   ├── oracle_integration_tests.rs
│   └── README.md           # Test database setup instructions
└── docs/
    └── (empty - documentation is in main repo)
```

### Dependencies

**Core Dependencies**:
```toml
# Async runtime
tokio = { workspace = true }
async-trait = { workspace = true }

# Serialization
serde = { workspace = true }
serde_json = { workspace = true }
toml = { workspace = true }

# Error handling
thiserror = { workspace = true }
anyhow = { workspace = true }

# Logging
tracing = "0.1"

# Encryption
aes-gcm = { workspace = true }
argon2 = { workspace = true }
rand = { workspace = true }

# Utilities
dirs = "5.0"  # Home directory expansion
```

**Database Drivers** (all optional via features):
```toml
sqlx = { version = "0.7", optional = true, features = ["runtime-tokio", "tls-rustls", "chrono"] }
mongodb = { version = "2.8", optional = true }
oracle = { version = "0.5", optional = true }
tiberius = { version = "0.12", optional = true, features = ["tds73", "chrono"] }
tokio-util = { version = "0.7", optional = true, features = ["compat"] }
futures-util = { version = "0.3", optional = true }
futures = { version = "0.3", optional = true }
```

**Feature Flags**:
```toml
[features]
default = []
postgres = ["sqlx", "sqlx/postgres"]
mysql = ["sqlx", "sqlx/mysql"]
sqlite = ["sqlx", "sqlx/sqlite"]
mssql = ["tiberius", "tokio-util", "futures-util"]
oracle = ["dep:oracle", "futures"]
mongodb = ["mongodb"]
all-databases = ["postgres", "mysql", "sqlite", "mssql", "mongodb", "oracle"]
```

### Testing Strategy

#### Integration Test Pattern

**Setup Pattern** (from `postgres_integration_tests.rs`):
```rust
static INIT: AtomicBool = AtomicBool::new(false);

async fn ensure_test_database() -> Result<()> {
    // One-time database creation
    let mut adapter = PostgresAdapter::new();
    adapter.connect(&postgres_config, Some(TEST_PASSWORD)).await?;
    let _ = adapter.execute_query(&format!("CREATE DATABASE {}", TEST_DB_NAME)).await;
    adapter.disconnect().await?;
    Ok(())
}

async fn setup() -> Result<PostgresAdapter> {
    if !INIT.load(Ordering::Relaxed) {
        ensure_test_database().await?;
        INIT.store(true, Ordering::Relaxed);
    }
    let mut adapter = PostgresAdapter::new();
    adapter.connect(&config, Some(TEST_PASSWORD)).await?;
    Ok(adapter)
}
```

**Test Pattern**:
```rust
#[tokio::test]
#[ignore]  // Don't run by default (requires database)
async fn test_postgres_execute_query() -> Result<()> {
    info!("Starting test: test_postgres_execute_query");
    
    let mut adapter = setup().await?;
    
    // 1. Create test data
    adapter.execute_query("CREATE TABLE test_users (...)").await?;
    adapter.execute_query("INSERT INTO test_users ...").await?;
    
    // 2. Test operation
    let result = adapter.execute_query("SELECT * FROM test_users").await?;
    assert_eq!(result.rows.len(), 3);
    
    // 3. Cleanup
    adapter.execute_query("DROP TABLE test_users").await?;
    adapter.disconnect().await?;
    
    info!("Test completed: test_postgres_execute_query");
    Ok(())
}
```

**Key Testing Features**:
- `#[ignore]` attribute: Tests require running databases
- Atomic initialization: Database created once per test suite
- Self-contained: Each test creates and cleans up its own tables
- Comprehensive logging: `tracing::info!` and `tracing::debug!` throughout
- Error propagation: Uses `Result<()>` return type

#### Test Database Setup

Location: `tests/README.md` and `compose.yml`

**Docker/Podman Compose**:
- PostgreSQL: port 5432
- MySQL: port 3306
- SQL Server: port 1433
- Oracle XE: port 1521
- MongoDB: port 27017
- SQLite: file-based (no container)

**Test Credentials**:
- PostgreSQL: `test_user` / `test_password`
- MySQL: `test_user` / `test_password`
- SQL Server: `sa` / `Test_Password123!`
- Oracle: `system` / `Test_Password123!`
- MongoDB: `test_user` / `test_password`

---

## Part 2: rusty-logging Architecture

### Overview

`rusty-logging` is a lightweight, production-ready logging infrastructure built on the `tracing` ecosystem. It provides structured logging with flexible configuration for console and file outputs.

### Core Architecture

#### 1. Configuration System

**LoggingConfig** (`src/config.rs`):
```rust
pub struct LoggingConfig {
    // Global filter (can be overridden per-output)
    filter: String,
    
    // Console configuration
    console_filter: Option<String>,
    console_format: ConsoleFormat,
    console_writer: ConsoleWriter,
    console_enabled: bool,
    
    // File configuration
    file_filter: Option<String>,
    file_enabled: bool,
    file_format: FileFormat,
    file_directory: PathBuf,
    file_prefix: String,
    rotation_policy: RotationPolicy,
}
```

**Configuration Options**:
```rust
pub enum ConsoleFormat {
    Pretty,   // Colorized, full context (development)
    Compact,  // Minimal output (production)
}

pub enum ConsoleWriter {
    Stdout,
    Stderr,  // Default
}

pub enum FileFormat {
    Text,  // Human-readable .log files
    Json,  // JSON Lines .jsonl for log aggregation
}

pub enum RotationPolicy {
    Daily,     // Rotate at midnight
    Hourly,    // Rotate every hour
    Minutely,  // Rotate every minute (testing)
    Never,     // Single file, append forever
}
```

#### 2. Builder API

**LoggingConfigBuilder** (`src/config.rs`):
```rust
impl LoggingConfig {
    pub fn builder() -> LoggingConfigBuilder;
}

impl LoggingConfigBuilder {
    // Enable console output
    pub fn with_console_pretty(self) -> Self;
    pub fn with_console_compact(self) -> Self;
    pub fn with_console_stdout(self) -> Self;
    pub fn with_console_stderr(self) -> Self;
    pub fn with_console_filter(self, filter: impl Into<String>) -> Self;
    
    // Enable file output
    pub fn with_file_text(self) -> Self;
    pub fn with_file_json(self) -> Self;
    pub fn with_file_directory(self, dir: impl Into<PathBuf>) -> Self;
    pub fn with_file_prefix(self, prefix: impl Into<String>) -> Self;
    pub fn with_rotation_policy(self, policy: RotationPolicy) -> Self;
    pub fn with_file_filter(self, filter: impl Into<String>) -> Self;
    
    // Global filter
    pub fn with_filter(self, filter: impl Into<String>) -> Self;
    
    pub fn build(self) -> Result<LoggingConfig>;
}
```

#### 3. Initialization

**Simple Initialization** (`src/lib.rs`):
```rust
// Default: pretty console, INFO level
pub fn init_default();

// With custom filter
pub fn init_default_with_filter(filter: &str) -> Result<()>;

// With full configuration
pub fn init(config: LoggingConfig) -> Result<()>;
```

#### 4. Features

**Dual Output Support**:
- Console and file can be enabled simultaneously
- Independent log level filters (e.g., INFO to console, DEBUG to file)
- Different formats per output (e.g., pretty console + JSON file)

**Environment Variable Support**:
- Honors `RUST_LOG` environment variable
- Overrides config if set
- Supports complex filters: `rusty_data=debug,rusty_app=info`

**Structured Logging**:
```rust
// Key-value fields
tracing::info!(user_id = 123, action = "login", "User logged in");

// Spans for context
let span = tracing::info_span!("database_query", table = "users");
let _guard = span.enter();
tracing::info!("Executing query");
```

**File Rotation**:
- Uses `tracing_appender::rolling::RollingFileAppender`
- Timestamped file names
- Automatic cleanup of old files (based on policy)

#### 5. Module Structure

```
rusty-logging/
├── src/
│   ├── lib.rs      # Public API, convenience functions (180 lines)
│   ├── config.rs   # LoggingConfig, builder (998 lines)
│   └── error.rs    # LoggingError enum (90 lines)
├── examples/
│   └── (none currently)
└── README.md       # Usage examples (272 lines)
```

### Dependencies

```toml
[dependencies]
tracing = "0.1"
tracing-subscriber = { version = "0.3", features = ["env-filter", "fmt", "json", "ansi"] }
tracing-appender = "0.2"

[dev-dependencies]
tokio = { workspace = true, features = ["test-util"] }
```

### Usage Examples

**1. Simple Console Logging**:
```rust
rusty_logging::init_default();
tracing::info!("App started");
```

**2. Compact Console for Production**:
```rust
LoggingConfig::builder()
    .with_console_compact()
    .with_filter("info")
    .build()?
    .apply()?;
```

**3. File Logging with Daily Rotation**:
```rust
LoggingConfig::builder()
    .with_file_text()
    .with_file_directory("./logs")
    .with_file_prefix("myapp")
    .with_rotation_policy(RotationPolicy::Daily)
    .build()?
    .apply()?;
```

**4. Dual Output (Console + JSON File)**:
```rust
LoggingConfig::builder()
    .with_console_compact()
    .with_console_filter("info")
    .with_file_json()
    .with_file_directory("./logs")
    .with_file_filter("debug")  // More verbose in files
    .build()?
    .apply()?;
```

**5. Structured Logging**:
```rust
tracing::info!(
    database = "postgres",
    query_time_ms = 42,
    rows = 100,
    "Query completed"
);
// JSON output: {"timestamp":"...","level":"INFO","fields":{"database":"postgres","query_time_ms":42,"rows":100},"message":"Query completed"}
```

### Error Handling

**LoggingError** (`src/error.rs`):
```rust
pub enum LoggingError {
    ConfigError(String),    // Invalid configuration
    InitError(String),      // Failed to initialize subscriber
    IoError(std::io::Error), // File I/O errors
    FilterError(String),    // Invalid log level filter
}
```

---

## Part 3: Key Differences for Arni Adaptation

### 1. Data Interchange Format

**rusty-data** uses custom `QueryResult`:
```rust
pub struct QueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<QueryValue>>,  // Row-major format
    pub rows_affected: Option<u64>,
}
```

**arni** should use Polars DataFrames:
```rust
pub struct QueryResult {
    pub dataframe: DataFrame,        // Polars DataFrame
    pub rows_affected: Option<u64>,
}
```

**Advantages of Polars**:
- Native columnar storage (better performance)
- Built-in type inference
- Rich data manipulation API
- Arrow interop for other tools
- Statistical functions
- Memory-efficient operations

**Challenges**:
- Type conversion from database types to Polars types
- Error handling during DataFrame construction
- NULL handling (Polars uses Option mechanism)

### 2. Type Mapping

**rusty-data's QueryValue → Polars Series mapping**:

| QueryValue | Polars Type | Notes |
|------------|-------------|-------|
| `Null` | `Option<T>` | Handled via nullable series |
| `Bool(bool)` | `DataType::Boolean` | Direct mapping |
| `Int(i64)` | `DataType::Int64` | May need downcast if smaller |
| `Float(f64)` | `DataType::Float64` | Direct mapping |
| `Text(String)` | `DataType::Utf8` | Direct mapping |
| `Bytes(Vec<u8>)` | `DataType::Binary` | Direct mapping |

**Additional Polars Types to Support**:
- `Date` and `Datetime` (from database timestamps)
- `UInt8/16/32/64` (for unsigned integers)
- `Categorical` (for enum-like columns)
- `List` (for array types in PostgreSQL)

### 3. Adapter Trait Changes

**Current DatabaseAdapter** returns `QueryResult`:
```rust
async fn execute_query(&self, query: &str) -> Result<QueryResult>;
```

**Proposed for Arni**:
```rust
async fn execute_query(&self, query: &str) -> Result<DataFrame>;

// Or with metadata:
async fn execute_query(&self, query: &str) -> Result<QueryResult> {
    // where QueryResult contains DataFrame + rows_affected
}
```

**Additional Methods for Arni**:
```rust
// Export DataFrame to database table
async fn export_dataframe(
    &self,
    df: &DataFrame,
    table_name: &str,
    if_exists: IfExistsStrategy,
) -> Result<u64>;

pub enum IfExistsStrategy {
    Fail,       // Error if table exists
    Replace,    // DROP and CREATE
    Append,     // INSERT INTO existing table
}

// Read table directly to DataFrame
async fn read_table(&self, table_name: &str, schema: Option<&str>) -> Result<DataFrame>;

// Read with query (convenience)
async fn read_sql(&self, query: &str) -> Result<DataFrame>;
```

### 4. Configuration Simplification

**rusty-data** has extensive configuration with encryption. For arni:

**Simplified Config** (if appropriate):
- Remove encryption (or make optional)
- Focus on connection parameters
- Environment variable support
- Simpler serialization (no master password)

**OR Keep Security** (recommended for production use):
- Reuse ConfigManager pattern
- Add integration with system keychains (keyring crate)
- Support environment variables for CI/CD

### 5. Connection Pooling Strategy

**rusty-data** uses different pools per adapter:
- SQLx: Built-in pools
- MongoDB: Client handles pooling
- MSSQL/Oracle: Custom Pool<T>

**For arni**:
- Keep existing pattern (works well)
- Consider uniform pool configuration
- Add pool size tuning options

### 6. Error Handling Enhancement

**Add DataFrame-specific errors**:
```rust
pub enum DataError {
    // Existing errors...
    DataFrameConversion(String),  // Failed to convert to/from DataFrame
    SchemaIncompatible(String),   // DataFrame schema incompatible with table
    TypeInference(String),        // Failed to infer Polars types
}
```

### 7. Logging Integration

**rusty-logging** can be used as-is for arni:
- Add as workspace dependency
- Configure for development (pretty console) and production (compact + JSON file)
- Use structured logging for query performance tracking

**Recommended Logging**:
```rust
tracing::info!(
    adapter = "postgres",
    query_rows = df.height(),
    query_cols = df.width(),
    elapsed_ms = elapsed.as_millis(),
    "Query executed successfully"
);
```

---

## Part 4: Recommended Adaptation Strategy

### Phase 1: Core Infrastructure (Week 1-2)

#### 1.1 Setup Project Structure
- [ ] Create workspace with `arni` and `arni-cli` crates
- [ ] Copy `rusty-logging` as dependency (minimal changes needed)
- [ ] Setup feature flags for database drivers

#### 1.2 Adapt Error Types
- [ ] Copy `error.rs` from rusty-data
- [ ] Add DataFrame-specific error variants
- [ ] Add Polars error conversions

#### 1.3 Create Core Types
- [ ] Copy `ConnectionConfig` and `DatabaseType`
- [ ] Create new `QueryResult` with DataFrame field
- [ ] Define `IfExistsStrategy` enum
- [ ] Define `TableInfo`, `ColumnInfo`, etc. (keep same)

#### 1.4 Define Adapter Trait
- [ ] Copy `DatabaseAdapter` trait skeleton
- [ ] Change `execute_query` to return DataFrame
- [ ] Add `export_dataframe` method
- [ ] Add `read_table` convenience method
- [ ] Keep introspection methods as-is

### Phase 2: Implement First Adapter (Week 3-4)

#### 2.1 PostgreSQL Adapter (Recommended First)
Why PostgreSQL first:
- Most comprehensive rusty-data implementation
- Best type coverage
- Excellent sqlx support
- Rich feature set for testing

**Implementation Steps**:
1. Copy `adapters/postgres.rs` structure
2. Adapt `row_to_values` → `rows_to_dataframe`:
   ```rust
   fn rows_to_dataframe(rows: Vec<PgRow>) -> Result<DataFrame> {
       if rows.is_empty() {
           return Ok(DataFrame::default());
       }
       
       // Extract column names
       let columns: Vec<&str> = rows[0].columns()
           .iter()
           .map(|col| col.name())
           .collect();
       
       // Create series for each column
       let mut series_vec = Vec::new();
       for (i, col_name) in columns.iter().enumerate() {
           let series = column_to_series(&rows, i, col_name)?;
           series_vec.push(series);
       }
       
       DataFrame::new(series_vec)
           .map_err(|e| DataError::DataFrameConversion(e.to_string()))
   }
   
   fn column_to_series(rows: &[PgRow], col_idx: usize, name: &str) -> Result<Series> {
       let type_info = rows[0].columns()[col_idx].type_info();
       match type_info.name() {
           "INT4" => {
               let values: Vec<Option<i32>> = rows.iter()
                   .map(|row| row.try_get(col_idx).unwrap_or(None))
                   .collect();
               Ok(Series::new(name, values))
           }
           "TEXT" | "VARCHAR" => {
               let values: Vec<Option<String>> = rows.iter()
                   .map(|row| row.try_get(col_idx).unwrap_or(None))
                   .collect();
               Ok(Series::new(name, values))
           }
           // ... other types
           _ => Err(DataError::TypeInference(format!("Unsupported type: {}", type_info.name())))
       }
   }
   ```

3. Implement `export_dataframe`:
   ```rust
   async fn export_dataframe(
       &self,
       df: &DataFrame,
       table_name: &str,
       if_exists: IfExistsStrategy,
   ) -> Result<u64> {
       // 1. Check if table exists
       let tables = self.list_tables(None).await?;
       let exists = tables.contains(&table_name.to_string());
       
       // 2. Handle strategy
       match (exists, if_exists) {
           (true, IfExistsStrategy::Fail) => {
               return Err(DataError::Query(format!("Table {} already exists", table_name)));
           }
           (true, IfExistsStrategy::Replace) => {
               self.execute_query(&format!("DROP TABLE {}", table_name)).await?;
               // Continue to create
           }
           (true, IfExistsStrategy::Append) => {
               // Verify schema compatibility
               let table_info = self.describe_table(table_name, None).await?;
               verify_schema_compatible(df, &table_info)?;
               // Continue to insert
           }
           (false, _) => {
               // Create table
           }
       }
       
       // 3. Build CREATE TABLE statement (if needed)
       if !exists || if_exists == IfExistsStrategy::Replace {
           let create_sql = build_create_table_sql(df, table_name)?;
           self.execute_query(&create_sql).await?;
       }
       
       // 4. Build INSERT statement
       let insert_sql = build_insert_sql(df, table_name)?;
       let result = self.execute_query(&insert_sql).await?;
       
       Ok(result.rows_affected.unwrap_or(0))
   }
   ```

4. Write unit tests for type conversion
5. Write integration tests for export/import

#### 2.2 Testing Infrastructure
- [ ] Setup test database (podman-compose)
- [ ] Create test fixtures with sample DataFrames
- [ ] Test round-trip: DataFrame → DB → DataFrame
- [ ] Test type preservation
- [ ] Test NULL handling

### Phase 3: Expand Adapter Coverage (Week 5-8)

#### 3.1 Implement Remaining SQL Adapters
**Order of Implementation**:
1. **SQLite** (easiest, file-based, good for testing)
2. **MySQL** (similar to PostgreSQL, widely used)
3. **SQL Server** (enterprise use cases)
4. **Oracle** (enterprise, most complex)

**For Each Adapter**:
- Copy structure from rusty-data
- Adapt type conversion to Polars
- Test export/import thoroughly
- Document type mapping quirks

#### 3.2 MongoDB Adapter (Special Case)
**Challenges**:
- Document-based, not tabular
- Need schema inference from BSON
- Arrays and nested documents

**Approach**:
```rust
// Convert BSON documents to DataFrame
// Flatten nested structures or use JSON column type
async fn read_collection(&self, collection: &str, filter: Document) -> Result<DataFrame> {
    let docs = self.collection(collection).find(filter).await?;
    
    // Infer schema from first N documents
    let schema = infer_schema_from_docs(&docs)?;
    
    // Convert to DataFrame with flattened structure
    docs_to_dataframe(docs, schema)
}
```

### Phase 4: Configuration & CLI (Week 9-10)

#### 4.1 Configuration Management
**Decision**: Keep encryption or simplify?

**Option A - Keep Security** (Recommended):
- Copy ConfigManager from rusty-data
- Add keyring support for master password
- Support environment variables

**Option B - Simplify**:
- Plain text config (dev only)
- Environment variables
- Document security concerns

#### 4.2 CLI Development
- [ ] Connection management (add/list/remove/test)
- [ ] Interactive query execution
- [ ] DataFrame export to CSV/Parquet
- [ ] Results visualization (table formatter)

### Phase 5: Advanced Features (Week 11-12)

#### 5.1 Performance Optimization
- [ ] Batch inserts for large DataFrames
- [ ] Streaming for large result sets
- [ ] Connection pool tuning
- [ ] Query result caching

#### 5.2 Schema Introspection Enhancement
- [ ] Generate Polars schema from table metadata
- [ ] Automatic type inference
- [ ] Schema validation before export

#### 5.3 Additional Adapters (Optional)
- [ ] DuckDB (analytical queries)
- [ ] ClickHouse (time-series)
- [ ] BigQuery (cloud analytics)

---

## Part 5: Code Examples for Arni

### Example 1: Basic Usage

```rust
use arni::{PostgresAdapter, ConnectionConfig, DatabaseType, IfExistsStrategy};
use polars::prelude::*;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize logging
    arni_logging::init_default();
    
    // Create connection config
    let config = ConnectionConfig {
        id: "my-postgres".to_string(),
        name: "Production DB".to_string(),
        db_type: DatabaseType::Postgres,
        host: Some("localhost".to_string()),
        port: Some(5432),
        database: "mydb".to_string(),
        username: Some("user".to_string()),
        use_ssl: false,
        parameters: Default::default(),
    };
    
    // Connect
    let mut adapter = PostgresAdapter::new();
    adapter.connect(&config, Some("password")).await?;
    
    // Read data to DataFrame
    let df = adapter.read_table("users", None).await?;
    println!("Loaded {} rows", df.height());
    
    // Process with Polars
    let filtered = df.lazy()
        .filter(col("age").gt(18))
        .select(&[col("id"), col("name"), col("email")])
        .collect()?;
    
    // Export to another table
    adapter.export_dataframe(&filtered, "adult_users", IfExistsStrategy::Replace).await?;
    
    adapter.disconnect().await?;
    Ok(())
}
```

### Example 2: Type Conversion Helper

```rust
use polars::prelude::*;
use sqlx::postgres::PgRow;
use sqlx::{Column, Row, TypeInfo};

pub fn postgres_rows_to_dataframe(rows: Vec<PgRow>) -> Result<DataFrame> {
    if rows.is_empty() {
        return Ok(DataFrame::default());
    }
    
    let columns = rows[0].columns();
    let mut series_vec = Vec::new();
    
    for (idx, col) in columns.iter().enumerate() {
        let col_name = col.name();
        let type_name = col.type_info().name();
        
        let series = match type_name {
            "BOOL" => {
                let values: Vec<Option<bool>> = rows.iter()
                    .map(|row| row.try_get(idx).ok().flatten())
                    .collect();
                Series::new(col_name, values)
            }
            "INT2" | "INT4" => {
                let values: Vec<Option<i32>> = rows.iter()
                    .map(|row| row.try_get(idx).ok().flatten())
                    .collect();
                Series::new(col_name, values)
            }
            "INT8" => {
                let values: Vec<Option<i64>> = rows.iter()
                    .map(|row| row.try_get(idx).ok().flatten())
                    .collect();
                Series::new(col_name, values)
            }
            "FLOAT4" | "FLOAT8" => {
                let values: Vec<Option<f64>> = rows.iter()
                    .map(|row| row.try_get(idx).ok().flatten())
                    .collect();
                Series::new(col_name, values)
            }
            "TEXT" | "VARCHAR" => {
                let values: Vec<Option<String>> = rows.iter()
                    .map(|row| row.try_get(idx).ok().flatten())
                    .collect();
                Series::new(col_name, values)
            }
            "TIMESTAMP" | "TIMESTAMPTZ" => {
                use chrono::NaiveDateTime;
                let values: Vec<Option<i64>> = rows.iter()
                    .map(|row| {
                        row.try_get::<Option<NaiveDateTime>, _>(idx)
                            .ok()
                            .flatten()
                            .map(|dt| dt.timestamp_millis())
                    })
                    .collect();
                Series::new(col_name, values)
                    .cast(&DataType::Datetime(TimeUnit::Milliseconds, None))?
            }
            _ => {
                // Fallback to string
                let values: Vec<Option<String>> = rows.iter()
                    .map(|row| row.try_get(idx).ok().flatten())
                    .collect();
                Series::new(col_name, values)
            }
        };
        
        series_vec.push(series);
    }
    
    DataFrame::new(series_vec)
        .map_err(|e| DataError::DataFrameConversion(e.to_string()))
}
```

### Example 3: DataFrame Export Helper

```rust
pub fn dataframe_to_insert_sql(df: &DataFrame, table_name: &str) -> Result<String> {
    let column_names: Vec<&str> = df.get_column_names();
    let height = df.height();
    
    if height == 0 {
        return Err(DataError::Config("Cannot export empty DataFrame".to_string()));
    }
    
    // Build column list
    let columns = column_names.join(", ");
    
    // Build values
    let mut value_rows = Vec::new();
    for row_idx in 0..height {
        let mut row_values = Vec::new();
        
        for col_name in &column_names {
            let series = df.column(col_name)?;
            let value = series_value_to_sql_string(series, row_idx)?;
            row_values.push(value);
        }
        
        value_rows.push(format!("({})", row_values.join(", ")));
    }
    
    Ok(format!(
        "INSERT INTO {} ({}) VALUES {}",
        table_name,
        columns,
        value_rows.join(", ")
    ))
}

fn series_value_to_sql_string(series: &Series, idx: usize) -> Result<String> {
    if series.is_null(idx)? {
        return Ok("NULL".to_string());
    }
    
    match series.dtype() {
        DataType::Boolean => {
            let val = series.bool()?.get(idx).unwrap();
            Ok(val.to_string())
        }
        DataType::Int64 => {
            let val = series.i64()?.get(idx).unwrap();
            Ok(val.to_string())
        }
        DataType::Float64 => {
            let val = series.f64()?.get(idx).unwrap();
            Ok(val.to_string())
        }
        DataType::Utf8 => {
            let val = series.utf8()?.get(idx).unwrap();
            Ok(format!("'{}'", val.replace("'", "''")))
        }
        _ => Err(DataError::TypeInference(format!(
            "Unsupported type for SQL export: {:?}",
            series.dtype()
        )))
    }
}
```

---

## Part 6: Key Implementation Considerations

### 1. Type System Mapping

**Challenge**: Database types don't always map 1:1 to Polars types

**Solution**: Create comprehensive type mapping tables per database:

```rust
pub struct TypeMapper {
    db_type: DatabaseType,
}

impl TypeMapper {
    pub fn db_type_to_polars(&self, db_type: &str) -> Result<DataType> {
        match (self.db_type, db_type) {
            (DatabaseType::Postgres, "INT4") => Ok(DataType::Int32),
            (DatabaseType::Postgres, "INT8") => Ok(DataType::Int64),
            (DatabaseType::Postgres, "FLOAT8") => Ok(DataType::Float64),
            (DatabaseType::Postgres, "TEXT") => Ok(DataType::Utf8),
            (DatabaseType::Postgres, "TIMESTAMP") => {
                Ok(DataType::Datetime(TimeUnit::Microseconds, None))
            }
            // ... comprehensive mapping
            _ => Err(DataError::TypeInference(format!(
                "Unknown type mapping: {} for {:?}",
                db_type, self.db_type
            )))
        }
    }
    
    pub fn polars_type_to_db(&self, polars_type: &DataType) -> Result<String> {
        match (self.db_type, polars_type) {
            (DatabaseType::Postgres, DataType::Int32) => Ok("INTEGER".to_string()),
            (DatabaseType::Postgres, DataType::Int64) => Ok("BIGINT".to_string()),
            (DatabaseType::Postgres, DataType::Float64) => Ok("DOUBLE PRECISION".to_string()),
            (DatabaseType::Postgres, DataType::Utf8) => Ok("TEXT".to_string()),
            (DatabaseType::Postgres, DataType::Datetime(_, _)) => Ok("TIMESTAMP".to_string()),
            // ... comprehensive mapping
            _ => Err(DataError::TypeInference(format!(
                "Unknown type mapping: {:?} for {:?}",
                polars_type, self.db_type
            )))
        }
    }
}
```

### 2. NULL Handling

**Polars**: Uses `Option<T>` in Series (nullable by default)  
**Databases**: Explicit NULL support

**Key Points**:
- Preserve NULL semantics when reading
- Handle NULL correctly when writing
- Test edge cases (all NULLs, no NULLs, partial NULLs)

### 3. Memory Management

**Large DataFrames**:
- Consider streaming for large result sets
- Implement chunking for exports
- Monitor memory usage in tests

```rust
async fn export_dataframe_chunked(
    &self,
    df: &DataFrame,
    table_name: &str,
    chunk_size: usize,
) -> Result<u64> {
    let height = df.height();
    let mut total_inserted = 0;
    
    for chunk_start in (0..height).step_by(chunk_size) {
        let chunk_end = (chunk_start + chunk_size).min(height);
        let chunk = df.slice(chunk_start as i64, chunk_end - chunk_start);
        
        let inserted = self.export_dataframe(&chunk, table_name, IfExistsStrategy::Append).await?;
        total_inserted += inserted;
    }
    
    Ok(total_inserted)
}
```

### 4. Transaction Support

**rusty-data** doesn't expose transactions. For arni:

```rust
#[async_trait]
pub trait DatabaseAdapter {
    // ... existing methods
    
    async fn begin_transaction(&mut self) -> Result<()>;
    async fn commit_transaction(&mut self) -> Result<()>;
    async fn rollback_transaction(&mut self) -> Result<()>;
}

// Usage
adapter.begin_transaction().await?;
match adapter.export_dataframe(&df1, "table1", IfExistsStrategy::Replace).await {
    Ok(_) => {
        adapter.export_dataframe(&df2, "table2", IfExistsStrategy::Replace).await?;
        adapter.commit_transaction().await?;
    }
    Err(e) => {
        adapter.rollback_transaction().await?;
        return Err(e);
    }
}
```

### 5. Performance Benchmarks

**Establish Baselines**:
- Time to read 1M rows to DataFrame
- Time to export 1M rows from DataFrame
- Memory usage for various DataFrame sizes
- Connection pool efficiency

**Optimization Targets**:
- < 1s for 100k row reads
- < 2s for 100k row exports
- < 500MB memory for 1M row DataFrame

### 6. Testing Strategy

**Unit Tests**:
- Type conversion (each database type)
- NULL handling
- Error cases (invalid types, empty DataFrames)

**Integration Tests**:
- Round-trip (export → import → compare)
- Large DataFrames (chunking)
- Concurrent operations
- Transaction rollback

**Property-Based Tests** (using `proptest`):
- Generate random DataFrames
- Export and import back
- Verify data integrity

---

## Part 7: Migration Checklist

### Codebase Setup
- [ ] Create `arni` workspace with `arni` and `arni-cli` crates
- [ ] Copy `rusty-logging` (minimal changes)
- [ ] Setup CI/CD with GitHub Actions
- [ ] Configure `clippy` and `rustfmt`

### Core Types
- [ ] Copy and adapt `error.rs` (add DataFrame errors)
- [ ] Copy `ConnectionConfig`, `DatabaseType`
- [ ] Create new `QueryResult` with DataFrame
- [ ] Define `IfExistsStrategy`
- [ ] Copy schema types (`TableInfo`, `ColumnInfo`, etc.)

### Adapter Trait
- [ ] Define new `DatabaseAdapter` trait with DataFrame methods
- [ ] Keep introspection methods as-is
- [ ] Add `export_dataframe`, `read_table` methods

### Type Conversion
- [ ] Create `TypeMapper` utility
- [ ] Implement database → Polars conversions
- [ ] Implement Polars → database conversions
- [ ] Test NULL handling thoroughly

### PostgreSQL Adapter
- [ ] Copy structure from rusty-data
- [ ] Implement `rows_to_dataframe`
- [ ] Implement `export_dataframe`
- [ ] Write unit tests
- [ ] Write integration tests

### Other Adapters
- [ ] SQLite adapter
- [ ] MySQL adapter
- [ ] MongoDB adapter (special handling)
- [ ] SQL Server adapter
- [ ] Oracle adapter

### Configuration
- [ ] Decide on security model (keep encryption or simplify)
- [ ] Implement ConfigManager
- [ ] Support environment variables

### CLI
- [ ] Connection management commands
- [ ] Query execution
- [ ] DataFrame export (CSV, Parquet, Arrow)
- [ ] Results formatting

### Documentation
- [ ] API documentation (rustdoc)
- [ ] User guide
- [ ] Type mapping reference
- [ ] Performance tuning guide

### Testing
- [ ] Setup test database containers
- [ ] Write comprehensive integration tests
- [ ] Performance benchmarks
- [ ] Memory leak tests

---

## Conclusion

The rusty-app codebase provides an excellent foundation for the arni project. The adapter pattern, error handling, configuration management, and logging infrastructure are production-ready and can be adapted with focused changes:

**Key Adaptation Work**:
1. **Replace QueryResult with DataFrame** (core change)
2. **Implement type conversion layers** (database ↔ Polars)
3. **Add export_dataframe method** (new functionality)
4. **Enhance error handling** (DataFrame-specific errors)

**Reusable Components**:
- Connection management
- Configuration system (with encryption)
- Logging infrastructure (use as-is)
- Introspection methods (minor tweaks)
- Testing patterns (adapt for DataFrames)

**Estimated Timeline**: 10-12 weeks for full implementation with comprehensive testing

**Recommended Start**: PostgreSQL adapter (most feature-complete, good testing ground)

This analysis provides a detailed roadmap for successfully adapting rusty-data and rusty-logging to the arni project while leveraging the Polars DataFrame ecosystem.
