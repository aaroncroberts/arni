# Cloudflare Adapters

Arni includes adapters for three Cloudflare storage services, each accessible via Cloudflare's REST APIs. No native Rust drivers exist for these services outside of Cloudflare Workers, so all three adapters communicate over HTTPS.

| Adapter | Service | Feature flag | Auth model |
|---------|---------|--------------|------------|
| `D1Adapter` | SQL database (SQLite-compatible) | `cloudflare-d1` | Bearer token (API token) |
| `KVAdapter` | Key-value store | `cloudflare-kv` | Bearer token (API token) |
| `R2Adapter` | Object storage (S3-compatible) | `cloudflare-r2` | R2 access key pair |

Enable all three with the `cloudflare` bundle feature.

## Prerequisites

### Cloudflare API Token (D1 and KV)

D1 and KV use Cloudflare's account-level REST API with Bearer token authentication. Create a token at **Cloudflare Dashboard → My Profile → API Tokens**:

- **D1**: requires `D1:Read` and `D1:Write` permissions
- **KV**: requires `Workers KV Storage:Read` and `Workers KV Storage:Write` permissions

### R2 Access Keys

R2 uses an S3-compatible API with its own access key pair. Generate R2 keys at **Cloudflare Dashboard → R2 → Manage R2 API Tokens**. These are separate from your Cloudflare API token.

## Connection Configuration

All three adapters use `ConnectionConfig.parameters` for their credentials. The `host`, `port`, and `username` fields are not used.

### D1

```yaml
# config.yaml
profiles:
  my-d1-db:
    connections:
      - id: d1-prod
        name: "Production D1"
        db_type: cloudflare_d1
        database: ""          # unused, kept for trait compatibility
        parameters:
          account_id: "abc123..."       # Your Cloudflare account ID
          api_token: "your-api-token"   # API token with D1 scope
          database_id: "uuid-of-db"     # D1 database UUID
```

### KV

```yaml
profiles:
  my-kv-namespace:
    connections:
      - id: kv-cache
        name: "KV Cache"
        db_type: cloudflare_kv
        database: ""
        parameters:
          account_id: "abc123..."
          api_token: "your-api-token"    # API token with KV scope
          namespace_id: "uuid-of-ns"     # KV namespace UUID
```

### R2

```yaml
profiles:
  my-r2-bucket:
    connections:
      - id: r2-assets
        name: "R2 Assets Bucket"
        db_type: cloudflare_r2
        database: ""
        parameters:
          account_id: "abc123..."
          access_key_id: "r2-access-key-id"
          secret_access_key: "r2-secret-key"
          bucket_name: "my-bucket"
```

## D1 Adapter

D1 maps cleanly onto arni's `DbAdapter` trait — it's a SQLite-compatible database, so standard SQL works as expected.

### execute_query

Sends arbitrary SQL to D1 via the `/raw` endpoint and returns a `QueryResult`:

```rust
let result = adapter.execute_query("SELECT * FROM users WHERE active = true").await?;
```

Parameterized queries use positional `?` placeholders (SQLite syntax). Parameters are passed via JSON array in the request body.

### read_table

Lists all rows from a table:

```rust
let result = adapter.read_table("users", None).await?;
```

### describe_table

Uses `PRAGMA table_info(table_name)` to return column metadata:

```rust
let info = adapter.describe_table("users", None).await?;
for col in info.columns {
    println!("{}: {} (nullable: {})", col.name, col.data_type, col.is_nullable);
}
```

### list_tables

Returns all user tables (queries `sqlite_master`):

```rust
let tables = adapter.list_tables(None, None).await?;
```

### export_dataframe (feature: `polars`)

Creates the table if it doesn't exist (DROP IF EXISTS + CREATE), then inserts rows one-by-one:

```rust
adapter.export_dataframe(&df, "users", None, false).await?;
```

### Integration tests

Set `TEST_CLOUDFLARE_D1_AVAILABLE=true` to enable D1 integration tests:

```bash
TEST_CLOUDFLARE_D1_AVAILABLE=true \
  CLOUDFLARE_ACCOUNT_ID=abc123 \
  CLOUDFLARE_API_TOKEN=token \
  CLOUDFLARE_D1_DATABASE_ID=uuid \
  cargo test -p arni --features cloudflare-d1 -- --ignored
```

## KV Adapter

Cloudflare KV is a key-value store, not a SQL database. Arni exposes it via a line-oriented DSL passed as the SQL string to `execute_query`.

### DSL commands

| Command | Description | Returns |
|---------|-------------|---------|
| `GET <key>` | Fetch the value for `key` | Single row: `(key, value)` |
| `PUT <key> <value>` | Store `value` under `key` | Empty result |
| `DELETE <key>` | Remove `key` | Empty result |
| `LIST [prefix]` | List all keys (optionally filtered by prefix) | Rows of `(key,)` |

```rust
// Read a value
let result = adapter.execute_query("GET config:app-settings").await?;
let value = &result.rows[0][1]; // QueryValue::Text(...)

// Write a value
adapter.execute_query("PUT config:app-settings {\"theme\":\"dark\"}").await?;

// List keys under a prefix
let result = adapter.execute_query("LIST config:").await?;

// Delete a key
adapter.execute_query("DELETE config:app-settings").await?;
```

### read_table

Treats the `table_name` parameter as a key prefix. Lists all matching keys and fetches their values, returning a two-column `QueryResult` with columns `key` and `value`:

```rust
// Returns all keys under "users/" and their values
let result = adapter.read_table("users/", None).await?;
```

### export_dataframe (feature: `polars`)

Serializes each DataFrame row as a JSON object stored under `<table_name>/<index>` (e.g., `users/0`, `users/1`, …):

```rust
adapter.export_dataframe(&df, "users", None, false).await?;
```

### Integration tests

```bash
TEST_CLOUDFLARE_KV_AVAILABLE=true \
  CLOUDFLARE_ACCOUNT_ID=abc123 \
  CLOUDFLARE_API_TOKEN=token \
  CLOUDFLARE_KV_NAMESPACE_ID=uuid \
  cargo test -p arni --features cloudflare-kv -- --ignored
```

## R2 Adapter

Cloudflare R2 is object storage. Arni exposes it via a DSL similar to the KV adapter.

### DSL commands

| Command | Description | Returns |
|---------|-------------|---------|
| `LIST [prefix]` | List objects (optionally filtered by prefix) | Rows: `(key, size, etag, last_modified)` |
| `GET <key>` | Download an object | Single row: `(key, bytes_as_base64)` |
| `DELETE <key>` | Remove an object | Empty result |

> **Note:** `PUT` is not supported via the DSL. Use `export_dataframe` to upload structured data, or use the AWS S3 SDK directly for raw object uploads.

```rust
// List all objects
let result = adapter.execute_query("LIST").await?;

// List objects under a prefix
let result = adapter.execute_query("LIST data/2025/").await?;

// Download an object
let result = adapter.execute_query("GET reports/annual.parquet").await?;

// Delete an object
adapter.execute_query("DELETE old/archive.tar.gz").await?;
```

### read_table

Lists all objects under the given prefix and returns a four-column result:

```rust
// Returns (key, size, etag, last_modified) for all objects under "reports/"
let result = adapter.read_table("reports/", None).await?;
```

### export_dataframe (feature: `polars`)

Serializes the DataFrame to Parquet in memory and uploads it to R2 as `<table_name>.parquet`:

```rust
// Uploads to R2 as "users.parquet"
adapter.export_dataframe(&df, "users", None, false).await?;
```

This is the primary way to store structured data in R2 via arni.

### Integration tests

```bash
TEST_CLOUDFLARE_R2_AVAILABLE=true \
  CLOUDFLARE_ACCOUNT_ID=abc123 \
  CLOUDFLARE_R2_ACCESS_KEY_ID=key \
  CLOUDFLARE_R2_SECRET_ACCESS_KEY=secret \
  CLOUDFLARE_R2_BUCKET_NAME=my-bucket \
  cargo test -p arni --features cloudflare-r2 -- --ignored
```

## Building with Cloudflare features

```bash
# Individual adapters
cargo build --features cloudflare-d1
cargo build --features cloudflare-kv
cargo build --features cloudflare-r2

# All three
cargo build --features cloudflare

# CLI with all Cloudflare adapters + polars
cargo build -p arni-cli --features "cloudflare,polars"

# MCP server with Cloudflare support
cargo build -p arni-mcp --features cloudflare
```

## Authentication summary

| Adapter | Credential fields in `parameters` |
|---------|-----------------------------------|
| D1 | `account_id`, `api_token`, `database_id` |
| KV | `account_id`, `api_token`, `namespace_id` |
| R2 | `account_id`, `access_key_id`, `secret_access_key`, `bucket_name` |

Credentials should be stored in `parameters` in your config file (see [configuration.md](configuration.md)) or injected via environment variable substitution supported by the config loader.
