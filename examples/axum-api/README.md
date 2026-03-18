# axum-api

An Axum HTTP API backed by arni for unified database access.

Uses **SQLite in-memory** by default — zero configuration, clone and run immediately.

## Run

```bash
cargo run -p axum-api
```

The server starts on `http://localhost:3000` and seeds three sample rows into a `users` table.

## Routes

### `GET /tables`

List all tables in the database.

```bash
curl http://localhost:3000/tables
# {"tables":["users"]}
```

### `GET /query?sql=<SQL>`

Execute a SELECT query. Returns columns and rows as JSON.

```bash
curl "http://localhost:3000/query?sql=SELECT+*+FROM+users+ORDER+BY+score+DESC"
# {"columns":["id","name","email","score"],"rows":[...]}
```

### `POST /bulk-insert`

Insert multiple rows into a table.

```bash
curl -X POST http://localhost:3000/bulk-insert \
  -H "Content-Type: application/json" \
  -d '{
    "table": "users",
    "columns": ["id","name","email","score"],
    "rows": [
      [4, "Dave", "dave@example.com", 88.5],
      [5, "Eve",  "eve@example.com",  91.0]
    ]
  }'
# {"inserted":2}
```

## Switching databases

Only `make_adapter()` in `src/main.rs` needs to change — the routes are
completely adapter-agnostic. For example, to use PostgreSQL:

```rust
use arni::{adapters::postgres::PostgresAdapter, ConnectionConfig, DatabaseType};
use std::collections::HashMap;

let config = ConnectionConfig {
    id: "api".into(),
    name: "API DB".into(),
    db_type: DatabaseType::Postgres,
    host: Some("localhost".into()),
    port: Some(5432),
    database: "myapp".into(),
    username: Some("myuser".into()),
    use_ssl: false,
    parameters: HashMap::from([("password".into(), "mypass".into())]),
    pool_config: None,
};
let mut adapter = PostgresAdapter::new(config.clone());
adapter.connect(&config, None).await?;
```

Add the feature flag:

```toml
arni = { path = "../../crates/arni", features = ["postgres"] }
```

For local dev databases see [`docs/local-databases.md`](../../docs/local-databases.md).
