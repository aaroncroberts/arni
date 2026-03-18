# Cloudflare Worker & Node.js Integration Guide

This guide shows how to expose the arni daemon over the network so that Cloudflare Workers, serverless functions, and remote Node.js processes can query databases through arni.

## Architecture overview

```
┌─────────────────┐    HTTPS     ┌──────────────────────┐
│ Cloudflare      │ ──────────► │ HTTP Bridge Server    │
│ Worker          │ ◄────────── │ (Node.js / any lang)  │
│                 │  JSON resp  │                        │
└─────────────────┘             │  Unix socket client   │
                                └──────────┬───────────-┘
                                           │ NDJSON over
                                           │ Unix socket
                                ┌──────────▼────────────┐
                                │   arni daemon          │
                                │   /tmp/arni.sock       │
                                └──────────┬────────────┘
                                           │
                      ┌────────────────────┼──────────────────┐
                      ▼                    ▼                   ▼
                 PostgreSQL            DuckDB              MySQL
```

The daemon itself only listens on a Unix socket — it is not directly network-accessible. A thin **HTTP bridge** process sits between the network and the daemon: it receives HTTP requests from Workers (or any HTTP client), forwards them as daemon commands over the socket, and streams the response back.

This design keeps the daemon simple and trust-free. All network-level authentication, TLS, and rate limiting are handled by the bridge.

---

## Part 1 — HTTP bridge server

The bridge is a small Node.js HTTP server. It translates incoming HTTP POST requests into daemon commands and returns the daemon's JSON response.

### `bridge/server.js`

```js
'use strict';

const http   = require('http');
const net    = require('net');
const crypto = require('crypto');

const SOCKET_PATH = process.env.ARNI_SOCKET  ?? '/tmp/arni.sock';
const PORT        = parseInt(process.env.PORT ?? '4200', 10);
const API_KEY     = process.env.ARNI_API_KEY;        // Required in production

if (!API_KEY) {
  console.warn('[arni-bridge] WARNING: ARNI_API_KEY is not set — all requests are accepted');
}

// ── Daemon client ─────────────────────────────────────────────────────────────

/**
 * Send a single command to the arni daemon and return the parsed response.
 * Each call opens a fresh socket connection and closes it when done.
 * For high-throughput use, a connection pool is preferable.
 */
function sendDaemonCommand(cmd) {
  return new Promise((resolve, reject) => {
    const socket = net.createConnection(SOCKET_PATH);
    let buffer = '';

    socket.setEncoding('utf8');
    socket.setTimeout(30_000);

    socket.on('connect', () => {
      socket.write(JSON.stringify(cmd) + '\n');
    });

    socket.on('data', (chunk) => {
      buffer += chunk;
      const newline = buffer.indexOf('\n');
      if (newline !== -1) {
        const line = buffer.slice(0, newline).trim();
        socket.destroy();
        try {
          resolve(JSON.parse(line));
        } catch {
          reject(new Error(`Daemon returned non-JSON: ${line}`));
        }
      }
    });

    socket.on('timeout', () => {
      socket.destroy();
      reject(new Error('Daemon connection timed out'));
    });

    socket.on('error', reject);
  });
}

// ── HTTP server ───────────────────────────────────────────────────────────────

const server = http.createServer(async (req, res) => {
  // Only accept POST /cmd
  if (req.method !== 'POST' || req.url !== '/cmd') {
    res.writeHead(404, { 'Content-Type': 'application/json' });
    return res.end(JSON.stringify({ ok: false, error: 'Not found' }));
  }

  // API key check
  if (API_KEY) {
    const auth = req.headers['x-arni-key'] ?? '';
    if (!crypto.timingSafeEqual(Buffer.from(auth), Buffer.from(API_KEY))) {
      res.writeHead(401, { 'Content-Type': 'application/json' });
      return res.end(JSON.stringify({ ok: false, error: 'Unauthorized' }));
    }
  }

  // Read request body
  let body = '';
  for await (const chunk of req) body += chunk;

  let cmd;
  try {
    cmd = JSON.parse(body);
  } catch {
    res.writeHead(400, { 'Content-Type': 'application/json' });
    return res.end(JSON.stringify({ ok: false, error: 'Invalid JSON body' }));
  }

  // Block dangerous commands at the bridge layer
  const BLOCKED = ['shutdown'];
  if (BLOCKED.includes(cmd.cmd)) {
    res.writeHead(403, { 'Content-Type': 'application/json' });
    return res.end(JSON.stringify({ ok: false, error: `Command '${cmd.cmd}' is not allowed` }));
  }

  try {
    const response = await sendDaemonCommand(cmd);
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify(response));
  } catch (err) {
    res.writeHead(502, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ ok: false, error: `Bridge error: ${err.message}` }));
  }
});

server.listen(PORT, '127.0.0.1', () => {
  console.log(`[arni-bridge] Listening on http://127.0.0.1:${PORT}`);
  console.log(`[arni-bridge] Daemon socket: ${SOCKET_PATH}`);
});
```

### Running the bridge

```bash
# Set your secret key
export ARNI_API_KEY="$(openssl rand -hex 32)"

# Start the daemon (in another terminal or as a service)
arni daemon --socket /tmp/arni.sock &

# Start the bridge
node bridge/server.js
```

The bridge binds to `127.0.0.1` only. Use a reverse proxy (nginx, Caddy) or a tunnel (Cloudflare Tunnel) to add TLS before exposing it to the internet.

---

## Part 2 — Cloudflare Worker

The Worker calls the bridge over HTTPS and forwards the result to the client. It can be deployed as a standard Cloudflare Worker or as an API endpoint in a Pages project.

### `worker/index.js`

```js
/**
 * Cloudflare Worker — arni database gateway
 *
 * Routes:
 *   POST /query     Run a SQL query
 *   POST /tables    List tables
 *   POST /meta      Describe table, list databases, get indexes, etc.
 *   POST /write     Bulk insert / update / delete
 */

const BRIDGE_URL = 'https://your-bridge-host.example.com/cmd';
const ALLOWED_COMMANDS = new Set([
  'connect', 'disconnect', 'query', 'tables',
  'describe_table', 'list_databases', 'get_indexes', 'get_foreign_keys',
  'get_views', 'get_server_info', 'list_stored_procedures', 'find_tables',
  'bulk_insert', 'bulk_update', 'bulk_delete', 'version',
]);

export default {
  async fetch(request, env) {
    if (request.method !== 'POST') {
      return new Response(JSON.stringify({ ok: false, error: 'POST required' }), {
        status: 405,
        headers: { 'Content-Type': 'application/json' },
      });
    }

    let cmd;
    try {
      cmd = await request.json();
    } catch {
      return jsonError(400, 'Invalid JSON body');
    }

    if (!ALLOWED_COMMANDS.has(cmd.cmd)) {
      return jsonError(400, `Unknown command: ${cmd.cmd}`);
    }

    // Forward to bridge
    const bridgeRes = await fetch(BRIDGE_URL, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'X-Arni-Key': env.ARNI_API_KEY,   // Stored in Worker secret
      },
      body: JSON.stringify(cmd),
    });

    const data = await bridgeRes.json();

    return new Response(JSON.stringify(data), {
      status: bridgeRes.ok ? 200 : 502,
      headers: { 'Content-Type': 'application/json' },
    });
  },
};

function jsonError(status, message) {
  return new Response(JSON.stringify({ ok: false, error: message }), {
    status,
    headers: { 'Content-Type': 'application/json' },
  });
}
```

### Deploying

1. Create a Worker secret for the API key:
   ```bash
   wrangler secret put ARNI_API_KEY
   ```

2. Update `wrangler.toml`:
   ```toml
   name = "arni-gateway"
   main = "worker/index.js"
   compatibility_date = "2025-01-01"
   ```

3. Deploy:
   ```bash
   wrangler deploy
   ```

### Calling the Worker from client code

```js
const WORKER = 'https://arni-gateway.your-account.workers.dev';

async function arniQuery(profile, sql) {
  const res = await fetch(WORKER, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ cmd: 'query', profile, sql }),
  });
  return res.json();
}

const result = await arniQuery('analytics', 'SELECT count(*) FROM events');
console.log(result.rows);  // [[42000]]
```

---

## Part 3 — Node.js client library

For server-side Node.js code that connects directly to the daemon socket (no bridge needed), use the client library in `docs/examples/node_client.js` as a base. Here is a production-ready version with connection pooling and full command coverage:

### `arni-client.js`

```js
'use strict';

const net = require('net');

/**
 * A persistent client connection to the arni daemon.
 *
 * Usage:
 *   const client = new ArniClient('/tmp/arni.sock');
 *   await client.connect();
 *   const rows = await client.query('mydb', 'SELECT 1');
 *   client.close();
 */
class ArniClient {
  constructor(socketPath = '/tmp/arni.sock') {
    this.socketPath = socketPath;
    this._socket   = null;
    this._buffer   = '';
    this._pending  = [];   // { resolve, reject }[]
  }

  /** Open the socket connection. Must be called before any commands. */
  connect() {
    return new Promise((resolve, reject) => {
      this._socket = net.createConnection(this.socketPath);
      this._socket.setEncoding('utf8');

      this._socket.on('data', (chunk) => {
        this._buffer += chunk;
        let newline;
        while ((newline = this._buffer.indexOf('\n')) !== -1) {
          const line = this._buffer.slice(0, newline).trim();
          this._buffer = this._buffer.slice(newline + 1);
          if (!line) continue;
          const waiter = this._pending.shift();
          if (waiter) {
            try { waiter.resolve(JSON.parse(line)); }
            catch (e) { waiter.reject(new Error(`Bad JSON from daemon: ${line}`)); }
          }
        }
      });

      this._socket.on('error', (err) => {
        for (const w of this._pending.splice(0)) w.reject(err);
        reject(err);
      });

      this._socket.once('connect', resolve);
    });
  }

  /** Send one command object and return the parsed response. */
  send(cmd) {
    return new Promise((resolve, reject) => {
      this._pending.push({ resolve, reject });
      this._socket.write(JSON.stringify(cmd) + '\n');
    });
  }

  /** Close the socket. */
  close() {
    if (this._socket) this._socket.destroy();
  }

  // ── Convenience wrappers ────────────────────────────────────────────────────

  /** Explicitly connect a profile (optional — other methods connect lazily). */
  preConnect(profile) {
    return this.send({ cmd: 'connect', profile });
  }

  /** Run a SQL query. Returns { ok, columns, rows }. */
  query(profile, sql) {
    return this.send({ cmd: 'query', profile, sql });
  }

  /** List tables in the default schema. */
  tables(profile) {
    return this.send({ cmd: 'tables', profile });
  }

  /** Describe a table's columns and statistics. */
  describeTable(profile, table, schema = null) {
    return this.send({ cmd: 'describe_table', profile, table, schema });
  }

  /** List all databases/schemas. */
  listDatabases(profile) {
    return this.send({ cmd: 'list_databases', profile });
  }

  /** Get indexes for a table. */
  getIndexes(profile, table, schema = null) {
    return this.send({ cmd: 'get_indexes', profile, table, schema });
  }

  /** Get foreign keys for a table. */
  getForeignKeys(profile, table, schema = null) {
    return this.send({ cmd: 'get_foreign_keys', profile, table, schema });
  }

  /** List views in a schema. */
  getViews(profile, schema = null) {
    return this.send({ cmd: 'get_views', profile, schema });
  }

  /** Get database server version and type. */
  getServerInfo(profile) {
    return this.send({ cmd: 'get_server_info', profile });
  }

  /** List stored procedures in a schema. */
  listStoredProcedures(profile, schema = null) {
    return this.send({ cmd: 'list_stored_procedures', profile, schema });
  }

  /**
   * Search for tables by name pattern.
   * @param {string} mode  "contains" | "starts" | "ends"
   */
  findTables(profile, pattern, mode = 'contains', schema = null) {
    return this.send({ cmd: 'find_tables', profile, pattern, mode, schema });
  }

  /**
   * Insert multiple rows.
   * @param {string[]} columns  Column names in order.
   * @param {Array[]}  rows     Arrays of values matching columns.
   */
  bulkInsert(profile, table, columns, rows, schema = null) {
    return this.send({ cmd: 'bulk_insert', profile, table, columns, rows, schema });
  }

  /**
   * Update rows matching a filter.
   * @param {object} filter  Filter DSL expression.
   * @param {object} values  Flat { col: newValue } map.
   */
  bulkUpdate(profile, table, filter, values, schema = null) {
    return this.send({ cmd: 'bulk_update', profile, table, filter, values, schema });
  }

  /**
   * Delete rows matching a filter.
   * @param {object} filter  Filter DSL expression.
   */
  bulkDelete(profile, table, filter, schema = null) {
    return this.send({ cmd: 'bulk_delete', profile, table, filter, schema });
  }

  /** Evict a profile from the connection registry. */
  disconnect(profile) {
    return this.send({ cmd: 'disconnect', profile });
  }

  /** Return daemon protocol and arni version. */
  version() {
    return this.send({ cmd: 'version' });
  }
}

module.exports = { ArniClient };
```

### Full lifecycle example

```js
const { ArniClient } = require('./arni-client');

async function main() {
  const client = new ArniClient('/tmp/arni.sock');
  await client.connect();

  const PROFILE = 'mydb';

  // Version check
  const ver = await client.version();
  console.log('Daemon version:', ver);
  // { ok: true, protocol: '1.0', arni_version: '0.1.0' }

  // Query
  const q = await client.query(PROFILE, 'SELECT count(*) AS n FROM users');
  console.log('Row count:', q.rows[0][0]);

  // Describe table
  const schema = await client.describeTable(PROFILE, 'users');
  console.log('Columns:', schema.columns.map(c => c.name));

  // Bulk insert
  const ins = await client.bulkInsert(PROFILE, 'users',
    ['name', 'email'],
    [['Charlie', 'charlie@example.com'], ['Dana', 'dana@example.com']]
  );
  console.log('Inserted:', ins.rows_affected);

  // Bulk update with filter DSL
  const upd = await client.bulkUpdate(PROFILE, 'users',
    { name: { eq: 'Charlie' } },
    { email: 'charlie2@example.com' }
  );
  console.log('Updated:', upd.rows_affected);

  // Bulk delete
  const del = await client.bulkDelete(PROFILE, 'users',
    { name: { eq: 'Dana' } }
  );
  console.log('Deleted:', del.rows_affected);

  // Find tables
  const found = await client.findTables(PROFILE, 'user', 'contains');
  console.log('Tables matching "user":', found.tables);

  // Get server info
  const info = await client.getServerInfo(PROFILE);
  console.log('Server:', info.server.server_type, info.server.version);

  client.close();
}

main().catch(err => { console.error(err); process.exit(1); });
```

---

## Security checklist

Before deploying to production:

- [ ] **TLS**: Put the bridge behind nginx/Caddy with a valid certificate. Workers only call HTTPS endpoints.
- [ ] **API key**: Set `ARNI_API_KEY` to a strong random secret (`openssl rand -hex 32`). Store it as a Worker secret (`wrangler secret put`).
- [ ] **Allowed-command list**: The Worker's `ALLOWED_COMMANDS` set prevents clients from issuing unintended commands. Review it against your use case.
- [ ] **Blocked commands**: The bridge blocks `shutdown` by default. Add any other commands you don't want network clients to invoke.
- [ ] **Network isolation**: Bind the bridge to `127.0.0.1` (loopback only). Never expose it on `0.0.0.0` without a firewall rule.
- [ ] **Socket permissions**: Run the daemon as a dedicated user; restrict socket permissions to that user. See [daemon.md](daemon.md#security-considerations).
- [ ] **Rate limiting**: Add rate limiting in the bridge or at the Cloudflare layer to prevent abuse.
- [ ] **Read-only profiles**: Create a read-only database user for `query`/`tables`/`describe_table` calls that don't need write access.

---

## Deployment topology examples

### Development (local)

```
localhost
  arni daemon   (/tmp/arni.sock)
  bridge server (http://127.0.0.1:4200)
  → test with curl or the Node.js client
```

### Production on a VPS

```
VPS
  arni daemon   (/var/run/arni/arni.sock,  arni-svc user)
  bridge server (http://127.0.0.1:4200,    arni-svc user)
  nginx/Caddy   (https://db.example.com → 127.0.0.1:4200, TLS termination)
```

### Cloudflare Tunnel (no open ports)

```
VPS (no inbound ports open)
  arni daemon
  bridge server (http://127.0.0.1:4200)
  cloudflared tunnel → https://arni-bridge.example.com

Cloudflare Worker
  fetch("https://arni-bridge.example.com/cmd", ...)
```

This topology keeps the bridge entirely off the public internet — traffic flows through Cloudflare's network without any open TCP ports on the VPS.
