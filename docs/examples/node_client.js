#!/usr/bin/env node
/**
 * arni daemon Node.js client — v1.0 full-protocol example
 *
 * Demonstrates how a Node.js process can talk to the arni daemon over a
 * Unix-domain socket using its newline-delimited JSON (NDJSON) protocol.
 *
 * Prerequisites
 * -------------
 * 1. Add at least one connection profile:
 *      arni config add --name mydb --type sqlite --database :memory:
 *
 * 2. Start the daemon in a separate terminal (prints socket path on stdout):
 *      arni daemon --socket /tmp/arni.sock
 *
 * 3. Run this script:
 *      node docs/examples/node_client.js
 *
 * Protocol summary
 * ----------------
 * Every message is a single-line JSON object terminated by '\n'.
 *
 *  Request                               Response
 *  {"cmd":"connect","profile":"mydb"}    {"ok":true}
 *  {"cmd":"tables","profile":"mydb"}     {"ok":true,"tables":["foo","bar"]}
 *  {"cmd":"query","profile":"mydb",      {"ok":true,"columns":["n"],
 *         "sql":"SELECT 1 AS n"}          "rows":[[1]]}
 *  {"cmd":"version"}                     {"ok":true,"protocol":"1.0","arni_version":"0.1.0"}
 *  {"cmd":"shutdown"}                    {"ok":true}
 *
 * See docs/daemon.md for the full command reference.
 */

'use strict';

const net  = require('net');

const SOCKET_PATH = process.env.ARNI_SOCKET ?? '/tmp/arni.sock';
const PROFILE     = process.env.ARNI_PROFILE ?? 'mydb';

// ─── Low-level socket transport ───────────────────────────────────────────────

/**
 * Open a persistent connection to the arni daemon socket.
 *
 * Returns { send, close, ready } where `send(cmd)` serialises the command
 * object to JSON + '\n', writes it, and resolves with the parsed response.
 * One request → one response; responses arrive in strict request order.
 */
function createTransport(socketPath) {
  const socket = net.createConnection(socketPath);

  let buffer = '';
  const pending = [];   // { resolve, reject } queue

  socket.setEncoding('utf8');

  socket.on('data', (chunk) => {
    buffer += chunk;
    let nl;
    while ((nl = buffer.indexOf('\n')) !== -1) {
      const line = buffer.slice(0, nl);
      buffer = buffer.slice(nl + 1);
      if (line.trim() === '') continue;
      const waiter = pending.shift();
      if (waiter) {
        try   { waiter.resolve(JSON.parse(line)); }
        catch { waiter.reject(new Error(`Unparseable response: ${line}`)); }
      }
    }
  });

  socket.on('error', (err) => {
    for (const w of pending.splice(0)) w.reject(err);
  });

  function send(cmd) {
    return new Promise((resolve, reject) => {
      pending.push({ resolve, reject });
      socket.write(JSON.stringify(cmd) + '\n');
    });
  }

  function close() { socket.destroy(); }

  function ready() {
    return new Promise((resolve, reject) => {
      if (socket.readyState === 'open') return resolve();
      socket.once('connect', resolve);
      socket.once('error',   reject);
    });
  }

  return { send, close, ready };
}

// ─── High-level ArniClient ────────────────────────────────────────────────────

/**
 * Typed client for the full arni daemon v1.0 protocol.
 *
 * Usage:
 *   const client = await ArniClient.connect('/tmp/arni.sock');
 *   const { rows } = await client.query('mydb', 'SELECT 1');
 *   await client.close();
 */
class ArniClient {
  /** @param {ReturnType<typeof createTransport>} transport */
  constructor(transport) {
    this._t = transport;
  }

  /** Open a connection and return a ready ArniClient. */
  static async connect(socketPath = SOCKET_PATH) {
    const t = createTransport(socketPath);
    await t.ready();
    return new ArniClient(t);
  }

  /** Gracefully close the socket. */
  close() { this._t.close(); }

  // ── Utility ──────────────────────────────────────────────────────────────

  /** Return daemon protocol and build version. Use as a health-check. */
  version() {
    return this._t.send({ cmd: 'version' });
  }

  // ── Core ─────────────────────────────────────────────────────────────────

  /** Pre-warm a connection for `profile`. Optional — all commands connect lazily. */
  connect(profile) {
    return this._t.send({ cmd: 'connect', profile });
  }

  /** Evict `profile`'s connection from the registry. */
  disconnect(profile) {
    return this._t.send({ cmd: 'disconnect', profile });
  }

  /** Execute a SQL statement and return all rows. */
  query(profile, sql) {
    return this._t.send({ cmd: 'query', profile, sql });
  }

  /** List all tables in the database. */
  tables(profile) {
    return this._t.send({ cmd: 'tables', profile });
  }

  /** Stop the daemon. */
  shutdown() {
    return this._t.send({ cmd: 'shutdown' });
  }

  // ── Metadata ─────────────────────────────────────────────────────────────

  /**
   * Return column definitions and row statistics for a table.
   * @param {string|null} schema  Defaults to the DB default schema when null.
   */
  describeTable(profile, table, schema = null) {
    return this._t.send({ cmd: 'describe_table', profile, table, schema });
  }

  /** List all databases / schemas visible to the connected user. */
  listDatabases(profile) {
    return this._t.send({ cmd: 'list_databases', profile });
  }

  /** Return all indexes for a table. */
  getIndexes(profile, table, schema = null) {
    return this._t.send({ cmd: 'get_indexes', profile, table, schema });
  }

  /** Return all foreign keys defined on a table. */
  getForeignKeys(profile, table, schema = null) {
    return this._t.send({ cmd: 'get_foreign_keys', profile, table, schema });
  }

  /** List all views in a schema. */
  getViews(profile, schema = null) {
    return this._t.send({ cmd: 'get_views', profile, schema });
  }

  /** Return database server version and type. */
  getServerInfo(profile) {
    return this._t.send({ cmd: 'get_server_info', profile });
  }

  /** List stored procedures and functions in a schema. */
  listStoredProcedures(profile, schema = null) {
    return this._t.send({ cmd: 'list_stored_procedures', profile, schema });
  }

  /**
   * Search for tables whose names match a pattern.
   * @param {'contains'|'starts'|'ends'} mode  Default: 'contains'.
   */
  findTables(profile, pattern, mode = 'contains', schema = null) {
    return this._t.send({ cmd: 'find_tables', profile, pattern, mode, schema });
  }

  // ── Bulk operations ───────────────────────────────────────────────────────

  /**
   * Insert multiple rows in a single batched operation.
   * @param {string[]}   columns  Column names in order.
   * @param {Array[]}    rows     Each inner array maps to `columns`.
   */
  bulkInsert(profile, table, columns, rows, schema = null) {
    return this._t.send({ cmd: 'bulk_insert', profile, table, columns, rows, schema });
  }

  /**
   * Update rows matching a filter expression.
   * @param {object} filter  Filter DSL expression (see docs/daemon.md).
   * @param {object} values  Flat { column: newValue } map.
   */
  bulkUpdate(profile, table, filter, values, schema = null) {
    return this._t.send({ cmd: 'bulk_update', profile, table, filter, values, schema });
  }

  /**
   * Delete rows matching a filter expression.
   * @param {object} filter  Filter DSL expression (see docs/daemon.md).
   */
  bulkDelete(profile, table, filter, schema = null) {
    return this._t.send({ cmd: 'bulk_delete', profile, table, filter, schema });
  }
}

// ─── Demo ─────────────────────────────────────────────────────────────────────

async function main() {
  console.log(`Connecting to arni daemon at ${SOCKET_PATH} ...`);

  const client = await ArniClient.connect(SOCKET_PATH);
  console.log('Connected.\n');

  try {
    // ── Utility ────────────────────────────────────────────────────────────
    const ver = await client.version();
    console.log('version          →', ver);
    // → { ok: true, protocol: '1.0', arni_version: '0.1.0' }

    // ── Core ───────────────────────────────────────────────────────────────
    console.log('\n--- Core commands ---');

    let res = await client.connect(PROFILE);
    console.log('connect          →', res);

    res = await client.tables(PROFILE);
    console.log('tables           →', res);

    res = await client.query(PROFILE, 'SELECT 1 AS n, 42 AS answer');
    console.log('query            →', res);
    if (res.ok) {
      console.log('  columns:', res.columns);
      for (const row of res.rows) console.log('  row    :', row);
    }

    // ── Metadata ───────────────────────────────────────────────────────────
    console.log('\n--- Metadata commands ---');

    res = await client.getServerInfo(PROFILE);
    console.log('get_server_info  →', res);

    res = await client.listDatabases(PROFILE);
    console.log('list_databases   →', res);

    // Prepare a demo table for introspection
    await client.query(PROFILE, `
      CREATE TABLE IF NOT EXISTS users (
        id    INTEGER PRIMARY KEY,
        name  TEXT    NOT NULL,
        email TEXT    UNIQUE
      )
    `);

    res = await client.describeTable(PROFILE, 'users');
    console.log('describe_table   →', JSON.stringify(res, null, 2));

    res = await client.getIndexes(PROFILE, 'users');
    console.log('get_indexes      →', res);

    res = await client.getForeignKeys(PROFILE, 'users');
    console.log('get_foreign_keys →', res);

    res = await client.getViews(PROFILE);
    console.log('get_views        →', res);

    res = await client.listStoredProcedures(PROFILE);
    console.log('list_procs       →', res);

    res = await client.findTables(PROFILE, 'user', 'contains');
    console.log('find_tables      →', res);

    // ── Bulk operations ────────────────────────────────────────────────────
    console.log('\n--- Bulk operations ---');

    res = await client.bulkInsert(
      PROFILE, 'users',
      ['id', 'name', 'email'],
      [
        [1, 'Alice', 'alice@example.com'],
        [2, 'Bob',   'bob@example.com'],
      ]
    );
    console.log('bulk_insert      →', res);
    // → { ok: true, rows_affected: 2 }

    res = await client.bulkUpdate(
      PROFILE, 'users',
      { id: { eq: 2 } },          // filter: WHERE id = 2
      { name: 'Robert' }           // set: name = 'Robert'
    );
    console.log('bulk_update      →', res);
    // → { ok: true, rows_affected: 1 }

    res = await client.query(PROFILE, 'SELECT id, name, email FROM users');
    console.log('query after upd  →', res);

    res = await client.bulkDelete(
      PROFILE, 'users',
      { id: { in: [1, 2] } }       // filter: WHERE id IN (1, 2)
    );
    console.log('bulk_delete      →', res);
    // → { ok: true, rows_affected: 2 }

    // ── Cleanup ─────────────────────────────────────────────────────────────
    console.log('\n--- Cleanup ---');

    res = await client.disconnect(PROFILE);
    console.log('disconnect       →', res);

    // Comment out the next line to keep the daemon running after the demo.
    res = await client.shutdown();
    console.log('shutdown         →', res);

  } finally {
    client.close();
  }
}

main().catch((err) => {
  console.error('Error:', err.message);
  process.exit(1);
});
