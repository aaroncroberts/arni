#!/usr/bin/env node
/**
 * arni daemon Node.js client example
 *
 * Demonstrates how a Node.js process can talk to the arni daemon over a
 * Unix-domain socket using its newline-delimited JSON protocol.
 *
 * Prerequisites
 * -------------
 * 1. Add at least one connection profile:
 *      arni config add --name mydb --type sqlite --database :memory:
 *
 * 2. Start the daemon in a separate terminal (prints socket path on stdout):
 *      arni daemon --socket /tmp/arni.sock
 *    Or let this script start it automatically (see `startDaemon` below).
 *
 * 3. Run this script:
 *      node examples/node_client.js
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
 *  {"cmd":"disconnect","profile":"mydb"} {"ok":true}
 *  {"cmd":"shutdown"}                    {"ok":true}
 */

'use strict';

const net  = require('net');
const path = require('path');

const SOCKET_PATH = process.env.ARNI_SOCKET ?? '/tmp/arni.sock';
const PROFILE     = process.env.ARNI_PROFILE ?? 'mydb';

// ─── Low-level socket helper ──────────────────────────────────────────────────

/**
 * Open a persistent connection to the arni daemon socket.
 *
 * Returns an object with a single async method `send(cmd)` that:
 *  - serialises `cmd` to JSON + '\n'
 *  - writes it to the socket
 *  - waits for exactly one '\n'-terminated response line
 *  - parses and returns the response object
 *
 * Call `close()` when finished to tear down the socket.
 */
function createArniClient(socketPath) {
  const socket = net.createConnection(socketPath);

  // Buffer incoming bytes until a full '\n'-terminated line arrives.
  let buffer = '';
  const pending = [];  // queue of { resolve, reject } waiting for a response

  socket.setEncoding('utf8');

  socket.on('data', (chunk) => {
    buffer += chunk;
    let newline;
    while ((newline = buffer.indexOf('\n')) !== -1) {
      const line = buffer.slice(0, newline);
      buffer = buffer.slice(newline + 1);
      if (line.trim() === '') continue;
      const waiter = pending.shift();
      if (waiter) {
        try {
          waiter.resolve(JSON.parse(line));
        } catch (e) {
          waiter.reject(new Error(`Failed to parse response: ${line}`));
        }
      }
    }
  });

  socket.on('error', (err) => {
    // Reject all outstanding waiters on socket error.
    for (const w of pending.splice(0)) w.reject(err);
  });

  /**
   * Send one command and await the daemon's response.
   * @param {object} cmd  JSON-serialisable command object.
   * @returns {Promise<object>}  Parsed response from the daemon.
   */
  function send(cmd) {
    return new Promise((resolve, reject) => {
      pending.push({ resolve, reject });
      socket.write(JSON.stringify(cmd) + '\n');
    });
  }

  /** Gracefully close the socket. */
  function close() {
    socket.destroy();
  }

  /** Wait for the socket to be connected before sending commands. */
  function ready() {
    return new Promise((resolve, reject) => {
      if (socket.readyState === 'open') return resolve();
      socket.once('connect', resolve);
      socket.once('error', reject);
    });
  }

  return { send, close, ready };
}

// ─── Demo ─────────────────────────────────────────────────────────────────────

async function main() {
  console.log(`Connecting to arni daemon at ${SOCKET_PATH} ...`);

  const client = createArniClient(SOCKET_PATH);
  await client.ready();
  console.log('Connected.\n');

  try {
    // 1. Explicit connect (optional — query/tables connect lazily)
    let res = await client.send({ cmd: 'connect', profile: PROFILE });
    console.log('connect   →', res);

    // 2. List tables
    res = await client.send({ cmd: 'tables', profile: PROFILE });
    console.log('tables    →', res);

    // 3. Run a query
    res = await client.send({ cmd: 'query', profile: PROFILE, sql: 'SELECT 1 AS n, 42 AS answer' });
    console.log('query     →', res);

    if (res.ok) {
      console.log('\nResult set:');
      console.log('  columns:', res.columns);
      for (const row of res.rows) {
        console.log('  row    :', row);
      }
    }

    // 4. Disconnect (evicts the connection from the registry)
    res = await client.send({ cmd: 'disconnect', profile: PROFILE });
    console.log('\ndisconnect →', res);

    // 5. Shut the daemon down (comment this out if you want the daemon to
    //    keep running after the demo)
    res = await client.send({ cmd: 'shutdown' });
    console.log('shutdown   →', res);

  } finally {
    client.close();
  }
}

main().catch((err) => {
  console.error('Error:', err.message);
  process.exit(1);
});
