#!/usr/bin/env node
/**
 * The demo's agent seat: a Model Context Protocol client that joins the room the host opened
 * and edits a file in it, over the same stdio server an editor's agent would start.
 *
 * This is a client of `dist/main.js` and nothing else — the JSON-RPC below is what an agent's
 * host sends — so the transcript it prints is the room's own answer, not a mock.
 *
 *   SELVAGE_DEMO_LINK_FILE=/tmp/link \
 *   SELVAGE_MCP_MAIN=/path/to/mcp/dist/main.js \
 *   node scripts/demo/agent.mjs
 */

import { spawn } from 'node:child_process';
import { readFileSync } from 'node:fs';
import { createInterface } from 'node:readline';

const linkFile = process.env.SELVAGE_DEMO_LINK_FILE;
const main = process.env.SELVAGE_MCP_MAIN;
if (!linkFile || !main) {
  console.error('agent.mjs needs SELVAGE_DEMO_LINK_FILE and SELVAGE_MCP_MAIN');
  process.exit(2);
}

const pause = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const say = (text) => process.stdout.write(`${text}\n`);

function waitForLink() {
  for (let attempt = 0; attempt < 600; attempt += 1) {
    try {
      const text = readFileSync(linkFile, 'utf8').trim();
      if (text !== '') {
        return text;
      }
    } catch {
      // The host has not written it yet.
    }
    Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 100);
  }
  throw new Error('the host wrote no invite within 60s');
}

const child = spawn(process.execPath, [main], {
  stdio: ['pipe', 'pipe', 'inherit'],
  env: {
    ...process.env,
    SELVAGE_SERVER_URL: process.env.SELVAGE_SERVER_URL ?? '',
    SELVAGE_DISPLAY_NAME: process.env.SELVAGE_DISPLAY_NAME ?? 'agent',
  },
});

const pending = new Map();
let nextId = 1;

createInterface({ input: child.stdout }).on('line', (line) => {
  let message;
  try {
    message = JSON.parse(line);
  } catch {
    return;
  }
  const waiter = pending.get(message.id);
  if (waiter) {
    pending.delete(message.id);
    waiter(message);
  }
});

function request(method, params) {
  const id = nextId;
  nextId += 1;
  return new Promise((resolve, reject) => {
    pending.set(id, (message) => {
      if (message.error) {
        reject(new Error(`${method}: ${message.error.message}`));
      } else {
        resolve(message.result);
      }
    });
    child.stdin.write(`${JSON.stringify({ jsonrpc: '2.0', id, method, params })}\n`);
  });
}

function notify(method, params) {
  child.stdin.write(`${JSON.stringify({ jsonrpc: '2.0', method, params })}\n`);
}

function textOf(result) {
  const content = result?.content ?? [];
  return content
    .filter((part) => part.type === 'text')
    .map((part) => part.text)
    .join('\n');
}

async function call(name, args) {
  say(`→ ${name}`);
  const result = await request('tools/call', { name, arguments: args });
  const text = textOf(result);
  if (result?.isError) {
    throw new Error(`${name} refused: ${text}`);
  }
  say(`← ${text.split('\n').slice(0, 6).join('\n')}`);
  return text;
}

async function main_() {
  const link = waitForLink();

  await request('initialize', {
    protocolVersion: '2025-11-25',
    capabilities: {},
    clientInfo: { name: 'selvage-demo', version: '1.0.0' },
  });
  notify('notifications/initialized', {});
  say('the agent’s MCP server is up');

  await call('session_join', { invite: link, display_name: 'agent' });
  await pause(1200);
  await call('list_participants', {});
  await pause(1200);

  const board = await call('read_file', { path: 'src/limit.lua' });
  await pause(600);

  const before = 'M.limits = { doing = 2 }';
  if (!board.includes(before)) {
    throw new Error('the room’s src/limit.lua is not the demo’s own file');
  }
  await call('edit_file', {
    path: 'src/limit.lua',
    old_string: before,
    new_string: 'M.limits = { doing = 3 }',
  });

  await pause(2500);
  await call('list_participants', {});
  await pause(1500);
  await call('session_leave', {});
  say('the agent left the room');
}

main_()
  .then(() => {
    child.stdin.end();
    process.exitCode = 0;
  })
  .catch((error) => {
    console.error(`agent: ${error.message}`);
    child.kill();
    process.exitCode = 1;
  });
