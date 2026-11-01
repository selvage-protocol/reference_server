#!/usr/bin/env node
/**
 * The demo's browser seat: joins the room in Chromium, types a name, and opens the file the
 * editors are in, driven over the DevTools protocol so the clicks land on what they name
 * rather than on a pixel of a layout.
 *
 *   node scripts/demo/browser.mjs --port 9222 --link <the invite's page link>
 */

const args = new Map();
for (let index = 2; index < process.argv.length; index += 2) {
  args.set(process.argv[index].replace(/^--/, ''), process.argv[index + 1]);
}

const port = args.get('port') ?? '9222';
const link = args.get('link');
const name = args.get('name') ?? 'Grace (browser)';

if (!link) {
  console.error('browser.mjs needs --link');
  process.exit(2);
}

const pause = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

async function target() {
  for (let attempt = 0; attempt < 100; attempt += 1) {
    try {
      const list = await (await fetch(`http://127.0.0.1:${port}/json/list`)).json();
      const page = list.find((entry) => entry.type === 'page');
      if (page) {
        return page.webSocketDebuggerUrl;
      }
    } catch {
      // The browser is still starting.
    }
    await pause(200);
  }
  throw new Error('no Chromium page target answered');
}

const socket = new WebSocket(await target());
await new Promise((resolve, reject) => {
  socket.onopen = resolve;
  socket.onerror = () => reject(new Error('the DevTools socket refused'));
});

let nextId = 0;
const waiting = new Map();
socket.onmessage = (event) => {
  const message = JSON.parse(event.data);
  const resolve = waiting.get(message.id);
  if (resolve) {
    waiting.delete(message.id);
    resolve(message);
  }
};

function send(method, params = {}) {
  const id = ++nextId;
  return new Promise((resolve, reject) => {
    waiting.set(id, (message) => {
      if (message.error) {
        reject(new Error(`${method}: ${message.error.message}`));
      } else {
        resolve(message.result);
      }
    });
    socket.send(JSON.stringify({ id, method, params }));
  });
}

async function until(expression, what) {
  for (let attempt = 0; attempt < 150; attempt += 1) {
    const result = await send('Runtime.evaluate', { expression, returnByValue: true });
    if (result.result?.value) {
      return;
    }
    await pause(200);
  }
  throw new Error(`the page never showed ${what}`);
}

await send('Page.enable');
await send('Page.navigate', { url: link });
await until('!!document.getElementById("name")', 'the join card');

await send('Runtime.evaluate', {
  expression: `(() => {
    const field = document.getElementById('name');
    field.focus();
    field.value = ${JSON.stringify(name)};
    field.dispatchEvent(new Event('input', { bubbles: true }));
    return field.value;
  })()`,
  returnByValue: true,
});
await send('Input.dispatchKeyEvent', { type: 'keyDown', windowsVirtualKeyCode: 13, key: 'Enter' });
await send('Input.dispatchKeyEvent', { type: 'char', text: '\r' });
await send('Input.dispatchKeyEvent', { type: 'keyUp', windowsVirtualKeyCode: 13, key: 'Enter' });

await until('document.body.innerText.includes("In ")', 'the room');
await pause(1500);

const room = await send('Runtime.evaluate', {
  expression: 'document.body.innerText.replace(/\\n+/g, " ").slice(0, 160)',
  returnByValue: true,
});
process.stdout.write(`browser: ${room.result?.value ?? ''}\n`);

await pause(500);
socket.close();
process.exit(0);
