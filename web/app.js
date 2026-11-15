const byId = (id) => document.getElementById(id);

function status(target, state, text) {
  target.dataset.state = state;
  target.textContent = text;
}

const sseStatus = byId('sse-status');
const metrics = new EventSource('/api/v1/metrics/stream');
metrics.addEventListener('open', () => status(sseStatus, 'connected', 'Потік активний'));
metrics.addEventListener('error', () => status(sseStatus, 'error', 'Відновлення зв’язку…'));
metrics.addEventListener('message', (event) => {
  const { time, goroutines, alloc_mb } = JSON.parse(event.data);
  byId('goroutines').textContent = goroutines;
  byId('memory').textContent = alloc_mb.toFixed(2);
  const clock = time.slice(11, 19);
  const offset = time.endsWith('Z') ? 'UTC' : `UTC${time.slice(19)}`;
  byId('server-time').textContent = `${clock} ${offset}`;
});

const connection = byId('connection');
const connect = byId('connect');
const room = byId('room');
const name = byId('name');
const wsStatus = byId('ws-status');
const messages = byId('messages');
const message = byId('message');
const send = byId('send');
let socket;

function showMessage(entry) {
  const row = document.createElement('li');
  const sender = document.createElement('strong');
  sender.textContent = entry.sender;
  const time = document.createElement('time');
  time.dateTime = entry.time;
  time.textContent = entry.time.slice(11, 19);
  const body = document.createElement('p');
  body.textContent = entry.text;
  row.append(sender, time, body);
  messages.append(row);
  if (messages.children.length > 200) messages.firstElementChild.remove();
  messages.scrollTop = messages.scrollHeight;
}

connection.addEventListener('submit', (event) => {
  event.preventDefault();
  if (socket) {
    connect.disabled = true;
    message.disabled = true;
    send.disabled = true;
    socket.close(1000);
    return;
  }

  const url = new URL('/api/v1/ws', location.href);
  url.protocol = location.protocol === 'https:' ? 'wss:' : 'ws:';
  url.search = new URLSearchParams({ room: room.value, name: name.value });
  const activeRoom = room.value;
  const ws = new WebSocket(url);
  socket = ws;
  room.disabled = true;
  name.disabled = true;
  connect.textContent = 'Відключитися';
  status(wsStatus, '', 'Підключення…');
  let failed = false;

  ws.addEventListener('open', () => {
    if (ws.readyState !== WebSocket.OPEN) return;
    messages.replaceChildren();
    message.value = '';
    message.disabled = false;
    send.disabled = false;
    status(wsStatus, 'connected', `Кімната ${activeRoom}`);
    message.focus();
  });
  ws.addEventListener('message', (incoming) => showMessage(JSON.parse(incoming.data)));
  ws.addEventListener('error', () => { failed = true; });
  ws.addEventListener('close', (closed) => {
    socket = undefined;
    room.disabled = false;
    name.disabled = false;
    connect.disabled = false;
    connect.textContent = 'Підключитися';
    message.disabled = true;
    send.disabled = true;
    if (failed || !closed.wasClean) {
      status(wsStatus, 'error', 'Зв’язок перервано');
    } else {
      status(wsStatus, '', 'Відключено');
    }
  });
});

byId('message-form').addEventListener('submit', (event) => {
  event.preventDefault();
  const text = message.value.trim();
  if (!text || socket?.readyState !== WebSocket.OPEN) return;
  socket.send(text);
  message.value = '';
  message.focus();
});
