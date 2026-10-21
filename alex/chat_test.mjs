import { io } from 'socket.io-client';
const msgs = process.argv.slice(2);
const socket = io('http://127.0.0.1:4001/widget', { auth: { widgetKey: 'wk_92110a037412', page: 'https://premiumboxes.com/alex-test', pageTitle: 'Alex test' }, transports: ['websocket'] });
let resolveBot = null;
const t0 = Date.now();
const log = (s) => console.log(`[${((Date.now() - t0) / 1000).toFixed(1)}s] ${s}`);
socket.on('chat:message', ({ message: m }) => {
  log(`${m.senderType} (${m.kind}): ${m.kind === 'FILE' ? `${m.body} ${m.file?.mime} ${m.file?.size}B ${m.file?.downloadUrl}` : m.body}`);
  if (m.senderType === 'BOT' && m.kind === 'TEXT' && resolveBot) resolveBot();
});
socket.on('chat:typing', (t) => log(`typing=${t.typing}`));
socket.on('app:error', (e) => log(`ERROR ${JSON.stringify(e)}`));
socket.on('widget:ready', async (r) => {
  log(`ready as visitor ${r.visitor?.id}`);
  for (const body of msgs) {
    log(`VISITOR> ${body}`);
    const done = new Promise((res) => { resolveBot = res; setTimeout(res, 75000); });
    socket.emit('widget:message', { body, tempId: String(Math.random()) });
    await done; resolveBot = null;
  }
  socket.close(); process.exit(0);
});
setTimeout(() => { log('timeout'); process.exit(1); }, 280000);
