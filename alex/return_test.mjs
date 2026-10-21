import { io } from 'socket.io-client';
const t0 = Date.now(); const log = (s) => console.log(`[${((Date.now() - t0) / 1000).toFixed(1)}s] ${s}`);
let token = null;
function session(label, withToken, sendReply, endChat) {
  return new Promise((resolve) => {
    const s = io('http://127.0.0.1:4001/widget', { auth: { widgetKey: 'wk_92110a037412', page: 'https://premiumboxes.com/return-test', visitorToken: withToken ? token : undefined }, transports: ['websocket'] });
    s.on('widget:ready', (r) => { token = r.visitorToken; log(`${label}: ready conv=${r.conversation ? r.conversation.status : 'none'}`); });
    s.on('chat:message', ({ message: m }) => { if (m.senderType === 'BOT') log(`${label}: BOT ${m.sender?.name}: ${m.body.slice(0, 60)}`); });
    setTimeout(() => { if (sendReply) s.emit('widget:message', { body: 'yes, I need boxes', tempId: 'r' }); }, 3000);
    setTimeout(() => { if (endChat) { s.emit('widget:end-chat', {}); log(`${label}: end-chat sent`); } }, 9000);
    setTimeout(() => { s.close(); resolve(); }, 11000);
  });
}
await session('visit1', false, true, true);
await session('visit2 (returning)', true, false, false);
process.exit(0);
