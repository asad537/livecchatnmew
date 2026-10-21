import { io } from 'socket.io-client';
import fs from 'node:fs';
const s = io('http://127.0.0.1:4001/widget', { auth: { widgetKey: 'wk_92110a037412', page: 'https://premiumboxes.com/upload-test' }, transports: ['websocket'] });
let token, convId, step = 0;
s.on('widget:ready', (r) => { token = r.visitorToken; s.emit('widget:message', { body: 'here is the box I want, can you make this?', tempId: 'u' }); });
s.on('chat:message', async ({ message: m }) => {
  if (m.senderType === 'VISITOR' && !convId) {
    convId = m.conversationId;
    const fd = new FormData(); fd.append('conversationId', convId);
    fd.append('file', new Blob([fs.readFileSync('/root/alex/mockup-nova3.png')], { type: 'image/png' }), 'reference.png');
    await fetch('http://127.0.0.1:4001/api/uploads', { method: 'POST', headers: { authorization: `Bearer ${token}` }, body: fd });
  }
  if (m.senderType === 'BOT' && m.kind === 'TEXT') {
    console.log('BOT:', m.body.slice(0, 260)); step++;
    if (step === 1) setTimeout(() => s.emit('widget:message', { body: 'what colour is the logo and what does it say?', tempId: 'q' }), 500);
    if (step >= 2) { s.close(); process.exit(0); }
  }
});
setTimeout(() => process.exit(1), 60000);
