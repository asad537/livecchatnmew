import { io } from 'socket.io-client';
import fs from 'node:fs';
const s = io('http://127.0.0.1:4001/widget', { auth: { widgetKey: 'wk_92110a037412', page: 'https://premiumboxes.com/ref-test' }, transports: ['websocket'] });
let token, convId, step = 0;
s.on('widget:ready', (r) => { token = r.visitorToken; s.emit('widget:message', { body: 'this is my box design', tempId: 'u' }); });
s.on('chat:message', async ({ message: m }) => {
  if (m.senderType === 'VISITOR' && !convId) {
    convId = m.conversationId;
    const fd = new FormData(); fd.append('conversationId', convId);
    fd.append('file', new Blob([fs.readFileSync('/root/alex/mockup-nova3.png')], { type: 'image/png' }), 'my-design.png');
    await fetch('http://127.0.0.1:4001/api/uploads', { method: 'POST', headers: { authorization: `Bearer ${token}` }, body: fd });
  }
  if (m.senderType === 'BOT' && m.kind === 'FILE') console.log('FILE:', m.body, m.file?.mime, m.file?.size);
  if (m.senderType === 'BOT' && m.kind === 'TEXT') {
    console.log('BOT:', m.body.replace(/\n+/g, ' ⏎ ').slice(0, 220)); step++;
    if (step === 1) setTimeout(() => s.emit('widget:message', { body: 'can you show me a mockup of this box in matte black with the same logo?', tempId: 'q' }), 500);
    if (step === 2) setTimeout(() => s.emit('widget:message', { body: 'and a real photo of a printed mailer box you made before', tempId: 'p' }), 500);
    if (step >= 3) { s.close(); process.exit(0); }
  }
});
setTimeout(() => process.exit(1), 150000);
