import { io } from 'socket.io-client';
import fs from 'node:fs';
const base = process.argv[2] || 'http://127.0.0.1:4001';
const s = io('http://127.0.0.1:4001/widget', { auth: { widgetKey: 'wk_92110a037412', page: 'https://premiumboxes.com/upload-test' }, transports: ['websocket'] });
let token, convId;
s.on('widget:ready', (r) => { token = r.visitorToken; s.emit('widget:message', { body: 'can I send you my design?', tempId: 'u' }); });
s.on('chat:message', async ({ message: m }) => {
  if (m.senderType === 'VISITOR' && !convId) {
    convId = m.conversationId;
    const png = fs.readFileSync('/root/alex/mockup-nova3.png');
    const fd = new FormData();
    fd.append('conversationId', convId);
    fd.append('file', new Blob([png], { type: 'image/png' }), 'design.png');
    try {
      const res = await fetch(`${base}/api/uploads`, { method: 'POST', headers: { authorization: `Bearer ${token}`, origin: 'https://www.premiumboxes.com' }, body: fd });
      console.log('upload', base, res.status, (await res.text()).slice(0, 300));
    } catch (e) { console.log('upload error', e.message); }
    setTimeout(() => { s.close(); process.exit(0); }, 8000);
  }
  if (m.senderType === 'BOT') console.log('BOT:', m.kind, m.body.slice(0, 100));
});
setTimeout(() => process.exit(1), 30000);
