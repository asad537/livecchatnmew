import { io } from 'socket.io-client';
const reply = process.argv[2];
const socket = io('http://127.0.0.1:4001/widget', { auth: { widgetKey: 'wk_92110a037412', page: 'https://premiumboxes.com/greet-test' }, transports: ['websocket'] });
const t0 = Date.now(); const log = (s) => console.log(`[${((Date.now() - t0) / 1000).toFixed(1)}s] ${s}`);
let gotBot = 0;
socket.on('chat:message', ({ message: m }) => { log(`${m.senderType}${m.sender?.name ? ' ' + m.sender.name : ''}: ${m.body}`); if (m.senderType === 'BOT') gotBot++; });
socket.on('widget:ready', (r) => log(`ready, conversation=${r.conversation ? r.conversation.id : 'none'}`));
setTimeout(() => { if (reply) { log(`VISITOR> ${reply}`); socket.emit('widget:message', { body: reply, tempId: 'x' }); } }, 11000);
setTimeout(() => { socket.close(); process.exit(gotBot ? 0 : 1); }, reply ? 24000 : 12000);
