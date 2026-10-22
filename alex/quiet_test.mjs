// Like chat_test.mjs but waits until the bot goes quiet (typing off + 6 s with no bot message).
import { io } from "socket.io-client";
const msgs = process.argv.slice(2);
const socket = io("http://127.0.0.1:4001/widget", { auth: { widgetKey: "wk_92110a037412", page: "https://premiumboxes.com/alex-test", pageTitle: "Alex test" }, transports: ["websocket"] });
const t0 = Date.now();
const log = (s) => console.log(`[${((Date.now() - t0) / 1000).toFixed(1)}s] ${s}`);
let lastBot = 0, typingOn = false, gotBot = false;
socket.on("chat:message", ({ message: m }) => {
  log(`${m.senderType} (${m.kind}): ${m.kind === "FILE" ? `${m.body} ${m.file?.mime} ${m.file?.size}B` : m.body.replace(/\n+/g, " ⏎ ")}`);
  if (m.senderType === "BOT") { lastBot = Date.now(); gotBot = true; }
});
socket.on("chat:typing", (t) => { typingOn = !!t.typing; });
socket.on("app:error", (e) => log(`ERROR ${JSON.stringify(e)}`));
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
socket.on("widget:ready", async (r) => {
  log(`ready as visitor ${r.visitor?.id}`);
  for (const body of msgs) {
    log(`VISITOR> ${body}`);
    gotBot = false; lastBot = Date.now();
    socket.emit("widget:message", { body, tempId: String(Math.random()) });
    const start = Date.now();
    while (Date.now() - start < 240000) {
      await sleep(1000);
      if (gotBot && !typingOn && Date.now() - lastBot > 6000) break;
    }
  }
  socket.close(); process.exit(0);
});
setTimeout(() => { log("timeout"); process.exit(1); }, 600000);
