import { io } from "socket.io-client";
const socket = io("http://127.0.0.1:4001/widget", { auth: { widgetKey: "wk_92110a037412", page: "https://premiumboxes.com/alex-test", pageTitle: "Alex test" }, transports: ["websocket"] });
let token = null;
socket.on("chat:message", async ({ message: m }) => {
  if (m.kind === "FILE" && m.senderType === "BOT") {
    const base = "http://127.0.0.1:4001" + m.file.downloadUrl;
    for (const u of [base + "?token=" + encodeURIComponent(token), base + "?token=undefined", base]) {
      const r = await fetch(u); console.log("GET", u.replace(token, "<token>"), r.status, r.headers.get("content-type"), r.headers.get("content-length"));
    }
    socket.close(); process.exit(0);
  }
});
socket.on("widget:ready", (r) => { token = r.visitorToken; console.log("ready token?", !!token, Object.keys(r).join(",")); socket.emit("widget:message", { body: "show me a photo of a cigarette box", tempId: "1" }); });
setTimeout(() => process.exit(1), 120000);
