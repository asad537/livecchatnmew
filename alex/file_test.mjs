import { io } from "socket.io-client";
const socket = io("http://127.0.0.1:4001/widget", { auth: { widgetKey: "wk_92110a037412", page: "https://premiumboxes.com/alex-test", pageTitle: "Alex test" }, transports: ["websocket"] });
socket.on("chat:message", async ({ message: m }) => {
  if (m.kind === "FILE") { console.log("FILE msg:", JSON.stringify({ body: m.body, file: m.file }).slice(0, 600));
    const url = m.file?.url?.startsWith("http") ? m.file.url : "http://127.0.0.1:4001" + (m.file?.url ?? "");
    const r = await fetch(url); console.log("GET", url, r.status, r.headers.get("content-type"), r.headers.get("content-length")); socket.close(); process.exit(0); }
});
socket.on("widget:ready", () => { socket.emit("widget:message", { body: "show me a photo of a cigarette box", tempId: "1" }); });
setTimeout(() => process.exit(1), 120000);
