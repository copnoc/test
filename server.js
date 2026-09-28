import http from "node:http";
import https from "node:https";
import { WebSocketServer, WebSocket } from "ws";

const PLAIN = new Set(["80", "8080", "8880", "2052", "2082", "2086", "2095"]);
const PORT = process.env.PORT || 3000;

function parse(reqUrl) {
  const u = new URL(reqUrl, "http://x");
  const segs = u.pathname.replace(/^\/+/, "").split("/");
  if (!segs[0]) return null;
  const [host, port] = segs[0].split(":");
  const plain = !!port && PLAIN.has(port);
  return {
    host, port, plain,
    hostPort: port ? `${host}:${port}` : host,
    path: "/" + segs.slice(1).join("/") + u.search,
  };
}

const server = http.createServer((req, res) => {
  const t = parse(req.url);
  if (!t) { res.end("Service Operational"); return; }
  const mod = t.plain ? http : https;
  const up = mod.request(
    {
      host: t.host,
      port: t.port || (t.plain ? 80 : 443),
      path: t.path,
      method: req.method,
      headers: { ...req.headers, host: t.host },
    },
    (r) => { res.writeHead(r.statusCode, r.headers); r.pipe(res); }
  );
  up.on("error", () => { res.statusCode = 502; res.end(); });
  req.pipe(up);
});

const wss = new WebSocketServer({
  noServer: true,
  handleProtocols: (protocols) => protocols.values().next().value,
});

server.on("upgrade", (req, socket, head) => {
  const t = parse(req.url);
  if (!t) return socket.destroy();

  const protos = req.headers["sec-websocket-protocol"]
    ?.split(",").map((s) => s.trim()).filter(Boolean);

  wss.handleUpgrade(req, socket, head, (client) => {
    const upstream = new WebSocket(
      `${t.plain ? "ws" : "wss"}://${t.hostPort}${t.path}`,
      protos && protos.length ? protos : undefined
    );
    const queue = [];

    client.on("message", (data, isBinary) => {
      if (upstream.readyState === WebSocket.OPEN) upstream.send(data, { binary: isBinary });
      else queue.push([data, isBinary]);
    });
    upstream.on("open", () => {
      queue.forEach(([d, b]) => upstream.send(d, { binary: b }));
      queue.length = 0;
    });
    upstream.on("message", (data, isBinary) => {
      if (client.readyState === WebSocket.OPEN) client.send(data, { binary: isBinary });
    });

    const close = () => { client.terminate(); upstream.terminate(); };
    client.on("close", close);  client.on("error", close);
    upstream.on("close", close); upstream.on("error", close);
  });
});

server.listen(PORT, "0.0.0.0");