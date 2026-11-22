// NextHub VLESS-WS node (pure Node.js + ws)
// UUID: d243d338-5593-44f5-88aa-27c3f5eb7ebf
// WS path: /vless
// Listens on process.env.PORT || 8080
const http = require('http');
const net = require('net');
const WebSocket = require('ws');

const UUID = 'd243d338-5593-44f5-88aa-27c3f5eb7ebf';
const WS_PATH = '/vless';
const PORT = parseInt(process.env.PORT || '8080', 10);

function uuidToBytes(uuid) {
  const hex = uuid.replace(/-/g, '');
  const b = Buffer.alloc(16);
  for (let i = 0; i < 16; i++) b[i] = parseInt(hex.substr(i * 2, 2), 16);
  return b;
}
const UUID_BYTES = uuidToBytes(UUID);

const server = http.createServer((req, res) => {
  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end('NextHub VLESS node is running.\n');
});

const wss = new WebSocket.Server({ server, path: WS_PATH });

wss.on('connection', (ws) => {
  let tcp = null;
  let headerParsed = false;
  let cache = [];

  ws.on('message', (data) => {
    const buf = Buffer.isBuffer(data) ? data : Buffer.from(data);
    if (!headerParsed) {
      if (buf.length < 18) { ws.close(); return; }
      // version (1) + uuid (16) + addon len (1)
      if (!buf.slice(1, 17).equals(UUID_BYTES)) { ws.close(); return; }
      const addonLen = buf[17];
      let off = 18 + addonLen;
      if (buf.length < off + 4) { ws.close(); return; }
      const cmd = buf[off];
      if (cmd !== 1) { ws.close(); return; } // TCP only
      const port = buf.readUInt16BE(off + 1);
      const atyp = buf[off + 3];
      let addr;
      let hdrLen;
      if (atyp === 1) {
        if (buf.length < off + 8) { ws.close(); return; }
        addr = buf.slice(off + 4, off + 8).join('.');
        hdrLen = off + 8;
      } else if (atyp === 2) {
        const dlen = buf[off + 4];
        if (buf.length < off + 5 + dlen) { ws.close(); return; }
        addr = buf.slice(off + 5, off + 5 + dlen).toString();
        hdrLen = off + 5 + dlen;
      } else if (atyp === 3) {
        if (buf.length < off + 20) { ws.close(); return; }
        const parts = [];
        for (let i = 0; i < 8; i++) parts.push(buf.readUInt16BE(off + 4 + i * 2).toString(16));
        addr = parts.join(':');
        hdrLen = off + 20;
      } else { ws.close(); return; }

      headerParsed = true;
      // VLESS response: version 0, addon len 0
      ws.send(Buffer.from([0, 0]));

      tcp = net.connect(port, addr, () => {
        for (const c of cache) tcp.write(c);
        cache = [];
      });
      tcp.on('data', (d) => { if (ws.readyState === 1) ws.send(d); });
      tcp.on('error', () => ws.close());
      tcp.on('close', () => ws.close());

      const rest = buf.slice(hdrLen);
      if (rest.length) {
        if (tcp.connecting) cache.push(rest);
        else tcp.write(rest);
      }
    } else {
      if (tcp && !tcp.destroyed) {
        if (tcp.connecting) cache.push(buf);
        else tcp.write(buf);
      }
    }
  });

  ws.on('close', () => { if (tcp) tcp.destroy(); });
  ws.on('error', () => { if (tcp) tcp.destroy(); });
});

server.listen(PORT, '0.0.0.0', () => {
  console.log('VLESS-WS listening on 0.0.0.0:' + PORT + ' path ' + WS_PATH);
  console.log('UUID: ' + UUID);
});
