import express  from 'express';
import { createServer } from 'http';
import { Server } from 'socket.io';
import cors from 'cors';
import { v4 as uuidv4 } from 'uuid';
import { fileURLToPath } from 'url';
import path from 'path';
import QRCode from 'qrcode';
import pg from 'pg';
import pino from 'pino';
import { rmSync } from 'fs';
import makeWASocket, { DisconnectReason, useMultiFileAuthState } from '@whiskeysockets/baileys';

const { Pool } = pg;
const __dirname = path.dirname(fileURLToPath(import.meta.url));

const app    = express();
const server = createServer(app);
const io     = new Server(server, { cors: { origin: '*', methods: ['GET', 'POST'] } });

app.use(cors());
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

// ── Config ───────────────────────────────────────────────────────────────────
const HOST_URL  = process.env.HOST_URL
  || (process.env.RAILWAY_PUBLIC_DOMAIN ? `https://${process.env.RAILWAY_PUBLIC_DOMAIN}` : null)
  || `http://localhost:${process.env.PORT || 3000}`;
const MP_TOKEN  = process.env.MERCADOPAGO_ACCESS_TOKEN || '';
const ADMIN_KEY = process.env.ADMIN_KEY || 'admin123';
const DB_URL    = process.env.DATABASE_URL || '';

const FREIGHT_TABLE = [
  { maxKm: 3,        price: 8  },
  { maxKm: 6,        price: 12 },
  { maxKm: 10,       price: 16 },
  { maxKm: 15,       price: 22 },
  { maxKm: Infinity, price: 30 },
];

// ── PostgreSQL ────────────────────────────────────────────────────────────────
const db = DB_URL ? new Pool({ connectionString: DB_URL, ssl: { rejectUnauthorized: false } }) : null;

async function dbInit() {
  if (!db) { console.log('⚠️  DATABASE_URL não configurado — dados em memória'); return; }
  await db.query(`
    CREATE TABLE IF NOT EXISTS establishments (
      phone   TEXT PRIMARY KEY,
      name    TEXT NOT NULL,
      address TEXT NOT NULL,
      lat     DOUBLE PRECISION NOT NULL,
      lng     DOUBLE PRECISION NOT NULL
    );
  `);
  const rows = await db.query('SELECT * FROM establishments');
  for (const r of rows.rows)
    establishments.set(r.phone, { name: r.name, address: r.address, lat: r.lat, lng: r.lng });
  console.log(`✅ PostgreSQL conectado — ${rows.rows.length} estabelecimento(s)`);
}

// ── In-memory stores ─────────────────────────────────────────────────────────
const deliveries      = new Map();
const pendingRequests = new Map();
const sessions        = new Map();
const establishments  = new Map();
const orders          = new Map();
const paymentToOrder  = new Map();

// ── WhatsApp (Baileys) ────────────────────────────────────────────────────────
let waSocket  = null;
let currentQR = null;
let waStatus  = 'disconnected';
const WA_DIR  = '/tmp/wa_session';
const logger  = pino({ level: 'silent' });

// Log buffer para diagnóstico via /debug
const waLogs = [];
function waLog(...args) {
  const msg = args.join(' ');
  waLogs.push(`[${new Date().toISOString()}] ${msg}`);
  if (waLogs.length > 100) waLogs.shift();
  console.log(msg);
}

async function connectWA() {
  try {
    waLog('🔄 Iniciando Baileys...');
    const { state, saveCreds } = await useMultiFileAuthState(WA_DIR);
    waLog('✅ Auth state carregado');

    const sock = makeWASocket({
      auth: state,
      logger,
      printQRInTerminal: false,
      browser: ['MotoBot', 'Chrome', '120'],
      connectTimeoutMs: 60000,
    });
    waLog('✅ Socket criado, aguardando conexão...');

    sock.ev.on('creds.update', saveCreds);

    sock.ev.on('connection.update', ({ connection, lastDisconnect, qr }) => {
      waLog(`📡 connection.update: connection=${connection} qr=${!!qr}`);
      if (qr) { currentQR = qr; waStatus = 'qr'; waLog('📱 QR gerado!'); }
      if (connection === 'open')  { waSocket = sock; waStatus = 'open'; currentQR = null; waLog('✅ WhatsApp conectado!'); }
      if (connection === 'close') {
        waSocket = null; waStatus = 'disconnected';
        const code = lastDisconnect?.error?.output?.statusCode;
        const reconnect = code !== DisconnectReason.loggedOut;
        waLog(`⚠️ WA fechou (${code}) — ${reconnect ? 'reconectando' : 'sessão expirada'}`);
        if (!reconnect) rmSync(WA_DIR, { recursive: true, force: true });
        setTimeout(connectWA, reconnect ? 5000 : 3000);
      }
    });

    sock.ev.on('messages.upsert', async ({ messages, type }) => {
      if (type !== 'notify') return;
      for (const msg of messages) {
        if (msg.key.fromMe) continue;
        const phone = msg.key.remoteJid?.replace('@s.whatsapp.net', '');
        const text  = msg.message?.conversation || msg.message?.extendedTextMessage?.text || '';
        if (phone && text) await handleBotMessage(phone, text);
      }
    });
  } catch (e) {
    waLog(`❌ connectWA erro: ${e.message}`);
    waStatus = 'error';
    setTimeout(connectWA, 10000);
  }
}

async function sendWhatsApp(phone, text) {
  if (!waSocket) { console.log(`[WA offline → ${phone}]`, text.slice(0, 50)); return; }
  try { await waSocket.sendMessage(`${phone}@s.whatsapp.net`, { text }); }
  catch (e) { console.error('WA send error:', e.message); }
}

// ── Geocodificação ────────────────────────────────────────────────────────────
async function geocode(address) {
  try {
    const res  = await fetch(`https://nominatim.openstreetmap.org/search?q=${encodeURIComponent(address)}&format=json&limit=1&countrycodes=br`, { headers: { 'User-Agent': 'DeliveryBot/1.0' } });
    const data = await res.json();
    if (!data.length) return null;
    return { lat: parseFloat(data[0].lat), lng: parseFloat(data[0].lon), display: data[0].display_name };
  } catch { return null; }
}

async function getRoadDistanceKm(from, to) {
  try {
    const res  = await fetch(`https://router.project-osrm.org/route/v1/driving/${from.lng},${from.lat};${to.lng},${to.lat}?overview=false`);
    const data = await res.json();
    return data.code === 'Ok' ? data.routes[0].distance / 1000 : null;
  } catch { return null; }
}

function calcFreight(km) { return FREIGHT_TABLE.find(t => km <= t.maxKm).price; }

// ── Mercado Pago PIX ─────────────────────────────────────────────────────────
async function createPixPayment(orderId, amount, description, phone) {
  if (!MP_TOKEN) throw new Error('MERCADOPAGO_ACCESS_TOKEN não configurado');
  const res = await fetch('https://api.mercadopago.com/v1/payments', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${MP_TOKEN}`, 'X-Idempotency-Key': orderId },
    body: JSON.stringify({ transaction_amount: amount, description, payment_method_id: 'pix', payer: { email: `${phone}@entrega.bot` } }),
  });
  const data = await res.json();
  if (!data.id) throw new Error(JSON.stringify(data));
  return { paymentId: String(data.id), copyPaste: data.point_of_interaction?.transaction_data?.qr_code || '' };
}

async function getPaymentStatus(paymentId) {
  try {
    const res  = await fetch(`https://api.mercadopago.com/v1/payments/${paymentId}`, { headers: { 'Authorization': `Bearer ${MP_TOKEN}` } });
    const data = await res.json();
    return data.status;
  } catch { return null; }
}

// ── Criar corrida após pagamento ─────────────────────────────────────────────
async function createRequestFromOrder(orderId) {
  const order = orders.get(orderId);
  if (!order || order.requestId) return;
  const requestId = uuidv4().slice(0, 8).toUpperCase();
  const request = { requestId, customerName: order.establishmentName, address: order.deliveryAddress, pickupAddress: order.pickupAddress, phone: order.phone, note: order.note, distanceKm: order.distanceKm, freightPrice: order.freightPrice, orderId, status: 'pending', createdAt: new Date().toISOString() };
  order.requestId = requestId; order.status = 'pending';
  pendingRequests.set(requestId, request);
  io.emit('new-delivery-request', request);
  console.log(`🚀 Corrida #${requestId} criada — ${order.establishmentName}`);
  await sendWhatsApp(order.phone, `✅ *Pagamento confirmado!*\n\nSeu pedido *#${orderId}* está na fila.\n🛵 Localizando o motoboy mais próximo...`);
}

// ── Bot ───────────────────────────────────────────────────────────────────────
async function handleBotMessage(phone, text) {
  const msg   = text.trim();
  const estab = establishments.get(phone);

  if (!estab) {
    await sendWhatsApp(phone, '❌ Número não cadastrado. Fale com o administrador para cadastrar seu estabelecimento.');
    return;
  }

  let session = sessions.get(phone) || { state: 'greeting' };

  if (/^cancelar$/i.test(msg) && session.state !== 'awaiting_payment') {
    sessions.delete(phone);
    await sendWhatsApp(phone, '❌ Pedido cancelado. Quando precisar, é só chamar! 😊');
    return;
  }

  switch (session.state) {
    case 'greeting':
    default:
      sessions.set(phone, { state: 'awaiting_delivery_address' });
      await sendWhatsApp(phone, `👋 Olá, *${estab.name}*!\n\n📍 *Retirada:* ${estab.address}\n\nPara qual endereço é a entrega?`);
      break;

    case 'awaiting_delivery_address': {
      await sendWhatsApp(phone, '⏳ Calculando distância e frete...');
      const dest = await geocode(msg);
      if (!dest) { await sendWhatsApp(phone, '❌ Endereço não encontrado. Tente com rua, número e cidade:'); return; }
      const km = await getRoadDistanceKm({ lat: estab.lat, lng: estab.lng }, dest);
      if (!km) { await sendWhatsApp(phone, '❌ Não consegui calcular a rota. Tente novamente:'); return; }
      const freight = calcFreight(km);
      session = { state: 'awaiting_note', deliveryAddress: msg, deliveryCoords: dest, distanceKm: Math.round(km * 10) / 10, freightPrice: freight };
      sessions.set(phone, session);
      await sendWhatsApp(phone, `📦 *Resumo:*\n\n🏪 Retirada: ${estab.address}\n📍 Entrega: ${msg}\n📏 Distância: ${session.distanceKm} km\n💰 Frete: *R$ ${freight.toFixed(2).replace('.', ',')}*\n\nAlguma observação para o motoboy? (ou responda *não*)`);
      break;
    }

    case 'awaiting_note':
      session.note  = /^n[ãa]o$/i.test(msg) ? '' : msg;
      session.state = 'awaiting_confirm';
      sessions.set(phone, session);
      await sendWhatsApp(phone, `Confirme:\n\n🏪 *Retirada:* ${estab.address}\n📍 *Entrega:* ${session.deliveryAddress}\n📏 *Distância:* ${session.distanceKm} km\n💰 *Frete:* R$ ${session.freightPrice.toFixed(2).replace('.', ',')}${session.note ? `\n📝 *Obs:* ${session.note}` : ''}\n\n*SIM* para gerar o PIX ou *NÃO* para cancelar.`);
      break;

    case 'awaiting_confirm':
      if (/^sim$/i.test(msg)) {
        const orderId = uuidv4().slice(0, 8).toUpperCase();
        const order = { orderId, phone, establishmentName: estab.name, pickupAddress: estab.address, pickupCoords: { lat: estab.lat, lng: estab.lng }, deliveryAddress: session.deliveryAddress, deliveryCoords: session.deliveryCoords, note: session.note || '', distanceKm: session.distanceKm, freightPrice: session.freightPrice, paymentId: null, requestId: null, status: 'awaiting_payment', createdAt: new Date().toISOString() };
        orders.set(orderId, order);
        sessions.delete(phone);
        await sendWhatsApp(phone, '⏳ Gerando PIX...');
        try {
          const { paymentId, copyPaste } = await createPixPayment(orderId, session.freightPrice, `Frete #${orderId} — ${estab.name}`, phone);
          order.paymentId = paymentId;
          paymentToOrder.set(paymentId, orderId);
          sessions.set(phone, { state: 'awaiting_payment', orderId });
          await sendWhatsApp(phone, `💳 *PIX gerado!*\n\nValor: *R$ ${session.freightPrice.toFixed(2).replace('.', ',')}*\n\nCopie o código:\n\n${copyPaste}\n\n_Confirmação automática após pagamento._\n\nPara cancelar: *CANCELAR*`);
        } catch (e) {
          console.error('MP error:', e.message);
          orders.delete(orderId);
          await sendWhatsApp(phone, '❌ Erro ao gerar o PIX. Tente novamente.');
        }
      } else {
        sessions.delete(phone);
        await sendWhatsApp(phone, '❌ Pedido cancelado. Quando precisar, é só chamar! 😊');
      }
      break;

    case 'awaiting_payment':
      await sendWhatsApp(phone, '⏳ Aguardando pagamento PIX...\n\nPara cancelar: *CANCELAR*');
      break;
  }
}

// ── Helpers mapa ──────────────────────────────────────────────────────────────
function interpolatePoints(from, to, steps = 5) {
  const pts = [];
  for (let i = 1; i <= steps; i++) { const t = i / steps; pts.push({ lat: from.lat + (to.lat - from.lat) * t, lng: from.lng + (to.lng - from.lng) * t }); }
  return pts;
}
function emitInterpolated(deliveryId, points, ms = 280) {
  points.forEach((p, i) => setTimeout(() => io.to(deliveryId).emit('location-update', { deliveryId, lat: p.lat, lng: p.lng, step: i + 1, total: points.length }), i * ms));
}

// ── Admin ─────────────────────────────────────────────────────────────────────
function requireAdmin(req, res, next) {
  if (req.headers['x-admin-key'] !== ADMIN_KEY) return res.status(401).json({ error: 'Unauthorized' });
  next();
}

app.post('/admin/establishment', requireAdmin, async (req, res) => {
  const { phone, name, address } = req.body;
  if (!phone || !name || !address) return res.status(400).json({ error: 'phone, name e address obrigatórios' });
  const coords = await geocode(address);
  if (!coords) return res.status(400).json({ error: 'Endereço não encontrado' });
  const estab = { name, address, lat: coords.lat, lng: coords.lng };
  establishments.set(String(phone), estab);
  if (db) await db.query('INSERT INTO establishments(phone,name,address,lat,lng) VALUES($1,$2,$3,$4,$5) ON CONFLICT(phone) DO UPDATE SET name=$2,address=$3,lat=$4,lng=$5', [String(phone), name, address, coords.lat, coords.lng]);
  console.log(`🏪 Estabelecimento: ${name} (${phone})`);
  res.json({ ok: true, name, address, coords: { lat: coords.lat, lng: coords.lng } });
});

app.get('/admin/establishments', requireAdmin, (_req, res) =>
  res.json([...establishments.entries()].map(([phone, e]) => ({ phone, ...e }))));

app.get('/admin/orders', requireAdmin, (_req, res) =>
  res.json([...orders.values()].sort((a, b) => b.createdAt.localeCompare(a.createdAt))));

// ── QR Code ───────────────────────────────────────────────────────────────────
const page = (body, ref = 5) => `<html><head><meta http-equiv="refresh" content="${ref}"><style>body{font-family:sans-serif;text-align:center;padding:40px;background:#f5f5f5}img{border-radius:12px;box-shadow:0 4px 20px rgba(0,0,0,.15)}</style></head><body>${body}</body></html>`;

app.get('/qr', async (_req, res) => {
  if (waStatus === 'open')   return res.send(page('<h2 style="color:green">✅ WhatsApp conectado!</h2><p>Bot ativo.</p>', 60));
  if (!currentQR)            return res.send(page('<h2>⏳ Gerando QR Code...</h2><p>Aguarde. Página atualiza sozinha.</p>'));
  const img = await QRCode.toDataURL(currentQR);
  res.send(page(`<h2>📱 Escaneie com o WhatsApp</h2><img src="${img}" style="max-width:280px"><br><br><p>WhatsApp → <b>Dispositivos conectados</b> → <b>Conectar dispositivo</b></p><p><small>QR expira em ~60s</small></p>`, 20));
});

// ── Rotas de entrega ──────────────────────────────────────────────────────────
app.post('/request-delivery', (req, res) => {
  const { customerName = 'Cliente', address = '', phone = '', note = '' } = req.body;
  const requestId = uuidv4().slice(0, 8).toUpperCase();
  const request = { requestId, customerName, address, phone, note, status: 'pending', createdAt: new Date().toISOString() };
  pendingRequests.set(requestId, request);
  io.emit('new-delivery-request', request);
  res.json({ requestId, message: 'Pedido enviado' });
});

app.post('/update-location', (req, res) => {
  const { deliveryId, lat, lng } = req.body;
  if (!deliveryId || lat == null || lng == null) return res.status(400).json({ error: 'deliveryId, lat, lng obrigatórios' });
  const newPos = { lat: parseFloat(lat), lng: parseFloat(lng) };
  let d = deliveries.get(deliveryId);
  if (!d) { d = { lastPosition: newPos, history: [], status: 'active', createdAt: new Date().toISOString() }; deliveries.set(deliveryId, d); io.to(deliveryId).emit('location-update', { deliveryId, ...newPos, step: 1, total: 1, initial: true }); }
  else { emitInterpolated(deliveryId, interpolatePoints(d.lastPosition ?? newPos, newPos, 5), 280); d.lastPosition = newPos; d.status = 'active'; }
  d.history.push({ ...newPos, ts: Date.now() });
  if (d.history.length > 200) d.history.splice(0, 50);
  res.json({ ok: true, deliveryId, position: newPos });
});

app.get('/delivery/:id', (req, res) => {
  const d = deliveries.get(req.params.id.toUpperCase());
  if (!d) return res.status(404).json({ error: 'Não encontrada' });
  res.json({ lastPosition: d.lastPosition, status: d.status });
});

// ── Webhook Mercado Pago ──────────────────────────────────────────────────────
app.post('/webhook/mercadopago', async (req, res) => {
  res.sendStatus(200);
  const { type, data } = req.body;
  if (type !== 'payment' || !data?.id) return;
  const paymentId = String(data.id);
  const status    = await getPaymentStatus(paymentId);
  console.log(`💳 MP payment ${paymentId}: ${status}`);
  if (status !== 'approved') return;
  const orderId = paymentToOrder.get(paymentId);
  if (!orderId) return;
  const order = orders.get(orderId);
  if (!order || order.status !== 'awaiting_payment') return;
  order.status = 'paid';
  await createRequestFromOrder(orderId);
});

app.get('/health', (_req, res) => res.json({ ok: true, uptime: process.uptime(), whatsapp: waStatus }));
app.get('/debug',  (_req, res) => res.json({ waStatus, hasQR: !!currentQR, logs: waLogs }));
app.get('/track/:id', (_req, res) => res.sendFile(path.join(__dirname, 'public', 'track.html')));

// ── Socket.io ─────────────────────────────────────────────────────────────────
io.on('connection', (socket) => {
  socket.on('join-delivery', (rawId) => {
    const deliveryId = String(rawId).toUpperCase();
    socket.join(deliveryId);
    const d = deliveries.get(deliveryId);
    if (d?.lastPosition) socket.emit('location-update', { deliveryId, ...d.lastPosition, step: 1, total: 1, initial: true });
  });

  socket.on('accept-request', async (data) => {
    const requestId = String(data.requestId || '').toUpperCase();
    const request   = pendingRequests.get(requestId);
    if (!request || request.status !== 'pending') { socket.emit('request-unavailable', { requestId }); return; }
    request.status = 'accepted';
    const deliveryId  = uuidv4().slice(0, 8).toUpperCase();
    const trackingUrl = `${HOST_URL}/track/${deliveryId}`;
    deliveries.set(deliveryId, { lastPosition: null, history: [], status: 'active', phone: request.phone, createdAt: new Date().toISOString() });
    if (request.orderId) { const o = orders.get(request.orderId); if (o) { o.status = 'delivering'; o.deliveryId = deliveryId; } }
    socket.emit('delivery-assigned', { deliveryId, trackingUrl });
    socket.broadcast.emit('request-taken', { requestId });
    console.log(`✅ Corrida #${requestId} → Entrega #${deliveryId}`);
    if (request.phone) await sendWhatsApp(request.phone, `🛵 *Motoboy a caminho!*\n\nRastreie em tempo real:\n${trackingUrl}\n\n_Atualiza automaticamente._`);
  });

  socket.on('location-update-mobile', (data) => {
    const { deliveryId: rawId, lat, lng } = data;
    if (!rawId || lat == null || lng == null) return;
    const deliveryId = String(rawId).toUpperCase();
    const newPos     = { lat: parseFloat(lat), lng: parseFloat(lng) };
    const d          = deliveries.get(deliveryId);
    if (!d) return;
    emitInterpolated(deliveryId, interpolatePoints(d.lastPosition ?? newPos, newPos, 5), 280);
    d.lastPosition = newPos;
    d.history.push({ ...newPos, ts: Date.now() });
    if (d.history.length > 200) d.history.splice(0, 50);
  });
});

// ── Keep-alive ────────────────────────────────────────────────────────────────
if (process.env.NODE_ENV === 'production')
  setInterval(() => fetch(`${HOST_URL}/health`).catch(() => {}), 14 * 60 * 1000);

// ── Start ─────────────────────────────────────────────────────────────────────
const PORT = process.env.PORT || 3000;
server.listen(PORT, async () => {
  console.log(`\n🚚 Delivery Tracker — ${HOST_URL}`);
  console.log(`   QR Code:  ${HOST_URL}/qr`);
  console.log(`   Admin:    ${HOST_URL}/admin/establishment\n`);
  if (!MP_TOKEN) console.log('⚠️  MERCADOPAGO_ACCESS_TOKEN não configurado');
  await dbInit();
  connectWA();
});
