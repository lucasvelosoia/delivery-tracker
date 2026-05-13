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
import makeWASocket, { DisconnectReason, useMultiFileAuthState, fetchLatestBaileysVersion, Browsers } from '@whiskeysockets/baileys';

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
const GROQ_KEY  = process.env.GROQ_API_KEY || '';

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


let waReconnecting = false;

async function connectWA() {
  if (waReconnecting) return;
  waReconnecting = true;
  try {
    waLog('🔄 Iniciando Baileys...');
    const { state, saveCreds } = await useMultiFileAuthState(WA_DIR);
    waLog('✅ Auth state carregado');

    const { version } = await fetchLatestBaileysVersion();
    waLog(`📦 WA version: ${version.join('.')}`);

    const sock = makeWASocket({
      version,
      auth: state,
      logger,
      printQRInTerminal: false,
      browser: Browsers.macOS('Desktop'),
      connectTimeoutMs: 60000,
      keepAliveIntervalMs: 15000,
      getMessage: async () => ({ conversation: '' }),
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
        waReconnecting = false;
        setTimeout(connectWA, 5000);
      }
    });

    sock.ev.on('messages.upsert', async ({ messages, type }) => {
      waLog(`📨 messages.upsert type=${type} count=${messages.length}`);
      for (const msg of messages) {
        const jid    = msg.key.remoteJid || '';
        const fromMe = msg.key.fromMe;
        const phone  = jid.replace('@s.whatsapp.net', '').replace('@g.us', '');
        const text   = msg.message?.conversation
          || msg.message?.extendedTextMessage?.text
          || msg.message?.imageMessage?.caption
          || '';
        waLog(`  jid=${jid} fromMe=${fromMe} type=${type} text="${text.slice(0,30)}"`);
        if (fromMe) continue;
        if (jid.endsWith('@g.us')) continue; // ignora grupos
        if (type !== 'notify') continue;
        if (phone && text) await handleBotMessage(phone, text);
      }
    });
  } catch (e) {
    waLog(`❌ connectWA erro: ${e.message}`);
    waStatus = 'error';
    waReconnecting = false;
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

// ── IA (Groq / Llama 3.3) ────────────────────────────────────────────────────
const SYSTEM_PROMPT = `Você é um assistente de entregas por motoboy no Brasil. Seu trabalho é atender clientes via WhatsApp, coletar as informações necessárias e organizar a entrega.

Tabela de frete:
- Até 3km: R$8,00
- 3 a 6km: R$12,00
- 6 a 10km: R$16,00
- 10 a 15km: R$22,00
- Acima de 15km: R$30,00

Você precisa coletar OBRIGATORIAMENTE:
1. Nome do cliente
2. Endereço de RETIRADA (de onde o motoboy vai buscar)
3. Endereço de ENTREGA (para onde vai entregar)
4. Observações (opcional)

Regras:
- Seja simpático, rápido e direto. Use emojis com moderação.
- Quando tiver nome + endereço de retirada + endereço de entrega, use action "calculate_freight".
- Quando o cliente confirmar o pedido (sim, confirmo, pode ser, ok, etc.), use action "confirm_order".
- Quando o cliente cancelar, use action "cancel".
- Se o cliente perguntar preço antes de dar os endereços, explique a tabela e peça os endereços.
- Enquanto estiver coletando dados, use action "none".
- Se o pagamento já foi gerado e o cliente mandar qualquer coisa, use action "awaiting_payment".
- Responda SEMPRE em português brasileiro informal.

Responda SOMENTE com JSON válido neste formato:
{
  "message": "mensagem para o cliente",
  "action": "none|calculate_freight|confirm_order|cancel|awaiting_payment",
  "data": {
    "name": "nome extraído ou null",
    "pickup_address": "endereço de retirada extraído ou null",
    "delivery_address": "endereço de entrega extraído ou null",
    "note": "observação ou null"
  }
}`;

async function callGroq(messages) {
  const res = await fetch('https://api.groq.com/openai/v1/chat/completions', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${GROQ_KEY}` },
    body: JSON.stringify({
      model: 'llama-3.3-70b-versatile',
      messages: [{ role: 'system', content: SYSTEM_PROMPT }, ...messages],
      response_format: { type: 'json_object' },
      temperature: 0.3,
      max_tokens: 600,
    }),
  });
  const json = await res.json();
  if (!res.ok) throw new Error(json.error?.message || 'Groq error');
  return JSON.parse(json.choices[0].message.content);
}

// ── Bot com IA ────────────────────────────────────────────────────────────────
async function handleBotMessage(phone, text) {
  const msg = text.trim();

  // Sessão: { history: [{role,content}], data: {name,pickupAddress,pickupCoords,deliveryAddress,deliveryCoords,note,distanceKm,freightPrice}, state }
  let session = sessions.get(phone) || { history: [], data: {}, state: 'chatting' };

  // Pedido aguardando pagamento — não passa pela IA
  if (session.state === 'awaiting_payment') {
    if (/^cancelar$/i.test(msg)) {
      const order = orders.get(session.orderId);
      if (order) order.status = 'cancelled';
      sessions.delete(phone);
      await sendWhatsApp(phone, '❌ Pedido cancelado. Quando precisar é só chamar! 😊');
    } else {
      await sendWhatsApp(phone, '⏳ Aguardando confirmação do pagamento PIX...\n\nPara cancelar responda *CANCELAR*.');
    }
    return;
  }

  // Adiciona mensagem do usuário ao histórico
  session.history.push({ role: 'user', content: msg });
  if (session.history.length > 20) session.history = session.history.slice(-20);

  let aiReply;
  try {
    aiReply = await callGroq(session.history);
  } catch (e) {
    console.error('Groq error:', e.message);
    await sendWhatsApp(phone, '⚠️ Erro temporário. Tente novamente em instantes.');
    return;
  }

  // Merge dados extraídos pela IA
  const d = aiReply.data || {};
  if (d.name)             session.data.name            = d.name;
  if (d.pickup_address)   session.data.pickupAddress   = d.pickup_address;
  if (d.delivery_address) session.data.deliveryAddress = d.delivery_address;
  if (d.note)             session.data.note            = d.note;

  // Adiciona resposta da IA ao histórico
  session.history.push({ role: 'assistant', content: aiReply.message });
  sessions.set(phone, session);

  switch (aiReply.action) {

    case 'calculate_freight': {
      await sendWhatsApp(phone, aiReply.message);
      await sendWhatsApp(phone, '⏳ Calculando distância e frete...');

      const pickup = await geocode(session.data.pickupAddress);
      const dest   = await geocode(session.data.deliveryAddress);

      if (!pickup || !dest) {
        const errMsg = '❌ Não consegui encontrar um dos endereços. Pode confirmar com rua, número e cidade?';
        session.history.push({ role: 'assistant', content: errMsg });
        sessions.set(phone, session);
        await sendWhatsApp(phone, errMsg);
        return;
      }

      const km = await getRoadDistanceKm(pickup, dest);
      if (!km) {
        const errMsg = '❌ Não consegui calcular a rota. Tente informar os endereços novamente.';
        session.history.push({ role: 'assistant', content: errMsg });
        sessions.set(phone, session);
        await sendWhatsApp(phone, errMsg);
        return;
      }

      session.data.pickupCoords   = pickup;
      session.data.deliveryCoords = dest;
      session.data.distanceKm     = Math.round(km * 10) / 10;
      session.data.freightPrice   = calcFreight(km);

      const summary = `📦 *Resumo da entrega:*\n\n👤 *Cliente:* ${session.data.name}\n🏪 *Retirada:* ${session.data.pickupAddress}\n📍 *Entrega:* ${session.data.deliveryAddress}\n📏 *Distância:* ${session.data.distanceKm} km\n💰 *Frete:* R$ ${session.data.freightPrice.toFixed(2).replace('.', ',')}${session.data.note ? `\n📝 *Obs:* ${session.data.note}` : ''}\n\nConfirma o pedido? Responda *SIM* para gerar o PIX.`;

      session.history.push({ role: 'assistant', content: summary });
      sessions.set(phone, session);
      await sendWhatsApp(phone, summary);
      break;
    }

    case 'confirm_order': {
      await sendWhatsApp(phone, aiReply.message);

      if (!session.data.freightPrice) {
        await sendWhatsApp(phone, '⚠️ Ainda não calculamos o frete. Me informe os endereços de retirada e entrega.');
        return;
      }

      const orderId = uuidv4().slice(0, 8).toUpperCase();
      const order = {
        orderId, phone,
        establishmentName: session.data.name || 'Cliente',
        pickupAddress:     session.data.pickupAddress,
        pickupCoords:      session.data.pickupCoords,
        deliveryAddress:   session.data.deliveryAddress,
        deliveryCoords:    session.data.deliveryCoords,
        note:              session.data.note || '',
        distanceKm:        session.data.distanceKm,
        freightPrice:      session.data.freightPrice,
        paymentId: null, requestId: null,
        status: 'awaiting_payment',
        createdAt: new Date().toISOString(),
      };
      orders.set(orderId, order);
      sessions.set(phone, { ...session, state: 'awaiting_payment', orderId });

      await sendWhatsApp(phone, '⏳ Gerando PIX...');
      try {
        const { paymentId, copyPaste } = await createPixPayment(orderId, session.data.freightPrice, `Frete #${orderId}`, phone);
        order.paymentId = paymentId;
        paymentToOrder.set(paymentId, orderId);
        await sendWhatsApp(phone,
          `💳 *PIX gerado!*\n\nValor: *R$ ${session.data.freightPrice.toFixed(2).replace('.', ',')}*\n\nCopie o código abaixo:\n\n${copyPaste}\n\n_Confirmação automática após o pagamento._\n\nPara cancelar responda *CANCELAR*.`
        );
      } catch (e) {
        console.error('MP error:', e.message);
        orders.delete(orderId);
        sessions.set(phone, { ...session, state: 'chatting' });
        await sendWhatsApp(phone, '❌ Erro ao gerar o PIX. Tente novamente.');
      }
      break;
    }

    case 'cancel':
      sessions.delete(phone);
      await sendWhatsApp(phone, aiReply.message);
      break;

    default:
      await sendWhatsApp(phone, aiReply.message);
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
app.post('/admin/reset-wa', requireAdmin, (_req, res) => {
  waLog('🔄 Reset manual da sessão WA...');
  waReconnecting = false;
  if (waSocket) { try { waSocket.end(undefined); } catch(_) {} waSocket = null; }
  rmSync(WA_DIR, { recursive: true, force: true });
  setTimeout(connectWA, 1000);
  res.json({ ok: true, message: 'Sessão resetada. Acesse /qr para escanear.' });
});
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
