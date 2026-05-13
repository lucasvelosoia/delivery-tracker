'use strict';

const express  = require('express');
const http     = require('http');
const { Server } = require('socket.io');
const cors     = require('cors');
const { v4: uuidv4 } = require('uuid');
const path     = require('path');
const QRCode   = require('qrcode');
const { Pool } = require('pg');

const app    = express();
const server = http.createServer(app);
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
  if (!db) return;
  await db.query(`
    CREATE TABLE IF NOT EXISTS wa_session (
      key   TEXT PRIMARY KEY,
      value TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS establishments (
      phone   TEXT PRIMARY KEY,
      name    TEXT NOT NULL,
      address TEXT NOT NULL,
      lat     DOUBLE PRECISION NOT NULL,
      lng     DOUBLE PRECISION NOT NULL
    );
  `);
  // Carregar estabelecimentos do banco
  const rows = await db.query('SELECT * FROM establishments');
  for (const r of rows.rows) {
    establishments.set(r.phone, { name: r.name, address: r.address, lat: r.lat, lng: r.lng });
  }
  console.log(`\n✅ PostgreSQL conectado — ${rows.rows.length} estabelecimento(s) carregado(s)`);
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
let waStatus  = 'disconnected'; // disconnected | qr | open | error
const WA_SESSION_DIR = '/tmp/wa_session';

async function initWhatsApp() {
  try {
    const baileys = await import('@whiskeysockets/baileys');
    const makeWASocket          = baileys.default;
    const { DisconnectReason, useMultiFileAuthState } = baileys;

    const pino   = require('pino');
    const logger = pino({ level: 'silent' });
    const fs     = require('fs');

    function connect() {
      useMultiFileAuthState(WA_SESSION_DIR).then(({ state, saveCreds }) => {
        const sock = makeWASocket({
          auth: state,
          logger,
          printQRInTerminal: false,
          browser: ['MotoBot', 'Chrome', '120.0'],
        });

        sock.ev.on('creds.update', saveCreds);

        sock.ev.on('connection.update', ({ connection, lastDisconnect, qr }) => {
          if (qr) {
            currentQR = qr;
            waStatus  = 'qr';
            console.log('📱 QR disponível em /qr');
          }
          if (connection === 'open') {
            waSocket  = sock;
            waStatus  = 'open';
            currentQR = null;
            console.log('✅ WhatsApp conectado!');
          }
          if (connection === 'close') {
            waSocket = null;
            waStatus = 'disconnected';
            const code = lastDisconnect?.error?.output?.statusCode;
            const shouldReconnect = code !== DisconnectReason.loggedOut;
            console.log(`⚠️  WA desconectado (${code}) — ${shouldReconnect ? 'reconectando...' : 'sessão encerrada'}`);
            if (shouldReconnect) {
              setTimeout(connect, 5000);
            } else {
              fs.rmSync(WA_SESSION_DIR, { recursive: true, force: true });
              setTimeout(connect, 3000);
            }
          }
        });

        sock.ev.on('messages.upsert', async ({ messages, type }) => {
          if (type !== 'notify') return;
          for (const msg of messages) {
            if (msg.key.fromMe) continue;
            const phone = msg.key.remoteJid?.replace('@s.whatsapp.net', '');
            if (!phone) continue;
            const text = msg.message?.conversation
              || msg.message?.extendedTextMessage?.text
              || '';
            if (text) await handleBotMessage(phone, text);
          }
        });
      }).catch(e => {
        console.error('❌ WA connect error:', e.message);
        waStatus = 'error';
        setTimeout(connect, 10000);
      });
    }

    connect();
  } catch (e) {
    console.error('❌ WhatsApp init error:', e.message);
    waStatus = 'error';
  }
}

async function sendWhatsApp(phone, text) {
  if (!waSocket) {
    console.log(`[WA OFFLINE → ${phone}] ${text.slice(0, 60)}`);
    return;
  }
  try {
    await waSocket.sendMessage(`${phone}@s.whatsapp.net`, { text });
  } catch (e) {
    console.error('WA send error:', e.message);
  }
}

// ── Geocodificação (Nominatim) ────────────────────────────────────────────────
async function geocode(address) {
  try {
    const url = `https://nominatim.openstreetmap.org/search?q=${encodeURIComponent(address)}&format=json&limit=1&countrycodes=br`;
    const res  = await fetch(url, { headers: { 'User-Agent': 'DeliveryBot/1.0' } });
    const data = await res.json();
    if (!data.length) return null;
    return { lat: parseFloat(data[0].lat), lng: parseFloat(data[0].lon), display: data[0].display_name };
  } catch { return null; }
}

// ── Distância real (OSRM) ─────────────────────────────────────────────────────
async function getRoadDistanceKm(from, to) {
  try {
    const url = `https://router.project-osrm.org/route/v1/driving/${from.lng},${from.lat};${to.lng},${to.lat}?overview=false`;
    const res  = await fetch(url);
    const data = await res.json();
    if (data.code !== 'Ok') return null;
    return data.routes[0].distance / 1000;
  } catch { return null; }
}

function calcFreight(km) {
  return FREIGHT_TABLE.find(t => km <= t.maxKm).price;
}

// ── Mercado Pago PIX ─────────────────────────────────────────────────────────
async function createPixPayment(orderId, amount, description, phone) {
  if (!MP_TOKEN) throw new Error('MERCADOPAGO_ACCESS_TOKEN não configurado');
  const res = await fetch('https://api.mercadopago.com/v1/payments', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'Authorization': `Bearer ${MP_TOKEN}`,
      'X-Idempotency-Key': orderId,
    },
    body: JSON.stringify({
      transaction_amount: amount,
      description,
      payment_method_id: 'pix',
      payer: { email: `${phone}@entrega.bot` },
    }),
  });
  const data = await res.json();
  if (!data.id) throw new Error(JSON.stringify(data));
  return {
    paymentId: String(data.id),
    copyPaste: data.point_of_interaction?.transaction_data?.qr_code || '',
  };
}

async function getPaymentStatus(paymentId) {
  if (!MP_TOKEN) return null;
  try {
    const res  = await fetch(`https://api.mercadopago.com/v1/payments/${paymentId}`, {
      headers: { 'Authorization': `Bearer ${MP_TOKEN}` },
    });
    const data = await res.json();
    return data.status;
  } catch { return null; }
}

// ── Criar corrida após pagamento confirmado ───────────────────────────────────
async function createRequestFromOrder(orderId) {
  const order = orders.get(orderId);
  if (!order || order.requestId) return;

  const requestId = uuidv4().slice(0, 8).toUpperCase();
  const request = {
    requestId,
    customerName:  order.establishmentName,
    address:       order.deliveryAddress,
    pickupAddress: order.pickupAddress,
    phone:         order.phone,
    note:          order.note,
    distanceKm:    order.distanceKm,
    freightPrice:  order.freightPrice,
    orderId,
    status: 'pending',
    createdAt: new Date().toISOString(),
  };

  order.requestId = requestId;
  order.status    = 'pending';
  pendingRequests.set(requestId, request);
  io.emit('new-delivery-request', request);
  console.log(`\n🚀 Corrida #${requestId} criada para ${order.establishmentName}`);

  await sendWhatsApp(order.phone,
    `✅ *Pagamento confirmado!*\n\nSeu pedido *#${orderId}* está na fila.\n🛵 Estamos localizando o motoboy mais próximo...`
  );
}

// ── Bot — máquina de estados ─────────────────────────────────────────────────
async function handleBotMessage(phone, text) {
  const msg = text.trim();

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
      session = { state: 'awaiting_delivery_address' };
      sessions.set(phone, session);
      await sendWhatsApp(phone,
        `👋 Olá, *${estab.name}*!\n\n📍 *Retirada:* ${estab.address}\n\nPara qual endereço é a entrega?`
      );
      break;

    case 'awaiting_delivery_address': {
      await sendWhatsApp(phone, '⏳ Calculando distância e frete...');
      const destCoords = await geocode(msg);
      if (!destCoords) {
        await sendWhatsApp(phone, '❌ Endereço não encontrado. Tente ser mais específico (rua, número, cidade):');
        return;
      }
      const distKm = await getRoadDistanceKm({ lat: estab.lat, lng: estab.lng }, destCoords);
      if (!distKm) {
        await sendWhatsApp(phone, '❌ Não consegui calcular a rota. Tente novamente:');
        return;
      }
      const freight = calcFreight(distKm);
      session.deliveryAddress = msg;
      session.deliveryCoords  = destCoords;
      session.distanceKm      = Math.round(distKm * 10) / 10;
      session.freightPrice    = freight;
      session.state           = 'awaiting_note';
      sessions.set(phone, session);
      await sendWhatsApp(phone,
        `📦 *Resumo:*\n\n🏪 Retirada: ${estab.address}\n📍 Entrega: ${msg}\n📏 Distância: ${session.distanceKm} km\n💰 Frete: *R$ ${freight.toFixed(2).replace('.', ',')}*\n\nTem alguma observação para o motoboy? (ou responda *não*)`
      );
      break;
    }

    case 'awaiting_note':
      session.note  = /^n[ãa]o$/i.test(msg) ? '' : msg;
      session.state = 'awaiting_confirm';
      sessions.set(phone, session);
      await sendWhatsApp(phone,
        `Confirme seu pedido:\n\n🏪 *Retirada:* ${estab.address}\n📍 *Entrega:* ${session.deliveryAddress}\n📏 *Distância:* ${session.distanceKm} km\n💰 *Frete:* R$ ${session.freightPrice.toFixed(2).replace('.', ',')}${session.note ? `\n📝 *Obs:* ${session.note}` : ''}\n\nResponda *SIM* para gerar o PIX ou *NÃO* para cancelar.`
      );
      break;

    case 'awaiting_confirm':
      if (/^sim$/i.test(msg)) {
        const orderId = uuidv4().slice(0, 8).toUpperCase();
        const order = {
          orderId, phone,
          establishmentName: estab.name,
          pickupAddress: estab.address,
          pickupCoords:  { lat: estab.lat, lng: estab.lng },
          deliveryAddress: session.deliveryAddress,
          deliveryCoords:  session.deliveryCoords,
          note:            session.note || '',
          distanceKm:      session.distanceKm,
          freightPrice:    session.freightPrice,
          paymentId: null, requestId: null,
          status: 'awaiting_payment',
          createdAt: new Date().toISOString(),
        };
        orders.set(orderId, order);
        sessions.delete(phone);
        await sendWhatsApp(phone, '⏳ Gerando PIX...');
        try {
          const { paymentId, copyPaste } = await createPixPayment(
            orderId, session.freightPrice,
            `Frete entrega #${orderId} — ${estab.name}`, phone
          );
          order.paymentId = paymentId;
          paymentToOrder.set(paymentId, orderId);
          sessions.set(phone, { state: 'awaiting_payment', orderId });
          await sendWhatsApp(phone,
            `💳 *PIX gerado!*\n\nValor: *R$ ${session.freightPrice.toFixed(2).replace('.', ',')}*\n\nCopie o código abaixo:\n\n${copyPaste}\n\n_Confirmação automática após o pagamento._\n\nPara cancelar, responda *CANCELAR*.`
          );
        } catch (e) {
          console.error('MP error:', e.message);
          orders.delete(orderId);
          await sendWhatsApp(phone, '❌ Erro ao gerar o PIX. Tente novamente em instantes.');
        }
      } else {
        sessions.delete(phone);
        await sendWhatsApp(phone, '❌ Pedido cancelado. Quando precisar, é só chamar! 😊');
      }
      break;

    case 'awaiting_payment':
      await sendWhatsApp(phone, '⏳ Aguardando confirmação do pagamento PIX...\n\nPara cancelar, responda *CANCELAR*.');
      break;
  }
}

// ── Helpers ───────────────────────────────────────────────────────────────────
function interpolatePoints(from, to, steps = 5) {
  const pts = [];
  for (let i = 1; i <= steps; i++) {
    const t = i / steps;
    pts.push({ lat: from.lat + (to.lat - from.lat) * t, lng: from.lng + (to.lng - from.lng) * t });
  }
  return pts;
}

function emitInterpolated(deliveryId, points, stepMs = 280) {
  points.forEach((p, idx) => {
    setTimeout(() => {
      io.to(deliveryId).emit('location-update', { deliveryId, lat: p.lat, lng: p.lng, step: idx + 1, total: points.length });
    }, idx * stepMs);
  });
}

// ── Admin ─────────────────────────────────────────────────────────────────────
function requireAdmin(req, res, next) {
  if (req.headers['x-admin-key'] !== ADMIN_KEY) return res.status(401).json({ error: 'Unauthorized' });
  next();
}

app.post('/admin/establishment', requireAdmin, async (req, res) => {
  const { phone, name, address } = req.body;
  if (!phone || !name || !address)
    return res.status(400).json({ error: 'phone, name e address são obrigatórios' });

  const coords = await geocode(address);
  if (!coords) return res.status(400).json({ error: 'Endereço não encontrado.' });

  const estab = { name, address, lat: coords.lat, lng: coords.lng };
  establishments.set(String(phone), estab);

  if (db) {
    await db.query(
      'INSERT INTO establishments(phone,name,address,lat,lng) VALUES($1,$2,$3,$4,$5) ON CONFLICT(phone) DO UPDATE SET name=$2,address=$3,lat=$4,lng=$5',
      [String(phone), name, address, coords.lat, coords.lng]
    );
  }
  console.log(`\n🏪 Estabelecimento cadastrado: ${name} (${phone})`);
  res.json({ ok: true, name, address, coords: { lat: coords.lat, lng: coords.lng } });
});

app.get('/admin/establishments', requireAdmin, (_req, res) => {
  res.json([...establishments.entries()].map(([phone, e]) => ({ phone, ...e })));
});

app.get('/admin/orders', requireAdmin, (_req, res) => {
  res.json([...orders.values()].sort((a, b) => b.createdAt.localeCompare(a.createdAt)));
});

app.get('/admin/whatsapp', requireAdmin, (_req, res) => {
  res.json({ status: waStatus });
});

// ── QR Code ───────────────────────────────────────────────────────────────────
app.get('/qr', async (req, res) => {
  const html = (body, refresh = 5) =>
    `<html><head><meta http-equiv="refresh" content="${refresh}"><style>body{font-family:sans-serif;text-align:center;padding:40px;background:#f5f5f5}img{border-radius:12px;box-shadow:0 4px 20px rgba(0,0,0,.15)}</style></head><body>${body}</body></html>`;

  if (waStatus === 'open')
    return res.send(html('<h2 style="color:green">✅ WhatsApp conectado!</h2><p>O bot está ativo e recebendo mensagens.</p>', 30));

  if (waStatus === 'error')
    return res.send(html('<h2 style="color:red">❌ Erro ao iniciar WhatsApp</h2><p>Verifique os logs no painel do Render.</p><p>Tentando novamente automaticamente...</p>'));

  if (!currentQR)
    return res.send(html('<h2>⏳ Gerando QR Code...</h2><p>Aguarde alguns segundos. Esta página atualiza automaticamente.</p>'));

  const qrImage = await QRCode.toDataURL(currentQR);
  res.send(html(`<h2>📱 Escaneie com o WhatsApp</h2><img src="${qrImage}" style="max-width:280px"><br><br><p>Abra o WhatsApp → <b>Dispositivos conectados</b> → <b>Conectar dispositivo</b></p><p><small>QR expira em ~60s. A página atualiza automaticamente.</small></p>`, 20));
});

// ── Routes — Entrega ──────────────────────────────────────────────────────────
app.post('/request-delivery', (req, res) => {
  const { customerName = 'Cliente', address = '', phone = '', note = '' } = req.body;
  const requestId = uuidv4().slice(0, 8).toUpperCase();
  const request = { requestId, customerName, address, phone, note, status: 'pending', createdAt: new Date().toISOString() };
  pendingRequests.set(requestId, request);
  io.emit('new-delivery-request', request);
  res.json({ requestId, message: 'Pedido enviado para motoboys disponíveis' });
});

app.post('/update-location', (req, res) => {
  const { deliveryId, lat, lng } = req.body;
  if (!deliveryId || lat == null || lng == null)
    return res.status(400).json({ error: 'Campos obrigatórios: deliveryId, lat, lng' });
  const newPos = { lat: parseFloat(lat), lng: parseFloat(lng) };
  let delivery = deliveries.get(deliveryId);
  if (!delivery) {
    delivery = { lastPosition: newPos, history: [], status: 'active', createdAt: new Date().toISOString() };
    deliveries.set(deliveryId, delivery);
    io.to(deliveryId).emit('location-update', { deliveryId, ...newPos, step: 1, total: 1, initial: true });
  } else {
    emitInterpolated(deliveryId, interpolatePoints(delivery.lastPosition ?? newPos, newPos, 5), 280);
    delivery.lastPosition = newPos;
    delivery.status = 'active';
  }
  delivery.history.push({ ...newPos, ts: Date.now() });
  if (delivery.history.length > 200) delivery.history.splice(0, 50);
  res.json({ ok: true, deliveryId, position: newPos });
});

app.get('/delivery/:deliveryId', (req, res) => {
  const d = deliveries.get(req.params.deliveryId.toUpperCase());
  if (!d) return res.status(404).json({ error: 'Entrega não encontrada' });
  res.json({ lastPosition: d.lastPosition, status: d.status });
});

// ── Webhook Mercado Pago ──────────────────────────────────────────────────────
app.post('/webhook/mercadopago', async (req, res) => {
  res.sendStatus(200);
  const { type, data } = req.body;
  if (type !== 'payment' || !data?.id) return;
  const paymentId = String(data.id);
  const status    = await getPaymentStatus(paymentId);
  console.log(`\n💳 MP payment ${paymentId}: ${status}`);
  if (status !== 'approved') return;
  const orderId = paymentToOrder.get(paymentId);
  if (!orderId) return;
  const order = orders.get(orderId);
  if (!order || order.status !== 'awaiting_payment') return;
  order.status = 'paid';
  await createRequestFromOrder(orderId);
});

app.get('/health', (_req, res) => res.json({ ok: true, uptime: process.uptime(), whatsapp: waStatus }));

app.get('/track/:deliveryId', (_req, res) =>
  res.sendFile(path.join(__dirname, 'public', 'track.html')));

// ── Socket.io ─────────────────────────────────────────────────────────────────
io.on('connection', (socket) => {
  socket.on('join-delivery', (rawId) => {
    const deliveryId = String(rawId).toUpperCase();
    socket.join(deliveryId);
    const d = deliveries.get(deliveryId);
    if (d?.lastPosition)
      socket.emit('location-update', { deliveryId, ...d.lastPosition, step: 1, total: 1, initial: true });
  });

  socket.on('accept-request', async (data) => {
    const requestId = String(data.requestId || '').toUpperCase();
    const request   = pendingRequests.get(requestId);
    if (!request || request.status !== 'pending') {
      socket.emit('request-unavailable', { requestId }); return;
    }
    request.status = 'accepted';
    const deliveryId  = uuidv4().slice(0, 8).toUpperCase();
    const trackingUrl = `${HOST_URL}/track/${deliveryId}`;
    deliveries.set(deliveryId, { lastPosition: null, history: [], status: 'active', phone: request.phone, createdAt: new Date().toISOString() });
    if (request.orderId) {
      const order = orders.get(request.orderId);
      if (order) { order.status = 'delivering'; order.deliveryId = deliveryId; }
    }
    socket.emit('delivery-assigned', { deliveryId, trackingUrl });
    socket.broadcast.emit('request-taken', { requestId });
    console.log(`\n✅ Corrida #${requestId} aceita → Entrega #${deliveryId}`);
    if (request.phone) {
      await sendWhatsApp(request.phone,
        `🛵 *Motoboy a caminho!*\n\nAcompanhe sua entrega em tempo real:\n${trackingUrl}\n\n_A página atualiza automaticamente._`
      );
    }
  });

  socket.on('location-update-mobile', (data) => {
    const { deliveryId: rawId, lat, lng } = data;
    if (!rawId || lat == null || lng == null) return;
    const deliveryId = String(rawId).toUpperCase();
    const newPos     = { lat: parseFloat(lat), lng: parseFloat(lng) };
    const delivery   = deliveries.get(deliveryId);
    if (!delivery) return;
    emitInterpolated(deliveryId, interpolatePoints(delivery.lastPosition ?? newPos, newPos, 5), 280);
    delivery.lastPosition = newPos;
    delivery.history.push({ ...newPos, ts: Date.now() });
    if (delivery.history.length > 200) delivery.history.splice(0, 50);
  });
});

// ── Keep-alive (Render free tier) ─────────────────────────────────────────────
if (process.env.NODE_ENV === 'production') {
  setInterval(() => fetch(`${HOST_URL}/health`).catch(() => {}), 14 * 60 * 1000);
}

// ── Start ─────────────────────────────────────────────────────────────────────
const PORT = process.env.PORT || 3000;
server.listen(PORT, async () => {
  console.log('\n╔══════════════════════════════════════════════════════════╗');
  console.log('║   🚚  Delivery Tracker                                   ║');
  console.log(`║   ${HOST_URL.padEnd(54)}║`);
  console.log('╠══════════════════════════════════════════════════════════╣');
  console.log('║  QR Code:   /qr                                          ║');
  console.log('║  Admin:     /admin/establishment  (x-admin-key)          ║');
  console.log('║  MP Webhook: /webhook/mercadopago                        ║');
  console.log('╚══════════════════════════════════════════════════════════╝\n');
  if (!MP_TOKEN) console.log('⚠️  MERCADOPAGO_ACCESS_TOKEN não configurado');
  await dbInit();
  await initWhatsApp();
});
