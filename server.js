'use strict';

const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const cors = require('cors');
const { v4: uuidv4 } = require('uuid');
const path = require('path');

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
  cors: { origin: '*', methods: ['GET', 'POST'] }
});

app.use(cors());
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

// ── Config ───────────────────────────────────────────────────────────────────
const HOST_URL = process.env.HOST_URL
  || (process.env.RAILWAY_PUBLIC_DOMAIN ? `https://${process.env.RAILWAY_PUBLIC_DOMAIN}` : null)
  || `http://localhost:${process.env.PORT || 3000}`;

const EVO_URL      = (process.env.EVOLUTION_API_URL || '').replace(/\/$/, '');
const EVO_KEY      = process.env.EVOLUTION_API_KEY  || '';
const EVO_INSTANCE = process.env.EVOLUTION_INSTANCE || '';

// ── In-memory store ──────────────────────────────────────────────────────────
// deliveryId → { lastPosition, history, status, phone, createdAt }
const deliveries = new Map();
// requestId  → { customerName, address, phone, note, status, createdAt }
const pendingRequests = new Map();
// phone → { state, name, address, note }
const sessions = new Map();

// ── WhatsApp sender ──────────────────────────────────────────────────────────
async function sendWhatsApp(phone, text) {
  if (!EVO_URL || !EVO_KEY || !EVO_INSTANCE) return;
  try {
    const res = await fetch(`${EVO_URL}/message/sendText/${EVO_INSTANCE}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', apikey: EVO_KEY },
      body: JSON.stringify({ number: phone, text }),
    });
    if (!res.ok) console.error('WhatsApp send error:', await res.text());
  } catch (e) {
    console.error('WhatsApp send error:', e.message);
  }
}

// ── Bot conversation ─────────────────────────────────────────────────────────
// States: idle → awaiting_name → awaiting_address → awaiting_note → awaiting_confirm
async function handleBotMessage(phone, text) {
  const msg = text.trim();
  let session = sessions.get(phone) || { state: 'idle' };

  switch (session.state) {
    case 'idle':
    default:
      session = { state: 'awaiting_name' };
      sessions.set(phone, session);
      await sendWhatsApp(phone,
        '👋 Olá! Seja bem-vindo ao nosso serviço de entregas.\n\nPrimeiro, qual é o seu *nome*?'
      );
      break;

    case 'awaiting_name':
      session.name = msg;
      session.state = 'awaiting_address';
      sessions.set(phone, session);
      await sendWhatsApp(phone,
        `Obrigado, *${msg}*! 😊\n\nAgora me informe o *endereço completo* de entrega:`
      );
      break;

    case 'awaiting_address':
      session.address = msg;
      session.state = 'awaiting_note';
      sessions.set(phone, session);
      await sendWhatsApp(phone,
        '📝 Tem alguma *observação*? (ex: apartamento, complemento, ponto de referência)\n\nSe não tiver, responda *não*.'
      );
      break;

    case 'awaiting_note':
      session.note = /^n[ãa]o$/i.test(msg.trim()) ? '' : msg;
      session.state = 'awaiting_confirm';
      sessions.set(phone, session);

      const noteLine = session.note ? `\n📝 *Obs:* ${session.note}` : '';
      await sendWhatsApp(phone,
        `Confira seu pedido:\n\n👤 *Nome:* ${session.name}\n📍 *Endereço:* ${session.address}${noteLine}\n\nEstá correto? Responda *SIM* para confirmar ou *NÃO* para recomeçar.`
      );
      break;

    case 'awaiting_confirm':
      if (/^sim$/i.test(msg.trim())) {
        const requestId = uuidv4().slice(0, 8).toUpperCase();
        const request = {
          requestId,
          customerName: session.name,
          address: session.address,
          phone,
          note: session.note,
          status: 'pending',
          createdAt: new Date().toISOString(),
        };
        pendingRequests.set(requestId, request);
        io.emit('new-delivery-request', request);
        sessions.delete(phone);
        console.log(`\n📲 Pedido WhatsApp #${requestId} de ${session.name} (${phone})`);
        await sendWhatsApp(phone,
          '✅ *Pedido confirmado!*\n\nEstamos localizando um motoboy disponível. Assim que ele aceitar sua corrida, você receberá o link de rastreio em tempo real. 🛵'
        );
      } else {
        sessions.delete(phone);
        await sendWhatsApp(phone,
          '❌ Pedido cancelado. Quando quiser solicitar uma entrega, é só me mandar uma mensagem! 😊'
        );
      }
      break;
  }
}

// ── Helpers ──────────────────────────────────────────────────────────────────
function interpolatePoints(from, to, steps = 5) {
  const points = [];
  for (let i = 1; i <= steps; i++) {
    const t = i / steps;
    points.push({
      lat: from.lat + (to.lat - from.lat) * t,
      lng: from.lng + (to.lng - from.lng) * t,
    });
  }
  return points;
}

function emitInterpolated(deliveryId, points, stepDelayMs = 280) {
  points.forEach((point, idx) => {
    setTimeout(() => {
      io.to(deliveryId).emit('location-update', {
        deliveryId,
        lat: point.lat,
        lng: point.lng,
        step: idx + 1,
        total: points.length,
      });
    }, idx * stepDelayMs);
  });
}

// ── Routes ───────────────────────────────────────────────────────────────────

app.post('/create-delivery', (req, res) => {
  const deliveryId = uuidv4().slice(0, 8).toUpperCase();
  deliveries.set(deliveryId, {
    lastPosition: null,
    history: [],
    status: 'waiting',
    createdAt: new Date().toISOString(),
  });
  const base = `${req.protocol}://${req.get('host')}`;
  const trackingUrl = `${base}/track/${deliveryId}`;
  res.json({
    deliveryId,
    trackingUrl,
    whatsappText: `🛵 *Sua entrega está a caminho!*\n\nAcompanhe em tempo real:\n${trackingUrl}\n\n_Atualizado automaticamente._`,
  });
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
    const lastPos = delivery.lastPosition ?? newPos;
    emitInterpolated(deliveryId, interpolatePoints(lastPos, newPos, 5), 280);
    delivery.lastPosition = newPos;
    delivery.status = 'active';
  }

  delivery.history.push({ ...newPos, ts: Date.now() });
  if (delivery.history.length > 200) delivery.history.splice(0, 50);

  res.json({ ok: true, deliveryId, position: newPos, trackingUrl: `${req.protocol}://${req.get('host')}/track/${deliveryId}` });
});

app.get('/delivery/:deliveryId', (req, res) => {
  const d = deliveries.get(req.params.deliveryId.toUpperCase());
  if (!d) return res.status(404).json({ error: 'Entrega não encontrada' });
  res.json({ lastPosition: d.lastPosition, status: d.status, historyLength: d.history.length });
});

// Criação manual de corrida (sem WhatsApp)
app.post('/request-delivery', (req, res) => {
  const { customerName = 'Cliente', address = '', phone = '', note = '' } = req.body;
  const requestId = uuidv4().slice(0, 8).toUpperCase();
  const request = { requestId, customerName, address, phone, note, status: 'pending', createdAt: new Date().toISOString() };
  pendingRequests.set(requestId, request);
  io.emit('new-delivery-request', request);
  console.log(`\n📱 Nova solicitação #${requestId} de "${customerName}"`);
  res.json({ requestId, message: 'Pedido enviado para motoboys disponíveis' });
});

// Webhook Evolution API
app.post('/webhook/whatsapp', async (req, res) => {
  res.sendStatus(200); // responde rápido pro Evolution não reenviar

  const body = req.body;
  const event = body.event || body.type || '';

  const isMessage = event === 'messages.upsert' || event === 'message' || !!body.data?.message;
  if (!isMessage) return;

  // Ignora mensagens enviadas pelo próprio bot
  const fromMe = body.data?.key?.fromMe || body.key?.fromMe || false;
  if (fromMe) return;

  const msg = (
    body.data?.message?.conversation ||
    body.data?.message?.extendedTextMessage?.text ||
    body.message?.conversation ||
    ''
  ).trim();

  const phone = (body.data?.key?.remoteJid || body.key?.remoteJid || '').replace('@s.whatsapp.net', '');

  if (!msg || !phone) return;

  await handleBotMessage(phone, msg);
});

app.get('/health', (_req, res) => res.json({ ok: true, uptime: process.uptime() }));

app.get('/track/:deliveryId', (_req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'track.html'));
});

// ── Socket.io ────────────────────────────────────────────────────────────────

io.on('connection', (socket) => {
  socket.on('join-delivery', (rawId) => {
    const deliveryId = String(rawId).toUpperCase();
    socket.join(deliveryId);
    const d = deliveries.get(deliveryId);
    if (d?.lastPosition) {
      socket.emit('location-update', { deliveryId, ...d.lastPosition, step: 1, total: 1, initial: true });
    }
  });

  socket.on('accept-request', async (data) => {
    const requestId = String(data.requestId || '').toUpperCase();
    const request   = pendingRequests.get(requestId);

    if (!request || request.status !== 'pending') {
      socket.emit('request-unavailable', { requestId });
      return;
    }

    request.status = 'accepted';

    const deliveryId = uuidv4().slice(0, 8).toUpperCase();
    const trackingUrl = `${HOST_URL}/track/${deliveryId}`;

    deliveries.set(deliveryId, {
      lastPosition: null,
      history: [],
      status: 'active',
      phone: request.phone,
      createdAt: new Date().toISOString(),
    });

    socket.emit('delivery-assigned', { deliveryId, trackingUrl });
    socket.broadcast.emit('request-taken', { requestId });
    console.log(`\n✅ Corrida #${requestId} aceita → Entrega #${deliveryId}`);

    // Avisa o cliente no WhatsApp com o link de rastreio
    if (request.phone) {
      await sendWhatsApp(request.phone,
        `🛵 *Seu motoboy está a caminho!*\n\nAcompanhe em tempo real:\n${trackingUrl}\n\n_A página atualiza automaticamente, sem precisar dar refresh._`
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

    const lastPos = delivery.lastPosition ?? newPos;
    emitInterpolated(deliveryId, interpolatePoints(lastPos, newPos, 5), 280);

    delivery.lastPosition = newPos;
    delivery.history.push({ ...newPos, ts: Date.now() });
    if (delivery.history.length > 200) delivery.history.splice(0, 50);
  });
});

// ── Start ─────────────────────────────────────────────────────────────────────
const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log('\n╔══════════════════════════════════════════════════╗');
  console.log('║   🚚  Delivery Tracker — servidor iniciado        ║');
  console.log('╠══════════════════════════════════════════════════╣');
  console.log(`║  URL:          ${HOST_URL.padEnd(33)}║`);
  console.log(`║  Webhook:      POST /webhook/whatsapp            ║`);
  console.log(`║  Rastreio:     GET  /track/{deliveryId}          ║`);
  console.log('╚══════════════════════════════════════════════════╝');
  if (!EVO_URL) console.log('\n⚠️  EVOLUTION_API_URL não configurado — respostas WhatsApp desativadas\n');
  else console.log(`\n✅ WhatsApp: ${EVO_URL} (instância: ${EVO_INSTANCE})\n`);
});
