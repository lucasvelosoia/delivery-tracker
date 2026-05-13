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

// ── In-memory store ──────────────────────────────────────────────────────────
// deliveryId → { lastPosition, history, status, createdAt }
const deliveries = new Map();
// requestId → { customerName, address, phone, note, status, createdAt }
const pendingRequests = new Map();

const HOST_URL = process.env.HOST_URL
  || (process.env.RAILWAY_PUBLIC_DOMAIN ? `https://${process.env.RAILWAY_PUBLIC_DOMAIN}` : null)
  || `http://localhost:${process.env.PORT || 3000}`;

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

// Cria uma nova entrega e retorna o link de rastreio
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
    whatsappText: `🛵 *Sua entrega está a caminho!*\n\nAcompanhe em tempo real pelo link abaixo:\n${trackingUrl}\n\n_Atualizado automaticamente — sem precisar dar refresh._`,
  });
});

// Recebe a posição do app do motorista
app.post('/update-location', (req, res) => {
  const { deliveryId, lat, lng } = req.body;

  if (!deliveryId || lat == null || lng == null) {
    return res.status(400).json({ error: 'Campos obrigatórios: deliveryId, lat, lng' });
  }

  const newPos = { lat: parseFloat(lat), lng: parseFloat(lng) };

  let delivery = deliveries.get(deliveryId);

  if (!delivery) {
    delivery = {
      lastPosition: newPos,
      history: [],
      status: 'active',
      createdAt: new Date().toISOString(),
    };
    deliveries.set(deliveryId, delivery);

    // Primeira posição — emite direto sem interpolação
    io.to(deliveryId).emit('location-update', {
      deliveryId,
      ...newPos,
      step: 1,
      total: 1,
      initial: true,
    });
  } else {
    const lastPos = delivery.lastPosition ?? newPos;
    const points = interpolatePoints(lastPos, newPos, 5);
    emitInterpolated(deliveryId, points, 280);
    delivery.lastPosition = newPos;
    delivery.status = 'active';
  }

  delivery.history.push({ ...newPos, ts: Date.now() });
  if (delivery.history.length > 200) delivery.history.splice(0, 50);

  const trackingUrl = `${req.protocol}://${req.get('host')}/track/${deliveryId}`;
  res.json({ ok: true, deliveryId, position: newPos, trackingUrl });
});

// Retorna estado atual de uma entrega (útil para reconexão)
app.get('/delivery/:deliveryId', (req, res) => {
  const d = deliveries.get(req.params.deliveryId.toUpperCase());
  if (!d) return res.status(404).json({ error: 'Entrega não encontrada' });
  res.json({ lastPosition: d.lastPosition, status: d.status, historyLength: d.history.length });
});

// Cliente solicita uma corrida (chamado pelo webhook do WhatsApp ou página web)
app.post('/request-delivery', (req, res) => {
  const { customerName = 'Cliente', address = '', phone = '', note = '' } = req.body;
  const requestId = uuidv4().slice(0, 8).toUpperCase();

  const request = {
    requestId,
    customerName,
    address,
    phone,
    note,
    status: 'pending',
    createdAt: new Date().toISOString(),
  };

  pendingRequests.set(requestId, request);
  io.emit('new-delivery-request', request);

  console.log(`\n📱 Nova solicitação #${requestId} de "${customerName}"`);
  res.json({ requestId, message: 'Pedido enviado para motoboys disponíveis' });
});

// Webhook WhatsApp (Evolution API / WPPConnect / Baileys)
app.post('/webhook/whatsapp', (req, res) => {
  const body = req.body;
  const event = body.event || body.type || '';

  if (event === 'messages.upsert' || event === 'message' || body.data?.message) {
    const msg = body.data?.message?.conversation
              || body.data?.message?.extendedTextMessage?.text
              || body.message?.conversation
              || '';
    const phone = (body.data?.key?.remoteJid || body.key?.remoteJid || '').replace('@s.whatsapp.net', '');
    const name  = body.data?.pushName || body.pushName || 'Cliente WhatsApp';

    const lower = msg.toLowerCase();
    const isRequest = lower.includes('entrega') || lower.includes('pedido')
                   || lower.includes('quero') || lower.includes('solicito')
                   || lower.includes('motoboy');

    if (isRequest && msg.length > 0) {
      const requestId = uuidv4().slice(0, 8).toUpperCase();
      const request = {
        requestId,
        customerName: name,
        address: msg,
        phone,
        note: `Via WhatsApp`,
        status: 'pending',
        createdAt: new Date().toISOString(),
      };
      pendingRequests.set(requestId, request);
      io.emit('new-delivery-request', request);
      console.log(`\n📲 Pedido via WhatsApp #${requestId} de ${name} (${phone})`);
    }
  }
  res.sendStatus(200);
});

// Health check para Render / Railway
app.get('/health', (_req, res) => res.json({ ok: true, uptime: process.uptime() }));

// Servir a SPA de rastreio para qualquer /track/:id
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
      socket.emit('location-update', {
        deliveryId,
        ...d.lastPosition,
        step: 1,
        total: 1,
        initial: true,
      });
    }
  });

  // Motoboy aceita a corrida
  socket.on('accept-request', (data) => {
    const requestId = String(data.requestId || '').toUpperCase();
    const request   = pendingRequests.get(requestId);

    if (!request || request.status !== 'pending') {
      socket.emit('request-unavailable', { requestId });
      return;
    }

    request.status = 'accepted';

    const deliveryId = uuidv4().slice(0, 8).toUpperCase();
    deliveries.set(deliveryId, {
      lastPosition: null,
      history: [],
      status: 'active',
      createdAt: new Date().toISOString(),
    });

    socket.emit('delivery-assigned', {
      deliveryId,
      trackingUrl: `${HOST_URL}/track/${deliveryId}`,
    });

    // Avisa outros motoboys que a corrida foi pega
    socket.broadcast.emit('request-taken', { requestId });
    console.log(`\n✅ Corrida #${requestId} aceita → Entrega #${deliveryId}`);
  });

  // GPS do app Android via socket
  socket.on('location-update-mobile', (data) => {
    const { deliveryId: rawId, lat, lng } = data;
    if (!rawId || lat == null || lng == null) return;

    const deliveryId = String(rawId).toUpperCase();
    const newPos     = { lat: parseFloat(lat), lng: parseFloat(lng) };
    const delivery   = deliveries.get(deliveryId);
    if (!delivery) return;

    const lastPos = delivery.lastPosition ?? newPos;
    const points  = interpolatePoints(lastPos, newPos, 5);
    emitInterpolated(deliveryId, points, 280);

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
  console.log(`║  App:          http://localhost:${PORT}              ║`);
  console.log(`║  Nova entrega: POST /create-delivery             ║`);
  console.log(`║  Atualizar:    POST /update-location             ║`);
  console.log(`║  Rastreio:     GET  /track/{deliveryId}          ║`);
  console.log('╚══════════════════════════════════════════════════╝\n');
  console.log('Para simular uma entrega, em outro terminal rode:');
  console.log('  node simulate.js\n');
});
