'use strict';

const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const cors = require('cors');
const { v4: uuidv4 } = require('uuid');
const path = require('path');

const app = express();
const server = http.createServer(app);
const io = new Server(server, { cors: { origin: '*', methods: ['GET', 'POST'] } });

app.use(cors());
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

// ── Config ───────────────────────────────────────────────────────────────────
const HOST_URL     = process.env.HOST_URL
  || (process.env.RAILWAY_PUBLIC_DOMAIN ? `https://${process.env.RAILWAY_PUBLIC_DOMAIN}` : null)
  || `http://localhost:${process.env.PORT || 3000}`;
const EVO_URL      = (process.env.EVOLUTION_API_URL || '').replace(/\/$/, '');
const EVO_KEY      = process.env.EVOLUTION_API_KEY  || '';
const EVO_INSTANCE = process.env.EVOLUTION_INSTANCE || '';
const MP_TOKEN     = process.env.MERCADOPAGO_ACCESS_TOKEN || '';
const ADMIN_KEY    = process.env.ADMIN_KEY || 'admin123';

// Tabela de frete por distância (km → R$)
const FREIGHT_TABLE = [
  { maxKm: 3,        price: 8  },
  { maxKm: 6,        price: 12 },
  { maxKm: 10,       price: 16 },
  { maxKm: 15,       price: 22 },
  { maxKm: Infinity, price: 30 },
];

// ── In-memory stores ─────────────────────────────────────────────────────────
const deliveries      = new Map(); // deliveryId → delivery
const pendingRequests = new Map(); // requestId  → request
const sessions        = new Map(); // phone      → session
const establishments  = new Map(); // phone      → establishment
const orders          = new Map(); // orderId    → order
const paymentToOrder  = new Map(); // paymentId  → orderId

// ── WhatsApp ─────────────────────────────────────────────────────────────────
async function sendWhatsApp(phone, text) {
  if (!EVO_URL || !EVO_KEY || !EVO_INSTANCE) {
    console.log(`[WA → ${phone}] ${text.slice(0, 80)}...`);
    return;
  }
  try {
    const res = await fetch(`${EVO_URL}/message/sendText/${EVO_INSTANCE}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', apikey: EVO_KEY },
      body: JSON.stringify({ number: phone, text }),
    });
    if (!res.ok) console.error('WA error:', await res.text());
  } catch (e) { console.error('WA error:', e.message); }
}

// ── Geocodificação (Nominatim) ────────────────────────────────────────────────
async function geocode(address) {
  try {
    const url = `https://nominatim.openstreetmap.org/search?q=${encodeURIComponent(address)}&format=json&limit=1&countrycodes=br`;
    const res  = await fetch(url, { headers: { 'User-Agent': 'DeliveryBot/1.0 (contato@seudominio.com)' } });
    const data = await res.json();
    if (!data.length) return null;
    return {
      lat:     parseFloat(data[0].lat),
      lng:     parseFloat(data[0].lon),
      display: data[0].display_name,
    };
  } catch { return null; }
}

// ── Distância real via OSRM ──────────────────────────────────────────────────
async function getRoadDistanceKm(from, to) {
  try {
    const url = `https://router.project-osrm.org/route/v1/driving/${from.lng},${from.lat};${to.lng},${to.lat}?overview=false`;
    const res  = await fetch(url);
    const data = await res.json();
    if (data.code !== 'Ok') return null;
    return data.routes[0].distance / 1000;
  } catch { return null; }
}

// ── Cálculo do frete ─────────────────────────────────────────────────────────
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
    return data.status; // 'approved', 'pending', 'rejected', etc.
  } catch { return null; }
}

// ── Criar corrida após pagamento ─────────────────────────────────────────────
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
// Estados: greeting | awaiting_delivery_address | awaiting_note | awaiting_confirm | awaiting_payment

async function handleBotMessage(phone, text) {
  const msg   = text.trim();
  const lower = msg.toLowerCase();

  const estab = establishments.get(phone);
  if (!estab) {
    await sendWhatsApp(phone,
      '❌ Número não cadastrado em nossa plataforma.\nPeça ao administrador para cadastrar seu estabelecimento.'
    );
    return;
  }

  let session = sessions.get(phone) || { state: 'greeting' };

  // Comando global: cancelar pedido em andamento
  if (/^cancelar$/i.test(msg) && session.state !== 'awaiting_payment') {
    sessions.delete(phone);
    await sendWhatsApp(phone, '❌ Atendimento cancelado. Quando quiser, é só mandar uma mensagem!');
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
        await sendWhatsApp(phone,
          '❌ Não consegui encontrar esse endereço. Tente ser mais específico (rua, número, cidade):'
        );
        return;
      }

      const distKm = await getRoadDistanceKm(
        { lat: estab.lat, lng: estab.lng },
        destCoords
      );
      if (!distKm) {
        await sendWhatsApp(phone,
          '❌ Erro ao calcular a rota. Tente informar o endereço novamente:'
        );
        return;
      }

      const freight = calcFreight(distKm);

      session.deliveryAddress = msg;
      session.deliveryDisplay = destCoords.display;
      session.deliveryCoords  = destCoords;
      session.distanceKm      = Math.round(distKm * 10) / 10;
      session.freightPrice    = freight;
      session.state           = 'awaiting_note';
      sessions.set(phone, session);

      await sendWhatsApp(phone,
        `📦 *Resumo da entrega:*\n\n🏪 Retirada: ${estab.address}\n📍 Entrega: ${msg}\n📏 Distância: ${session.distanceKm} km\n💰 Frete: *R$ ${freight.toFixed(2).replace('.', ',')}*\n\nTem alguma observação para o motoboy? (ou responda *não*)`
      );
      break;
    }

    case 'awaiting_note':
      session.note  = /^n[ãa]o$/i.test(msg) ? '' : msg;
      session.state = 'awaiting_confirm';
      sessions.set(phone, session);

      const noteText = session.note ? `\n📝 Obs: ${session.note}` : '';
      await sendWhatsApp(phone,
        `Confirme seu pedido:\n\n🏪 *Retirada:* ${estab.address}\n📍 *Entrega:* ${session.deliveryAddress}\n📏 *Distância:* ${session.distanceKm} km\n💰 *Frete:* R$ ${session.freightPrice.toFixed(2).replace('.', ',')}${noteText}\n\nResponda *SIM* para gerar o PIX ou *NÃO* para cancelar.`
      );
      break;

    case 'awaiting_confirm':
      if (/^sim$/i.test(msg)) {
        const orderId = uuidv4().slice(0, 8).toUpperCase();
        const order = {
          orderId,
          phone,
          establishmentName: estab.name,
          pickupAddress:  estab.address,
          pickupCoords:   { lat: estab.lat, lng: estab.lng },
          deliveryAddress: session.deliveryAddress,
          deliveryCoords:  session.deliveryCoords,
          note:           session.note || '',
          distanceKm:     session.distanceKm,
          freightPrice:   session.freightPrice,
          paymentId:      null,
          requestId:      null,
          status:         'awaiting_payment',
          createdAt:      new Date().toISOString(),
        };
        orders.set(orderId, order);
        sessions.delete(phone);

        await sendWhatsApp(phone, '⏳ Gerando PIX...');

        try {
          const { paymentId, copyPaste } = await createPixPayment(
            orderId,
            session.freightPrice,
            `Frete entrega #${orderId} — ${estab.name}`,
            phone
          );
          order.paymentId = paymentId;
          order.status    = 'awaiting_payment';
          paymentToOrder.set(paymentId, orderId);

          sessions.set(phone, { state: 'awaiting_payment', orderId });

          await sendWhatsApp(phone,
            `💳 *PIX gerado!* Valor: *R$ ${session.freightPrice.toFixed(2).replace('.', ',')}*\n\nCopie o código abaixo e cole no seu app de pagamento:\n\n\`\`\`${copyPaste}\`\`\`\n\n_Após o pagamento, a confirmação é automática._\n\nSe quiser cancelar, responda *CANCELAR*.`
          );
        } catch (e) {
          console.error('MP error:', e.message);
          orders.delete(orderId);
          await sendWhatsApp(phone,
            '❌ Erro ao gerar o PIX. Tente novamente em instantes.'
          );
        }
      } else {
        sessions.delete(phone);
        await sendWhatsApp(phone, '❌ Pedido cancelado. Quando precisar, é só chamar! 😊');
      }
      break;

    case 'awaiting_payment':
      if (/^cancelar$/i.test(msg)) {
        const { orderId } = session;
        const order = orders.get(orderId);
        if (order) order.status = 'cancelled';
        sessions.delete(phone);
        await sendWhatsApp(phone, '❌ Pedido cancelado. Quando precisar, é só chamar! 😊');
      } else {
        await sendWhatsApp(phone,
          '⏳ Aguardando confirmação do pagamento PIX...\n\nSe quiser cancelar, responda *CANCELAR*.'
        );
      }
      break;
  }
}

// ── Helpers de mapa / localização ────────────────────────────────────────────
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

// ── Routes — Admin ────────────────────────────────────────────────────────────
function requireAdmin(req, res, next) {
  if (req.headers['x-admin-key'] !== ADMIN_KEY) return res.status(401).json({ error: 'Unauthorized' });
  next();
}

// Cadastrar estabelecimento
app.post('/admin/establishment', requireAdmin, async (req, res) => {
  const { phone, name, address } = req.body;
  if (!phone || !name || !address)
    return res.status(400).json({ error: 'phone, name e address são obrigatórios' });

  const coords = await geocode(address);
  if (!coords)
    return res.status(400).json({ error: 'Endereço não encontrado. Tente ser mais específico.' });

  establishments.set(String(phone), { name, address, lat: coords.lat, lng: coords.lng });
  console.log(`\n🏪 Estabelecimento cadastrado: ${name} (${phone})`);
  res.json({ ok: true, name, address, coords: { lat: coords.lat, lng: coords.lng } });
});

// Listar estabelecimentos
app.get('/admin/establishments', requireAdmin, (_req, res) => {
  const list = [...establishments.entries()].map(([phone, e]) => ({ phone, ...e }));
  res.json(list);
});

// Listar pedidos
app.get('/admin/orders', requireAdmin, (_req, res) => {
  const list = [...orders.values()].sort((a, b) => b.createdAt.localeCompare(a.createdAt));
  res.json(list);
});

// Tabela de frete (leitura)
app.get('/admin/freight-table', requireAdmin, (_req, res) => res.json(FREIGHT_TABLE));

// ── Routes — Entrega ──────────────────────────────────────────────────────────
app.post('/request-delivery', (req, res) => {
  const { customerName = 'Cliente', address = '', phone = '', note = '' } = req.body;
  const requestId = uuidv4().slice(0, 8).toUpperCase();
  const request = { requestId, customerName, address, phone, note, status: 'pending', createdAt: new Date().toISOString() };
  pendingRequests.set(requestId, request);
  io.emit('new-delivery-request', request);
  console.log(`\n📱 Nova solicitação manual #${requestId} de "${customerName}"`);
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

// ── Webhook — Mercado Pago ────────────────────────────────────────────────────
app.post('/webhook/mercadopago', async (req, res) => {
  res.sendStatus(200);
  const { type, data } = req.body;
  if (type !== 'payment' || !data?.id) return;

  const paymentId = String(data.id);
  const status    = await getPaymentStatus(paymentId);
  console.log(`\n💳 MP webhook — payment ${paymentId}: ${status}`);

  if (status !== 'approved') return;

  const orderId = paymentToOrder.get(paymentId);
  if (!orderId) return;

  const order = orders.get(orderId);
  if (!order || order.status !== 'awaiting_payment') return;

  order.status = 'paid';
  await createRequestFromOrder(orderId);
});

// ── Webhook — Evolution API ───────────────────────────────────────────────────
app.post('/webhook/whatsapp', async (req, res) => {
  res.sendStatus(200);
  const body  = req.body;
  const event = body.event || body.type || '';

  const isMessage = event === 'messages.upsert' || event === 'message' || !!body.data?.message;
  if (!isMessage) return;

  const fromMe = body.data?.key?.fromMe || body.key?.fromMe || false;
  if (fromMe) return;

  const msg = (
    body.data?.message?.conversation ||
    body.data?.message?.extendedTextMessage?.text ||
    body.message?.conversation || ''
  ).trim();

  const phone = (body.data?.key?.remoteJid || body.key?.remoteJid || '').replace('@s.whatsapp.net', '');
  if (!msg || !phone) return;

  await handleBotMessage(phone, msg);
});

app.get('/health', (_req, res) => res.json({ ok: true, uptime: process.uptime() }));

// Keep-alive: impede o Render de hibernar o serviço no plano gratuito
if (process.env.NODE_ENV === 'production') {
  setInterval(() => {
    fetch(`${HOST_URL}/health`).catch(() => {});
  }, 14 * 60 * 1000); // ping a cada 14 minutos
}
app.get('/track/:deliveryId', (_req, res) =>
  res.sendFile(path.join(__dirname, 'public', 'track.html')));

// ── Socket.io ────────────────────────────────────────────────────────────────
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

    deliveries.set(deliveryId, {
      lastPosition: null, history: [], status: 'active',
      phone: request.phone, createdAt: new Date().toISOString(),
    });

    // Atualiza status do pedido
    if (request.orderId) {
      const order = orders.get(request.orderId);
      if (order) { order.status = 'delivering'; order.deliveryId = deliveryId; }
    }

    socket.emit('delivery-assigned', { deliveryId, trackingUrl });
    socket.broadcast.emit('request-taken', { requestId });
    console.log(`\n✅ Corrida #${requestId} aceita → Entrega #${deliveryId}`);

    // Envia link de rastreio pro cliente
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

// ── Start ─────────────────────────────────────────────────────────────────────
const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log('\n╔══════════════════════════════════════════════════════════╗');
  console.log('║   🚚  Delivery Tracker — servidor iniciado               ║');
  console.log('╠══════════════════════════════════════════════════════════╣');
  console.log(`║  URL:          ${HOST_URL.padEnd(41)}║`);
  console.log(`║  WhatsApp:     POST /webhook/whatsapp                    ║`);
  console.log(`║  Mercado Pago: POST /webhook/mercadopago                 ║`);
  console.log(`║  Admin:        POST /admin/establishment (x-admin-key)   ║`);
  console.log('╚══════════════════════════════════════════════════════════╝');
  if (!MP_TOKEN)  console.log('\n⚠️  MERCADOPAGO_ACCESS_TOKEN não configurado — PIX desativado');
  if (!EVO_URL)   console.log('⚠️  EVOLUTION_API_URL não configurado — WhatsApp desativado');
  console.log('');
});
