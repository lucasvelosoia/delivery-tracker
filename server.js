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
import makeWASocket, { DisconnectReason, useMultiFileAuthState, fetchLatestBaileysVersion, Browsers, BufferJSON, initAuthCreds } from '@whiskeysockets/baileys';

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
    CREATE TABLE IF NOT EXISTS clients (
      phone          TEXT PRIMARY KEY,
      name           TEXT NOT NULL,
      pickup_address TEXT NOT NULL,
      pickup_lat     DOUBLE PRECISION NOT NULL,
      pickup_lng     DOUBLE PRECISION NOT NULL
    );
    CREATE TABLE IF NOT EXISTS pedidos (
      order_id         TEXT PRIMARY KEY,
      phone            TEXT,
      customer_name    TEXT,
      pickup_address   TEXT,
      delivery_address TEXT,
      distance_km      REAL,
      freight_price    REAL,
      status           TEXT DEFAULT 'awaiting_payment',
      payment_id       TEXT,
      request_id       TEXT,
      note             TEXT,
      created_at       TIMESTAMPTZ DEFAULT NOW()
    );
  `);
  const rows = await db.query('SELECT * FROM establishments');
  for (const r of rows.rows)
    establishments.set(r.phone, { name: r.name, address: r.address, lat: r.lat, lng: r.lng });
  const clientRows = await db.query('SELECT * FROM clients');
  for (const r of clientRows.rows)
    clients.set(r.phone, { name: r.name, pickupAddress: r.pickup_address, pickupCoords: { lat: r.pickup_lat, lng: r.pickup_lng } });
  const orderRows = await db.query('SELECT * FROM pedidos ORDER BY created_at DESC LIMIT 500');
  for (const r of orderRows.rows)
    orders.set(r.order_id, { orderId: r.order_id, phone: r.phone, establishmentName: r.customer_name, pickupAddress: r.pickup_address, deliveryAddress: r.delivery_address, distanceKm: r.distance_km, freightPrice: r.freight_price, status: r.status, paymentId: r.payment_id, requestId: r.request_id, note: r.note, createdAt: r.created_at?.toISOString?.() || r.created_at });
  console.log(`✅ PostgreSQL conectado — ${rows.rows.length} estabelecimento(s), ${clientRows.rows.length} cliente(s), ${orderRows.rows.length} pedido(s)`);
}

const dbSaveOrder = (o) => {
  if (!db) return Promise.resolve();
  return db.query(
    `INSERT INTO pedidos(order_id,phone,customer_name,pickup_address,delivery_address,distance_km,freight_price,status,note)
     VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9) ON CONFLICT(order_id) DO NOTHING`,
    [o.orderId, o.phone, o.establishmentName, o.pickupAddress, o.deliveryAddress, o.distanceKm, o.freightPrice, o.status, o.note]
  ).catch(e => console.error('dbSaveOrder:', e.message));
};

const dbUpdateOrder = (orderId, fields) => {
  if (!db) return Promise.resolve();
  const entries = Object.entries(fields);
  const sets    = entries.map(([k, _], i) => `${k}=$${i + 2}`).join(',');
  return db.query(`UPDATE pedidos SET ${sets} WHERE order_id=$1`, [orderId, ...entries.map(([_, v]) => v)])
    .catch(e => console.error('dbUpdateOrder:', e.message));
};

async function saveClient(phone, name, pickupAddress, pickupCoords) {
  clients.set(phone, { name, pickupAddress, pickupCoords });
  if (!db) return;
  await db.query(
    'INSERT INTO clients(phone,name,pickup_address,pickup_lat,pickup_lng) VALUES($1,$2,$3,$4,$5) ON CONFLICT(phone) DO UPDATE SET name=$2,pickup_address=$3,pickup_lat=$4,pickup_lng=$5',
    [phone, name, pickupAddress, pickupCoords.lat, pickupCoords.lng]
  ).catch(e => console.error('saveClient error:', e.message));
}

// ── In-memory stores ─────────────────────────────────────────────────────────
const deliveries      = new Map();
const pendingRequests = new Map();
const sessions        = new Map();
const establishments  = new Map();
const clients         = new Map(); // phone → { name, pickupAddress, pickupCoords }
const orders          = new Map();
const paymentToOrder  = new Map();

// ── WhatsApp auth state persistente no PostgreSQL ────────────────────────────
async function usePostgresAuthState(pool) {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS wa_auth (
      key   TEXT PRIMARY KEY,
      value TEXT NOT NULL
    )
  `);

  const readData = async (key) => {
    const r = await pool.query('SELECT value FROM wa_auth WHERE key=$1', [key]);
    return r.rows[0] ? JSON.parse(r.rows[0].value, BufferJSON.reviver) : null;
  };
  const writeData = (key, data) =>
    pool.query(
      'INSERT INTO wa_auth(key,value) VALUES($1,$2) ON CONFLICT(key) DO UPDATE SET value=$2',
      [key, JSON.stringify(data, BufferJSON.replacer)]
    );
  const removeData = (key) => pool.query('DELETE FROM wa_auth WHERE key=$1', [key]);

  let creds = (await readData('creds')) || initAuthCreds();

  return {
    state: {
      creds,
      keys: {
        get: async (type, ids) => {
          if (!ids.length) return {};
          const ph   = ids.map((_, i) => `$${i + 1}`).join(',');
          const rows = await pool.query(
            `SELECT key, value FROM wa_auth WHERE key IN (${ph})`,
            ids.map(id => `${type}-${id}`)
          );
          const data = {};
          for (const row of rows.rows) {
            const id = row.key.slice(type.length + 1);
            data[id] = JSON.parse(row.value, BufferJSON.reviver);
          }
          return data;
        },
        set: async (data) => {
          const tasks = [];
          for (const [type, typeData] of Object.entries(data))
            for (const [id, value] of Object.entries(typeData))
              tasks.push(value != null ? writeData(`${type}-${id}`, value) : removeData(`${type}-${id}`));
          await Promise.all(tasks);
        },
      },
    },
    saveCreds: () => writeData('creds', creds),
  };
}

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
    const { state, saveCreds } = db
      ? await usePostgresAuthState(db)
      : await useMultiFileAuthState(WA_DIR);
    waLog(`✅ Auth state carregado (${db ? 'PostgreSQL' : 'arquivo'})`);

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
        const code       = lastDisconnect?.error?.output?.statusCode;
        const isReplaced = code === DisconnectReason.connectionReplaced; // 440
        const reconnect  = code !== DisconnectReason.loggedOut;
        waLog(`⚠️ WA fechou (${code}) — ${isReplaced ? 'sessão substituída, aguardando...' : reconnect ? 'reconectando' : 'sessão expirada'}`);
        waReconnecting = false;
        // Código 440: outra instância assumiu (acontece durante deploys no Render).
        // Aguarda mais tempo para evitar loop de conflito entre instâncias.
        if (reconnect) setTimeout(connectWA, isReplaced ? 20000 : 5000);
      }
    });

    sock.ev.on('messages.upsert', async ({ messages, type }) => {
      waLog(`📨 messages.upsert type=${type} count=${messages.length}`);
      for (const msg of messages) {
        const jid    = msg.key.remoteJid || '';
        const fromMe = msg.key.fromMe;
        const text   = msg.message?.conversation
          || msg.message?.extendedTextMessage?.text
          || msg.message?.imageMessage?.caption
          || '';
        waLog(`  jid=${jid} fromMe=${fromMe} type=${type} text="${text.slice(0,30)}"`);
        if (fromMe) continue;
        if (jid.endsWith('@g.us')) continue;
        if (type !== 'notify') continue;
        if (!jid || !text) continue;
        handleBotMessage(jid, text).catch(async (e) => {
          console.error('Bot error:', e.message);
          await sendWhatsApp(jid, '⚠️ Erro interno. Tente novamente em instantes.').catch(() => {});
        });
      }
    });
  } catch (e) {
    waLog(`❌ connectWA erro: ${e.message}`);
    waStatus = 'error';
    waReconnecting = false;
    setTimeout(connectWA, 10000);
  }
}

function fetchWithTimeout(url, options = {}, ms = 12000) {
  const ctrl = new AbortController();
  const id   = setTimeout(() => ctrl.abort(), ms);
  return fetch(url, { ...options, signal: ctrl.signal }).finally(() => clearTimeout(id));
}

async function sendWhatsApp(phoneOrJid, text) {
  if (!waSocket) { console.log(`[WA offline → ${phoneOrJid}]`, text.slice(0, 50)); return; }
  const jid = phoneOrJid.includes('@') ? phoneOrJid : `${phoneOrJid}@s.whatsapp.net`;
  try { await waSocket.sendMessage(jid, { text }); }
  catch (e) { console.error('WA send error:', e.message); }
}

// ── Geocodificação ────────────────────────────────────────────────────────────
async function geocode(address) {
  if (!address) return null;
  const addr = address.trim();

  // Tenta variações progressivamente mais simples para achar o endereço
  const variants = [
    addr,
    `${addr}, Brasil`,
    // Remove número da casa ("nº 123", "n. 12", "123") e tenta só a rua + cidade
    addr.replace(/,?\s*(n[°º.]?\s*)?(\d+)\s*(?=-|,|$)/i, '').trim(),
  ].filter((v, i, a) => v.length > 4 && a.indexOf(v) === i); // remove duplicatas e strings muito curtas

  for (const q of variants) {
    try {
      const res  = await fetchWithTimeout(
        `https://nominatim.openstreetmap.org/search?q=${encodeURIComponent(q)}&format=json&limit=1&countrycodes=br`,
        { headers: { 'User-Agent': 'ZAPEntregas/1.0' } },
        8000
      );
      const data = await res.json();
      if (data.length) return { lat: parseFloat(data[0].lat), lng: parseFloat(data[0].lon), display: data[0].display_name };
    } catch {}
    await new Promise(r => setTimeout(r, 1100));
  }

  // Fallback: Photon (OpenStreetMap, cobertura melhor para endereços brasileiros)
  try {
    const res  = await fetchWithTimeout(
      `https://photon.komoot.io/api/?q=${encodeURIComponent(addr + ', Brasil')}&limit=1&lang=pt`,
      {}, 8000
    );
    const data = await res.json();
    const feat = data?.features?.[0];
    if (feat) {
      const [lng, lat] = feat.geometry.coordinates;
      return { lat, lng, display: feat.properties?.name || addr };
    }
  } catch {}

  return null;
}

function haversineKm(a, b) {
  const R = 6371, rad = Math.PI / 180;
  const dLat = (b.lat - a.lat) * rad;
  const dLon = (b.lng - a.lng) * rad;
  const h = Math.sin(dLat / 2) ** 2 + Math.cos(a.lat * rad) * Math.cos(b.lat * rad) * Math.sin(dLon / 2) ** 2;
  return R * 2 * Math.asin(Math.sqrt(h)) * 1.4; // ×1.4 fator de tortuosidade urbana
}

async function getRoadDistanceKm(from, to) {
  try {
    const res  = await fetchWithTimeout(
      `https://router.project-osrm.org/route/v1/driving/${from.lng},${from.lat};${to.lng},${to.lat}?overview=false`,
      {}, 8000
    );
    const data = await res.json();
    if (data.code === 'Ok') return data.routes[0].distance / 1000;
  } catch {}
  // Fallback: distância em linha reta × 1.4 (aproximação de rota urbana)
  return haversineKm(from, to);
}

function calcFreight(km) { return FREIGHT_TABLE.find(t => km <= t.maxKm).price; }

// ── Mercado Pago PIX ─────────────────────────────────────────────────────────
async function createPixPayment(orderId, amount, description, phone) {
  if (!MP_TOKEN) throw new Error('MERCADOPAGO_ACCESS_TOKEN não configurado');
  // phone pode ser JID completo (ex: 5511...@s.whatsapp.net) — limpa para usar no email
  const cleanPhone = String(phone).replace(/@.*$/, '').replace(/\D/g, '') || 'cliente';
  const res = await fetch('https://api.mercadopago.com/v1/payments', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${MP_TOKEN}`, 'X-Idempotency-Key': orderId },
    body: JSON.stringify({ transaction_amount: Number(amount), description, payment_method_id: 'pix', payer: { email: `frete${cleanPhone}@zapentregas.bot` } }),
  });
  const data = await res.json();
  if (!data.id) {
    console.error('MP API error:', JSON.stringify(data).slice(0, 400));
    throw new Error(data.message || data.error || 'Erro Mercado Pago');
  }
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
  await dbUpdateOrder(orderId, { status: 'pending', request_id: requestId });
  if (order.phone && order.establishmentName && order.pickupAddress && order.pickupCoords)
    await saveClient(order.phone, order.establishmentName, order.pickupAddress, order.pickupCoords);
  await sendWhatsApp(order.phone, `✅ *Pedido #${orderId} confirmado!*\n\n🛵 Procurando motoboy disponível...\n\nVocê receberá o link de rastreio assim que um motoboy aceitar.`);
}

// ── IA (Groq / Llama 3.3) ────────────────────────────────────────────────────
const SYSTEM_PROMPT = `Você é um assistente de entregas por motoboy chamado ZAP Entregas. Atende clientes via WhatsApp no Brasil.

Tabela de frete:
- Até 3km: R$8,00
- 3 a 6km: R$12,00
- 6 a 10km: R$16,00
- 10 a 15km: R$22,00
- Acima de 15km: R$30,00

Você precisa coletar (somente o que ainda não está confirmado na conversa):
1. Nome do cliente — se a saudação inicial já menciona o nome, NÃO pergunte novamente
2. Endereço de RETIRADA completo (rua + número + bairro/cidade) — se a saudação confirmou "mesmo local", está resolvido
3. Endereço de ENTREGA completo (rua + número + bairro/cidade)
4. Observações (opcional)

Regras:
- Seja simpático, rápido e direto. Use emojis com moderação.
- Pessoas frequentemente mandam o endereço PICADO (em partes, em mensagens separadas). Junte as partes antes de usar. Se ainda estiver incompleto, pergunte: "Pode confirmar o bairro/cidade?"
- Um endereço só está COMPLETO quando tem pelo menos: rua/local + referência de número ou ponto de referência + cidade ou bairro reconhecível.
- NUNCA dispare calculate_freight com endereço incompleto (ex: só "Rua das Flores" sem cidade/bairro).
- Se no histórico houver "Retirarei no mesmo local" ou "Retirada em: X", a retirada está confirmada — não pergunte de novo.
- Assim que tiver retirada + entrega completos, use action "calculate_freight".
- Para clientes novos: colete nome → retirada → entrega → obs (opcional).
- OBRIGATÓRIO: use "calculate_freight" ANTES de "confirm_order". NUNCA use "confirm_order" sem ter passado por "calculate_freight".
- Quando o cliente confirmar o pedido APÓS ver o resumo com frete (sim, confirmo, pode ser, ok, etc.), use action "confirm_order".
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
    "name": "nome extraído da conversa ou null",
    "pickup_address": "endereço de retirada extraído ou null",
    "delivery_address": "endereço de entrega extraído ou null",
    "note": "observação ou null"
  }
}`;

async function callGroq(messages) {
  const res = await fetchWithTimeout(
    'https://api.groq.com/openai/v1/chat/completions',
    {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${GROQ_KEY}` },
      body: JSON.stringify({
        model: 'llama-3.3-70b-versatile',
        messages: [{ role: 'system', content: SYSTEM_PROMPT }, ...messages],
        response_format: { type: 'json_object' },
        temperature: 0.3,
        max_tokens: 600,
      }),
    },
    25000 // Groq raramente demora mais que 10s, mas damos 25s de margem
  );
  const json = await res.json();
  if (!res.ok) throw new Error(json.error?.message || 'Groq error');
  return JSON.parse(json.choices[0].message.content);
}

// ── Bot com IA ────────────────────────────────────────────────────────────────
async function handleBotMessage(phone, text) {
  const msg = text.trim();

  // Sessão: { history, data, state, isReturning }
  let session = sessions.get(phone);
  const isNewSession = !session;
  if (!session) session = { history: [], data: {}, state: 'chatting', isReturning: false };

  // Primeira mensagem da sessão — saudação sem chamar IA
  if (isNewSession) {
    const known = clients.get(phone);
    if (known) {
      session.data.name    = known.name;
      session._savedPickup = { pickupAddress: known.pickupAddress, pickupCoords: known.pickupCoords };
      session.state        = 'confirm_pickup';
      const greeting = `Olá, *${known.name}*! 😊 Bem-vindo de volta ao *ZAP Entregas*! 🛵\n\nVai retirar no mesmo local da última vez?\n📍 _${known.pickupAddress}_\n\nResponda *SIM* ou informe o novo endereço de retirada.`;
      session.history.push({ role: 'user', content: msg });
      session.history.push({ role: 'assistant', content: greeting });
      sessions.set(phone, session);
      await sendWhatsApp(phone, greeting);
    } else {
      const greeting = `Olá! 👋 Bem-vindo ao *ZAP Entregas*! 🛵\n\nSou seu assistente de entregas por motoboy. Como posso te chamar?`;
      session.history.push({ role: 'user', content: msg });
      session.history.push({ role: 'assistant', content: greeting });
      sessions.set(phone, session);
      await sendWhatsApp(phone, greeting);
    }
    return;
  }

  // Confirmação do local de retirada (clientes recorrentes)
  if (session.state === 'confirm_pickup') {
    const saved  = session._savedPickup;
    const isYes  = /^(s|sim|yes|pode|pode ser|isso|isso mesmo|mesmo|ok|claro|tá|ta|tá bom|ta bom|confirma|confirmado)\s*[!.]*$/i.test(msg.trim());
    if (isYes) {
      session.isReturning        = true;
      session.data.pickupAddress = saved.pickupAddress;
      session.data.pickupCoords  = saved.pickupCoords;
      session.state              = 'chatting';
      const reply = `✅ Perfeito! Retirarei no mesmo local.\n\nQual é o endereço de *entrega*?`;
      session.history.push({ role: 'user', content: msg });
      session.history.push({ role: 'assistant', content: reply });
      sessions.set(phone, session);
      await sendWhatsApp(phone, reply);
    } else {
      session.isReturning        = false;
      session.data.pickupAddress = msg.trim();
      session.data.pickupCoords  = null;
      session.state              = 'chatting';
      const reply = `📍 Anotado! Retirada em: *${msg.trim()}*\n\nQual é o endereço de *entrega*?`;
      session.history.push({ role: 'user', content: msg });
      session.history.push({ role: 'assistant', content: reply });
      sessions.set(phone, session);
      await sendWhatsApp(phone, reply);
    }
    return;
  }

  // Pedido com erro de PIX — tentar gerar novamente
  if (session.state === 'pix_error') {
    const order = orders.get(session.orderId);
    if (!order) { sessions.delete(phone); return; }
    await sendWhatsApp(phone, '🔄 Tentando gerar o PIX novamente...');
    try {
      const { paymentId, copyPaste } = await createPixPayment(order.orderId, order.freightPrice, `Frete #${order.orderId}`, phone);
      order.paymentId = paymentId; order.status = 'awaiting_payment';
      paymentToOrder.set(paymentId, order.orderId);
      await dbUpdateOrder(order.orderId, { payment_id: paymentId, status: 'awaiting_payment' });
      sessions.set(phone, { ...session, state: 'awaiting_payment' });
      await sendWhatsApp(phone, `💳 *PIX gerado!*\n\nValor: *R$ ${order.freightPrice.toFixed(2).replace('.', ',')}*\n\nCopie o código abaixo:\n\n${copyPaste}\n\n_Confirmação automática após o pagamento._\n\nPara cancelar responda *CANCELAR*.`);
    } catch (e) {
      console.error('MP retry error:', e.message);
      await sendWhatsApp(phone, `❌ Erro ao gerar PIX: ${e.message}\n\nMande qualquer mensagem para tentar de novo.`);
    }
    return;
  }

  // Pedido aguardando pagamento — não passa pela IA
  if (session.state === 'awaiting_payment') {
    if (/^cancelar$/i.test(msg)) {
      const order = orders.get(session.orderId);
      if (order) { order.status = 'cancelled'; await dbUpdateOrder(order.orderId, { status: 'cancelled' }); }
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
      await sendWhatsApp(phone, '⏳ Calculando distância e frete...');

      // Clientes recorrentes já têm coords de retirada; novos precisam geocodificar
      const pickup = session.data.pickupCoords || await geocode(session.data.pickupAddress);
      const dest   = await geocode(session.data.deliveryAddress);

      if (!pickup && !session.data.pickupCoords) {
        const errMsg = `📍 Não encontrei o endereço de *retirada*: "${session.data.pickupAddress}"\n\nPode mandar a rua com número e cidade? Ex: _Rua das Flores, 123, São Paulo_`;
        session.history.push({ role: 'assistant', content: errMsg });
        sessions.set(phone, session);
        await sendWhatsApp(phone, errMsg);
        return;
      }
      if (!dest) {
        const errMsg = `📍 Não encontrei o endereço de *entrega*: "${session.data.deliveryAddress}"\n\nPode mandar a rua com número e cidade? Ex: _Av. Paulista, 1000, São Paulo_`;
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

      const pickupLine = session.isReturning
        ? 'Mesmo local da última vez ✅'
        : session.data.pickupAddress;
      const summary = `📦 *Resumo da entrega:*\n\n👤 *Cliente:* ${session.data.name}\n🏪 *Retirada:* ${pickupLine}\n📍 *Entrega:* ${session.data.deliveryAddress}\n📏 *Distância:* ${session.data.distanceKm} km\n💰 *Frete:* R$ ${session.data.freightPrice.toFixed(2).replace('.', ',')}${session.data.note ? `\n📝 *Obs:* ${session.data.note}` : ''}\n\nConfirma o pedido? Responda *SIM* para solicitar o motoboy.`;

      session.history.push({ role: 'assistant', content: summary });
      sessions.set(phone, session);
      await sendWhatsApp(phone, summary);
      break;
    }

    case 'confirm_order': {
      // Groq às vezes pula o calculate_freight e vai direto para confirm_order.
      // Se o frete ainda não foi calculado, calculamos aqui antes de continuar.
      if (!session.data.freightPrice) {
        if (!session.data.pickupAddress || !session.data.deliveryAddress) {
          await sendWhatsApp(phone, '⚠️ Preciso dos endereços de retirada e entrega para calcular o frete.');
          return;
        }
        await sendWhatsApp(phone, '⏳ Calculando frete...');
        const pickup = session.data.pickupCoords || await geocode(session.data.pickupAddress);
        const dest   = await geocode(session.data.deliveryAddress);
        if (!pickup || !dest) {
          await sendWhatsApp(phone, '❌ Não consegui encontrar um dos endereços. Pode confirmar com rua, número e cidade?');
          return;
        }
        const km = await getRoadDistanceKm(pickup, dest);
        if (!km) {
          await sendWhatsApp(phone, '❌ Não consegui calcular a rota. Tente informar os endereços novamente.');
          return;
        }
        session.data.pickupCoords   = pickup;
        session.data.deliveryCoords = dest;
        session.data.distanceKm     = Math.round(km * 10) / 10;
        session.data.freightPrice   = calcFreight(km);
        const pickupLine = session.isReturning ? 'Mesmo local da última vez ✅' : session.data.pickupAddress;
        const summary = `📦 *Resumo da entrega:*\n\n👤 *Cliente:* ${session.data.name}\n🏪 *Retirada:* ${pickupLine}\n📍 *Entrega:* ${session.data.deliveryAddress}\n📏 *Distância:* ${session.data.distanceKm} km\n💰 *Frete:* R$ ${session.data.freightPrice.toFixed(2).replace('.', ',')}${session.data.note ? `\n📝 *Obs:* ${session.data.note}` : ''}\n\nConfirma o pedido? Responda *SIM* para solicitar o motoboy.`;
        session.history.push({ role: 'assistant', content: summary });
        sessions.set(phone, session);
        await sendWhatsApp(phone, summary);
        return;
      }

      await sendWhatsApp(phone, aiReply.message);

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
        status: 'pending',
        createdAt: new Date().toISOString(),
      };
      orders.set(orderId, order);
      await dbSaveOrder(order);
      sessions.delete(phone); // libera sessão — pedido já foi criado
      await createRequestFromOrder(orderId);
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
  const key = req.headers['x-admin-key'] || req.query.key;
  if (key !== ADMIN_KEY) return res.status(401).json({ error: 'Unauthorized' });
  next();
}

const STATUS_LABEL = { awaiting_payment: '⏳ Aguardando PIX', pix_error: '⚠️ Erro PIX', paid: '💳 Pago', pending: '🔍 Aguardando motoboy', delivering: '🛵 Em entrega', cancelled: '❌ Cancelado', completed: '✅ Concluído' };
const STATUS_CSS   = { awaiting_payment: 'yellow', pix_error: 'red', paid: 'blue', pending: 'orange', delivering: 'green', cancelled: 'red', completed: 'darkgreen' };

app.get('/painel', (req, res) => {
  const key = req.headers['x-admin-key'] || req.query.key;
  if (key !== ADMIN_KEY) return res.send(`<!DOCTYPE html><html><head><meta charset="UTF-8"><title>ZAP Entregas</title><style>body{font-family:sans-serif;display:flex;align-items:center;justify-content:center;height:100vh;margin:0;background:#f0f2f5}form{background:#fff;padding:32px;border-radius:12px;box-shadow:0 2px 8px rgba(0,0,0,.12);text-align:center}h2{margin:0 0 16px}input{padding:10px 14px;border:1px solid #ddd;border-radius:8px;width:220px;font-size:14px}button{display:block;width:100%;margin-top:12px;padding:10px;background:#25D366;color:#fff;border:none;border-radius:8px;font-size:14px;cursor:pointer}</style></head><body><form method="GET"><h2>🛵 ZAP Entregas</h2><input name="key" type="password" placeholder="Chave de admin" required><button type="submit">Entrar</button></form></body></html>`);

  const all  = [...orders.values()].sort((a, b) => new Date(b.createdAt) - new Date(a.createdAt));
  const stats = {
    total:      all.length,
    aguardando: all.filter(o => o.status === 'awaiting_payment').length,
    entrega:    all.filter(o => o.status === 'delivering').length,
    cancelados: all.filter(o => o.status === 'cancelled').length,
  };

  const fmt = (v) => v ? `R$ ${Number(v).toFixed(2).replace('.', ',')}` : '—';
  const dt  = (s) => s ? new Date(s).toLocaleString('pt-BR', { timeZone: 'America/Sao_Paulo', day: '2-digit', month: '2-digit', hour: '2-digit', minute: '2-digit' }) : '—';

  const rows = all.map(o => `<tr>
    <td><strong>#${o.orderId}</strong><br><small>${dt(o.createdAt)}</small></td>
    <td>${o.establishmentName || '—'}<br><small>${o.phone || ''}</small></td>
    <td class="addr">${o.pickupAddress || '—'}</td>
    <td class="addr">${o.deliveryAddress || '—'}</td>
    <td>${o.distanceKm ? o.distanceKm + ' km' : '—'}</td>
    <td>${fmt(o.freightPrice)}</td>
    <td><span class="badge s-${STATUS_CSS[o.status] || 'gray'}">${STATUS_LABEL[o.status] || o.status}</span></td>
  </tr>`).join('');

  res.send(`<!DOCTYPE html>
<html lang="pt-BR"><head><meta charset="UTF-8">
<meta http-equiv="refresh" content="15;url=/painel?key=${encodeURIComponent(key)}">
<title>ZAP Entregas — Painel</title>
<style>
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:-apple-system,sans-serif;background:#f0f2f5}
header{background:#25D366;color:#fff;padding:14px 24px;display:flex;justify-content:space-between;align-items:center}
header h1{font-size:1rem;font-weight:700}
header small{opacity:.8;font-size:.78rem}
.stats{display:flex;gap:12px;padding:20px 24px;flex-wrap:wrap}
.stat{background:#fff;border-radius:10px;padding:14px 20px;box-shadow:0 1px 3px rgba(0,0,0,.1);min-width:130px}
.stat .n{font-size:2rem;font-weight:700;line-height:1}
.stat .l{font-size:.75rem;color:#666;margin-top:4px}
.wrap{padding:0 24px 32px;overflow-x:auto}
table{width:100%;border-collapse:collapse;background:#fff;border-radius:10px;overflow:hidden;box-shadow:0 1px 3px rgba(0,0,0,.1)}
th{padding:10px 14px;text-align:left;font-size:.72rem;color:#777;background:#fafafa;border-bottom:1px solid #eee;text-transform:uppercase;letter-spacing:.04em}
td{padding:10px 14px;border-top:1px solid #f0f0f0;font-size:.83rem;vertical-align:top}
.addr{max-width:160px;word-break:break-word}
small{color:#aaa;font-size:.73rem}
.badge{display:inline-block;padding:3px 9px;border-radius:20px;font-size:.72rem;font-weight:600}
.s-yellow{background:#fff3cd;color:#856404}
.s-blue{background:#cfe2ff;color:#0d47a1}
.s-orange{background:#ffe5d0;color:#9c4a0a}
.s-green{background:#d1fae5;color:#065f46}
.s-red{background:#fee2e2;color:#991b1b}
.s-darkgreen{background:#bbf7d0;color:#14532d}
.empty{text-align:center;color:#bbb;padding:40px!important}
</style></head><body>
<header>
  <h1>🛵 ZAP Entregas — Painel de Pedidos</h1>
  <small>Atualiza a cada 15s · ${new Date().toLocaleString('pt-BR', { timeZone: 'America/Sao_Paulo' })}</small>
</header>
<div class="stats">
  <div class="stat"><div class="n">${stats.total}</div><div class="l">Total de pedidos</div></div>
  <div class="stat"><div class="n">${stats.aguardando}</div><div class="l">Aguardando PIX</div></div>
  <div class="stat"><div class="n">${stats.entrega}</div><div class="l">Em entrega</div></div>
  <div class="stat"><div class="n">${stats.cancelados}</div><div class="l">Cancelados</div></div>
</div>
<div class="wrap">
<table><thead><tr>
  <th>Pedido</th><th>Cliente</th><th>Retirada</th><th>Entrega</th><th>Distância</th><th>Frete</th><th>Status</th>
</tr></thead><tbody>
${rows || '<tr><td colspan="7" class="empty">Nenhum pedido ainda</td></tr>'}
</tbody></table>
</div>
</body></html>`);
});

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
  await dbUpdateOrder(orderId, { status: 'paid' });
  await createRequestFromOrder(orderId);
});

app.get('/health', (_req, res) => res.json({ ok: true, uptime: process.uptime(), whatsapp: waStatus }));
app.get('/debug',  (_req, res) => res.json({ waStatus, hasQR: !!currentQR, logs: waLogs }));
app.post('/admin/reset-wa', requireAdmin, async (_req, res) => {
  waLog('🔄 Reset manual da sessão WA...');
  waReconnecting = false;
  if (waSocket) { try { waSocket.end(undefined); } catch(_) {} waSocket = null; }
  if (db) await db.query('DELETE FROM wa_auth').catch(() => {});
  else rmSync(WA_DIR, { recursive: true, force: true });
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
    if (request.orderId) { const o = orders.get(request.orderId); if (o) { o.status = 'delivering'; o.deliveryId = deliveryId; dbUpdateOrder(request.orderId, { status: 'delivering' }); } }
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
