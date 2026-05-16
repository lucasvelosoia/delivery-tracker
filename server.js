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
const ADMIN_KEY    = process.env.ADMIN_KEY || 'admin123';
const DB_URL       = process.env.DATABASE_URL || '';
const GROQ_KEY     = process.env.GROQ_API_KEY || '';
const GOOGLE_KEY   = process.env.GOOGLE_MAPS_KEY || '';
const MAPBOX_TOKEN = process.env.MAPBOX_TOKEN || '';

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
      status           TEXT DEFAULT 'pending',
      request_id       TEXT,
      delivery_id      TEXT,
      driver_name      TEXT,
      package_type     TEXT,
      note             TEXT,
      created_at       TIMESTAMPTZ DEFAULT NOW()
    );
    ALTER TABLE pedidos ADD COLUMN IF NOT EXISTS delivery_id TEXT;
    ALTER TABLE pedidos ADD COLUMN IF NOT EXISTS driver_name TEXT;
    ALTER TABLE pedidos ADD COLUMN IF NOT EXISTS package_type TEXT;
  `);
  const rows = await db.query('SELECT * FROM establishments');
  for (const r of rows.rows)
    establishments.set(r.phone, { name: r.name, address: r.address, lat: r.lat, lng: r.lng });
  const clientRows = await db.query('SELECT * FROM clients');
  for (const r of clientRows.rows)
    clients.set(r.phone, { name: r.name, pickupAddress: r.pickup_address, pickupCoords: { lat: r.pickup_lat, lng: r.pickup_lng } });
  const orderRows = await db.query('SELECT * FROM pedidos ORDER BY created_at DESC LIMIT 500');
  for (const r of orderRows.rows)
    orders.set(r.order_id, { orderId: r.order_id, phone: r.phone, establishmentName: r.customer_name, pickupAddress: r.pickup_address, deliveryAddress: r.delivery_address, distanceKm: r.distance_km, freightPrice: r.freight_price, status: r.status, requestId: r.request_id, deliveryId: r.delivery_id, driverName: r.driver_name, packageType: r.package_type, note: r.note, createdAt: r.created_at?.toISOString?.() || r.created_at });
  console.log(`✅ PostgreSQL conectado — ${rows.rows.length} estabelecimento(s), ${clientRows.rows.length} cliente(s), ${orderRows.rows.length} pedido(s)`);
}

const dbSaveOrder = (o) => {
  if (!db) return Promise.resolve();
  return db.query(
    `INSERT INTO pedidos(order_id,phone,customer_name,pickup_address,delivery_address,distance_km,freight_price,status,package_type,note)
     VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) ON CONFLICT(order_id) DO NOTHING`,
    [o.orderId, o.phone, o.establishmentName, o.pickupAddress, o.deliveryAddress, o.distanceKm, o.freightPrice, o.status, o.packageType || '', o.note]
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

// 1. ViaCEP — resolve CEP brasileiro em endereço completo
async function lookupCep(raw) {
  const digits = raw.replace(/\D/g, '');
  if (digits.length !== 8) return null;
  try {
    const res  = await fetchWithTimeout(`https://viacep.com.br/ws/${digits}/json/`, {}, 6000);
    const data = await res.json();
    if (data.erro) return null;
    return [data.logradouro, data.bairro, data.localidade, data.uf].filter(Boolean).join(', ');
  } catch { return null; }
}

// 2. Google Maps Geocoding (melhor cobertura para Brasil)
async function geocodeGoogle(addr) {
  if (!GOOGLE_KEY) return null;
  try {
    const res  = await fetchWithTimeout(
      `https://maps.googleapis.com/maps/api/geocode/json?address=${encodeURIComponent(addr)}&region=br&language=pt-BR&key=${GOOGLE_KEY}`,
      {}, 8000
    );
    const data = await res.json();
    const r    = data?.results?.[0];
    if (!r) return null;
    return { lat: r.geometry.location.lat, lng: r.geometry.location.lng, display: r.formatted_address };
  } catch { return null; }
}

// 3. Mapbox Geocoding (100k req/mês grátis, boa cobertura BR)
async function geocodeMapbox(addr) {
  if (!MAPBOX_TOKEN) return null;
  try {
    const res  = await fetchWithTimeout(
      `https://api.mapbox.com/geocoding/v5/mapbox.places/${encodeURIComponent(addr)}.json?country=br&language=pt&limit=1&access_token=${MAPBOX_TOKEN}`,
      {}, 8000
    );
    const data = await res.json();
    const feat = data?.features?.[0];
    if (!feat) return null;
    const [lng, lat] = feat.center;
    return { lat, lng, display: feat.place_name };
  } catch { return null; }
}

// 4. Nominatim (OSM) — sem chave, mas fraco para BR
async function geocodeNominatim(addr) {
  const variants = [
    addr,
    `${addr}, Brasil`,
    addr.replace(/,?\s*(n[°º.]?\s*)?(\d+)\s*(?=,|$)/i, '').trim(),
  ].filter((v, i, a) => v.length > 4 && a.indexOf(v) === i);

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
  return null;
}

// 5. Photon (Komoot) — fallback OSM com melhor cobertura BR
async function geocodePhoton(addr) {
  try {
    const res  = await fetchWithTimeout(
      `https://photon.komoot.io/api/?q=${encodeURIComponent(addr + ', Brasil')}&limit=1&lang=pt`,
      {}, 8000
    );
    const data = await res.json();
    const feat = data?.features?.[0];
    if (!feat) return null;
    const [lng, lat] = feat.geometry.coordinates;
    return { lat, lng, display: feat.properties?.name || addr };
  } catch { return null; }
}

// Orquestrador: CEP → Mapbox → Google → Nominatim → Photon
async function geocode(address) {
  if (!address) return null;
  const addr = address.trim();

  // Se parece com CEP, resolve via ViaCEP e usa o endereço expandido
  const cepExpanded = await lookupCep(addr);
  const query       = cepExpanded || addr;

  return (
    (await geocodeMapbox(query))    ||
    (await geocodeGoogle(query))    ||
    (await geocodeNominatim(query)) ||
    (await geocodePhoton(query))    ||
    null
  );
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

// ── Criar corrida após confirmação ───────────────────────────────────────────
async function createRequestFromOrder(orderId) {
  const order = orders.get(orderId);
  if (!order || order.requestId) return;
  const requestId = uuidv4().slice(0, 8).toUpperCase();
  const request = {
    requestId,
    customerName: order.establishmentName,
    address:      order.deliveryAddress,
    pickupAddress: order.pickupAddress,
    phone:        order.phone,
    note:         order.note,
    packageType:  order.packageType,
    distanceKm:   order.distanceKm,
    freightPrice: order.freightPrice,
    orderId,
    status: 'pending',
    createdAt: new Date().toISOString(),
  };
  order.requestId = requestId;
  order.status    = 'pending';
  pendingRequests.set(requestId, request);
  io.emit('new-delivery-request', request);
  console.log(`🚀 Corrida #${requestId} criada — ${order.establishmentName}`);
  await dbUpdateOrder(orderId, { status: 'pending', request_id: requestId });
  if (order.phone && order.establishmentName && order.pickupAddress && order.pickupCoords)
    await saveClient(order.phone, order.establishmentName, order.pickupAddress, order.pickupCoords);
}

// ── Parser IA — interpreta variações de linguagem (Groq, 100 tokens max) ─────
const PARSER_SYSTEM = 'Você é um parser de mensagens WhatsApp para um serviço de entregas no Brasil. Extraia exatamente o que for pedido e retorne JSON puro, sem texto extra.';

async function aiParse(userMsg, instruction) {
  if (!GROQ_KEY) return null;
  try {
    const res = await fetchWithTimeout('https://api.groq.com/openai/v1/chat/completions', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${GROQ_KEY}` },
      body: JSON.stringify({
        model: 'llama-3.3-70b-versatile',
        messages: [
          { role: 'system', content: PARSER_SYSTEM },
          { role: 'user',   content: `Mensagem do cliente: "${userMsg}"\n\n${instruction}` },
        ],
        response_format: { type: 'json_object' },
        temperature: 0.1,
        max_tokens: 80,
      }),
    }, 12000);
    const json = await res.json();
    if (!res.ok) return null;
    return JSON.parse(json.choices[0].message.content);
  } catch { return null; }
}

// Valida e normaliza endereço via IA. Retorna null se aiParse indisponível.
async function parseAddress(msg) {
  return aiParse(msg,
    'Extraia e normalize o local de retirada ou entrega. Pode ser endereço convencional, ' +
    'nome de estabelecimento, ponto de referência ou qualquer combinação — número NÃO é obrigatório. ' +
    'Se CEP presente, expanda para logradouro+bairro+cidade. ' +
    'O ÚNICO campo obrigatório para complete=true é que haja uma cidade ou localidade identificável. ' +
    'Retorne JSON: {"address": "local normalizado com cidade ou null", "complete": true | false, "missing": ["cidade"]}'
  );
}

// ── Bot — máquina de estado (fluxo e cálculos 100% server-side) ──────────────
const YES_RE    = /^(s|si|sim|yes|pode|pode ser|isso|isso mesmo|mesmo|ok|claro|tá|ta|tá bom|ta bom|confirma|confirmado|quero|vai|bora|manda)\s*[!.]*$/i;
const NO_RE     = /^(n|nao|não|nope|nop|cancel|cancelar|desistir|para)\s*[!.]*$/i;
const SKIP_RE   = /^(n|nao|não|nope|nop|pular|skip|-|nenhuma|nada|sem obs)\s*[!.]*$/i;
const CANCEL_RE = /^cancelar\s*[!.]*$/i;

function buildSummary(data, isReturning) {
  const pickupLine = isReturning ? 'Mesmo local da última vez ✅' : data.pickupAddress;
  const etaMin     = Math.ceil(data.distanceKm * 3 + 5);
  return (
    `📦 *Resumo da entrega:*\n\n` +
    `👤 *Cliente:* ${data.name}\n` +
    `🏪 *Retirada:* ${pickupLine}\n` +
    `📍 *Entrega:* ${data.deliveryAddress}\n` +
    `📦 *Pacote:* ${data.packageType}\n` +
    `📏 *Distância:* ${data.distanceKm} km\n` +
    `⏱️ *Tempo estimado:* ~${etaMin} min\n` +
    `💰 *Frete:* R$ ${data.freightPrice.toFixed(2).replace('.', ',')}` +
    (data.note ? `\n📝 *Obs:* ${data.note}` : '') +
    `\n\nConfirma o pedido? Responda *SIM* para solicitar o motoboy ou *NÃO* para cancelar.`
  );
}

async function handleBotMessage(phone, text) {
  const msg = text.trim();
  let session = sessions.get(phone);

  // ── CANCELAR global — encerra qualquer estado ──────────────────────────────
  if (session && CANCEL_RE.test(msg)) {
    const order = session.orderId ? orders.get(session.orderId) : null;
    if (order && order.status === 'pending') {
      order.status = 'cancelled';
      await dbUpdateOrder(order.orderId, { status: 'cancelled' });
      if (order.requestId) {
        const req = pendingRequests.get(order.requestId);
        if (req) req.status = 'cancelled';
        io.emit('request-taken', { requestId: order.requestId });
      }
    }
    sessions.delete(phone);
    await sendWhatsApp(phone, '❌ Atendimento encerrado. Quando precisar é só chamar! 😊');
    return;
  }

  // ── Primeira mensagem: sem sessão ──────────────────────────────────────────
  if (!session) {
    const known = clients.get(phone);
    if (known) {
      session = { state: 'confirm_pickup', data: { name: known.name, pickupAddress: known.pickupAddress, pickupCoords: known.pickupCoords }, isReturning: false };
      sessions.set(phone, session);
      await sendWhatsApp(phone, `Olá, *${known.name}*! 😊 Bem-vindo de volta ao *ZAP Entregas*! 🛵\n\nVai retirar no mesmo local da última vez?\n📍 _${known.pickupAddress}_\n\nResponda *SIM* ou informe o novo endereço de retirada.`);
    } else {
      session = { state: 'collect_name', data: {}, isReturning: false };
      sessions.set(phone, session);
      await sendWhatsApp(phone, `Olá! 👋 Bem-vindo ao *ZAP Entregas*! 🛵\n\nSou seu assistente de entregas por motoboy.\n\nComo posso te chamar?`);
    }
    return;
  }

  // ── Máquina de estados ────────────────────────────────────────────────────
  switch (session.state) {

    case 'collect_name': {
      const parsed = await aiParse(msg, 'Extraia apenas o nome próprio do cliente. Retorne: {"name": "nome ou null"}');
      const name   = (parsed?.name || msg.replace(/^(me chamo|sou o|sou a|meu nome é|pode chamar de|pode me chamar de)\s+/i, '')).trim();
      if (!name || name.length < 2) {
        await sendWhatsApp(phone, 'Por favor, me diga seu nome para continuar. 😊');
        return;
      }
      session.data.name = name;
      session.state     = 'collect_pickup';
      sessions.set(phone, session);
      await sendWhatsApp(phone, `Prazer, *${name}*! 😊\n\nQual é o endereço de *retirada*?\nEx: _Rua das Flores, 123, Centro, São Paulo_`);
      break;
    }

    case 'confirm_pickup': {
      const parsed = await aiParse(msg,
        'O cliente está confirmando o mesmo local anterior ou informando um novo endereço de retirada? ' +
        'Se novo endereço, normalize para o formato "Logradouro, Número, Bairro, Cidade, UF". ' +
        'Retorne JSON: {"intent": "confirm" | "new_address", "address": "endereço normalizado completo se new_address, senão null"}'
      );
      const intent  = parsed?.intent ?? (YES_RE.test(msg) ? 'confirm' : 'new_address');
      if (intent === 'confirm') {
        session.isReturning = true;
        session.state       = 'collect_delivery';
        sessions.set(phone, session);
        await sendWhatsApp(phone, `✅ Perfeito! Retirada no mesmo local.\n\nQual é o endereço de *entrega*?\nEx: _Av. Paulista, 1000, Bela Vista, São Paulo_`);
      } else {
        const addrParsed = await parseAddress(msg);
        if (addrParsed && !addrParsed.complete) {
          const missing = addrParsed.missing?.join(' e ') || 'cidade';
          await sendWhatsApp(phone, `📍 Falta *${missing}* no endereço. Pode completar?\nEx: _Rua das Flores, 123, Centro, São Paulo_\n\nOu responda *SIM* para usar o mesmo local anterior.`);
          return;
        }
        const rawAddress = addrParsed?.address || msg;
        await sendWhatsApp(phone, `⏳ Verificando endereço de retirada...`);
        const coords = await geocode(rawAddress);
        if (!coords) {
          await sendWhatsApp(phone, `📍 Não encontrei esse endereço. Pode informar com mais detalhes?\nEx: _Rua das Flores, 123, Centro, São Paulo_\n\nOu responda *SIM* para usar o mesmo local anterior.`);
          return;
        }
        session.isReturning        = false;
        session.data.pickupAddress = rawAddress;
        session.data.pickupCoords  = coords;
        session.state              = 'collect_delivery';
        sessions.set(phone, session);
        await sendWhatsApp(phone, `✅ Retirada confirmada!\n\nQual é o endereço de *entrega*?\nEx: _Av. Paulista, 1000, Bela Vista, São Paulo_`);
      }
      break;
    }

    case 'collect_pickup': {
      const addrParsed = await parseAddress(msg);
      if (addrParsed && !addrParsed.complete) {
        const missing = addrParsed.missing?.join(' e ') || 'cidade';
        await sendWhatsApp(phone, `📍 Falta *${missing}* no endereço. Pode completar?\nEx: _Rua das Flores, 123, Centro, São Paulo_`);
        return;
      }
      const rawAddress = addrParsed?.address || msg;
      await sendWhatsApp(phone, `⏳ Verificando endereço de retirada...`);
      const coords = await geocode(rawAddress);
      if (!coords) {
        await sendWhatsApp(phone, `📍 Não encontrei esse endereço. Pode informar com mais detalhes?\nEx: _Rua das Flores, 123, Centro, São Paulo_`);
        return;
      }
      session.data.pickupAddress = rawAddress;
      session.data.pickupCoords  = coords;
      session.state              = 'collect_delivery';
      sessions.set(phone, session);
      await sendWhatsApp(phone, `✅ Retirada confirmada!\n\nQual é o endereço de *entrega*?\nEx: _Av. Paulista, 1000, Bela Vista, São Paulo_`);
      break;
    }

    case 'collect_delivery': {
      const addrParsed = await parseAddress(msg);
      if (addrParsed && !addrParsed.complete) {
        const missing = addrParsed.missing?.join(' e ') || 'cidade';
        await sendWhatsApp(phone, `📍 Falta *${missing}* no endereço de entrega. Pode completar?\nEx: _Av. Paulista, 1000, Bela Vista, São Paulo_`);
        return;
      }
      const rawAddress = addrParsed?.address || msg;
      await sendWhatsApp(phone, `⏳ Verificando endereço e calculando frete...`);
      const dest = await geocode(rawAddress);
      if (!dest) {
        await sendWhatsApp(phone, `📍 Não encontrei esse endereço. Pode informar com mais detalhes?\nEx: _Av. Paulista, 1000, Bela Vista, São Paulo_`);
        return;
      }
      const km = await getRoadDistanceKm(session.data.pickupCoords, dest);
      session.data.deliveryAddress = rawAddress;
      session.data.deliveryCoords  = dest;
      session.data.distanceKm      = Math.round(km * 10) / 10;
      session.data.freightPrice    = calcFreight(km);
      session.state                = 'collect_package';
      sessions.set(phone, session);
      await sendWhatsApp(phone, `✅ Entrega confirmada! Frete: *R$ ${session.data.freightPrice.toFixed(2).replace('.', ',')}* (${session.data.distanceKm} km)\n\nO que será entregue?\nEx: _documento, caixa pequena, roupa, eletrônico, remédio..._`);
      break;
    }

    case 'collect_package': {
      const parsed      = await aiParse(msg, 'Extraia o tipo de objeto ou pacote a ser entregue. Retorne: {"package": "tipo extraído e resumido ou null"}');
      const packageType = (parsed?.package || msg).trim();
      if (!packageType || packageType.length < 2) {
        await sendWhatsApp(phone, 'Por favor, descreva o que será entregue (ex: documento, caixa, roupa...).');
        return;
      }
      session.data.packageType = packageType;
      session.state            = 'collect_note';
      sessions.set(phone, session);
      await sendWhatsApp(phone, `Tem alguma *observação* para o entregador?\nEx: _portão preto_, _ligar antes_, _frágil_\n\nOu responda *NÃO* para pular.`);
      break;
    }

    case 'collect_note': {
      const parsed = await aiParse(msg, 'O cliente quer pular observações ou tem alguma nota? Retorne: {"skip": true | false, "note": "observação extraída ou null"}');
      const skip   = parsed?.skip ?? SKIP_RE.test(msg);
      session.data.note = skip ? '' : (parsed?.note || msg).trim();
      session.state     = 'confirming';
      sessions.set(phone, session);
      await sendWhatsApp(phone, buildSummary(session.data, session.isReturning));
      break;
    }

    case 'confirming': {
      const parsed = await aiParse(msg, 'O cliente está confirmando (sim) ou cancelando (não) o pedido? Retorne: {"intent": "confirm" | "cancel" | "unclear"}');
      const intent = parsed?.intent ?? (YES_RE.test(msg) ? 'confirm' : NO_RE.test(msg) ? 'cancel' : 'unclear');
      if (intent === 'confirm') {
        const orderId = uuidv4().slice(0, 8).toUpperCase();
        const order = {
          orderId, phone,
          establishmentName: session.data.name,
          pickupAddress:     session.data.pickupAddress,
          pickupCoords:      session.data.pickupCoords,
          deliveryAddress:   session.data.deliveryAddress,
          deliveryCoords:    session.data.deliveryCoords,
          packageType:       session.data.packageType,
          note:              session.data.note || '',
          distanceKm:        session.data.distanceKm,
          freightPrice:      session.data.freightPrice,
          requestId: null,
          status: 'pending',
          createdAt: new Date().toISOString(),
        };
        orders.set(orderId, order);
        await dbSaveOrder(order);
        session.state   = 'waiting_driver';
        session.orderId = orderId;
        sessions.set(phone, session);
        await sendWhatsApp(phone, `✅ *Pedido #${orderId} registrado!*\n\n🔍 Buscando entregador disponível...\nVocê será avisado assim que um motoboy aceitar.\n\nPara cancelar, responda *CANCELAR*.`);
        await createRequestFromOrder(orderId);
      } else if (intent === 'cancel') {
        sessions.delete(phone);
        await sendWhatsApp(phone, '❌ Pedido cancelado. Quando precisar é só chamar! 😊');
      } else {
        await sendWhatsApp(phone, 'Responda *SIM* para confirmar o pedido ou *NÃO* para cancelar.');
      }
      break;
    }

    case 'waiting_driver': {
      if (CANCEL_RE.test(msg)) {
        const order = orders.get(session.orderId);
        if (order && order.status === 'pending') {
          order.status = 'cancelled';
          await dbUpdateOrder(order.orderId, { status: 'cancelled' });
          if (order.requestId) {
            const req = pendingRequests.get(order.requestId);
            if (req) req.status = 'cancelled';
            io.emit('request-taken', { requestId: order.requestId });
          }
          sessions.delete(phone);
          await sendWhatsApp(phone, '❌ Pedido cancelado. Quando precisar é só chamar! 😊');
        } else {
          sessions.delete(phone);
          await sendWhatsApp(phone, '⚠️ Seu pedido já foi aceito por um entregador e não pode ser cancelado.');
        }
      } else {
        await sendWhatsApp(phone, `🔍 Ainda buscando um entregador para seu pedido *#${session.orderId}*.\n\nVocê será avisado assim que alguém aceitar.\n\nPara cancelar, responda *CANCELAR*.`);
      }
      break;
    }

    case 'in_transit': {
      const order = orders.get(session.orderId);
      const trackingUrl = order?.deliveryId ? `${HOST_URL}/track/${order.deliveryId}` : null;
      await sendWhatsApp(phone, `🛵 Seu pedido *#${session.orderId}* está em andamento.${trackingUrl ? `\n\n📍 Rastreie em: ${trackingUrl}` : ''}`);
      break;
    }

    default:
      sessions.delete(phone);
      await sendWhatsApp(phone, `Olá! 👋 Mande qualquer mensagem para começar um novo pedido pelo *ZAP Entregas*. 🛵`);
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

const STATUS_LABEL = { pending: '🔍 Aguardando motoboy', accepted: '✅ Motoboy aceito', in_transit: '🛵 Em entrega', cancelled: '❌ Cancelado', completed: '✅ Concluído' };
const STATUS_CSS   = { pending: 'orange', accepted: 'blue', in_transit: 'green', cancelled: 'red', completed: 'darkgreen' };

app.get('/painel', (req, res) => {
  const key = req.headers['x-admin-key'] || req.query.key;
  if (key !== ADMIN_KEY) return res.send(`<!DOCTYPE html><html><head><meta charset="UTF-8"><title>ZAP Entregas</title><style>body{font-family:sans-serif;display:flex;align-items:center;justify-content:center;height:100vh;margin:0;background:#f0f2f5}form{background:#fff;padding:32px;border-radius:12px;box-shadow:0 2px 8px rgba(0,0,0,.12);text-align:center}h2{margin:0 0 16px}input{padding:10px 14px;border:1px solid #ddd;border-radius:8px;width:220px;font-size:14px}button{display:block;width:100%;margin-top:12px;padding:10px;background:#25D366;color:#fff;border:none;border-radius:8px;font-size:14px;cursor:pointer}</style></head><body><form method="GET"><h2>🛵 ZAP Entregas</h2><input name="key" type="password" placeholder="Chave de admin" required><button type="submit">Entrar</button></form></body></html>`);

  const all  = [...orders.values()].sort((a, b) => new Date(b.createdAt) - new Date(a.createdAt));
  const stats = {
    total:      all.length,
    aguardando: all.filter(o => o.status === 'pending').length,
    entrega:    all.filter(o => ['accepted', 'in_transit'].includes(o.status)).length,
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
  <div class="stat"><div class="n">${stats.aguardando}</div><div class="l">Aguardando motoboy</div></div>
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
    const requestId  = String(data.requestId || '').toUpperCase();
    const driverName = (data.driverName || '').trim() || 'Entregador';
    const request    = pendingRequests.get(requestId);
    if (!request || request.status !== 'pending') { socket.emit('request-unavailable', { requestId }); return; }
    request.status = 'accepted';
    const deliveryId  = uuidv4().slice(0, 8).toUpperCase();
    const trackingUrl = `${HOST_URL}/track/${deliveryId}`;
    deliveries.set(deliveryId, { lastPosition: null, history: [], status: 'active', phone: request.phone, driverName, createdAt: new Date().toISOString() });
    if (request.orderId) {
      const o = orders.get(request.orderId);
      if (o) {
        o.status     = 'accepted';
        o.deliveryId = deliveryId;
        o.driverName = driverName;
        dbUpdateOrder(request.orderId, { status: 'accepted', delivery_id: deliveryId, driver_name: driverName });
      }
    }
    socket.emit('delivery-assigned', { deliveryId, trackingUrl, requestId });
    socket.broadcast.emit('request-taken', { requestId });
    console.log(`✅ Corrida #${requestId} → Entrega #${deliveryId} (${driverName})`);
    if (request.phone) {
      // Atualiza sessão do cliente para in_transit
      const sess = sessions.get(request.phone);
      if (sess && sess.state === 'waiting_driver') {
        sess.state      = 'in_transit';
        sess.deliveryId = deliveryId;
        sessions.set(request.phone, sess);
      }
      await sendWhatsApp(request.phone, `🛵 *Entregador a caminho!*\n\n👤 *${driverName}* vai buscar seu pacote em breve.\n\n📍 Acompanhe em tempo real:\n${trackingUrl}\n\n_O link atualiza automaticamente._`);
    }
  });

  socket.on('pickup-confirmed', async (data) => {
    const deliveryId = String(data.deliveryId || '').toUpperCase();
    const d = deliveries.get(deliveryId);
    if (!d) return;
    d.status = 'in_transit';
    const order = [...orders.values()].find(o => o.deliveryId === deliveryId);
    if (order) {
      order.status = 'in_transit';
      dbUpdateOrder(order.orderId, { status: 'in_transit' });
      if (order.phone) {
        const trackingUrl = `${HOST_URL}/track/${deliveryId}`;
        await sendWhatsApp(order.phone, `📦 *Pacote retirado!*\n\n${d.driverName || 'O entregador'} já coletou seu pacote e está a caminho do destino.\n\n📍 Acompanhe: ${trackingUrl}`);
      }
    }
    socket.emit('pickup-ack', { deliveryId });
  });

  socket.on('delivery-completed', async (data) => {
    const deliveryId = String(data.deliveryId || '').toUpperCase();
    const d = deliveries.get(deliveryId);
    if (!d) return;
    d.status = 'completed';
    const order = [...orders.values()].find(o => o.deliveryId === deliveryId);
    if (order) {
      order.status = 'completed';
      dbUpdateOrder(order.orderId, { status: 'completed' });
      if (order.phone) {
        sessions.delete(order.phone);
        await sendWhatsApp(order.phone, `✅ *Entrega concluída!*\n\nSeu pacote foi entregue com sucesso. Obrigado por usar o *ZAP Entregas*! 🛵\n\nQualquer coisa é só chamar. 😊`);
      }
    }
    socket.emit('delivery-complete-ack', { deliveryId });
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
  await dbInit();
  connectWA();
});
