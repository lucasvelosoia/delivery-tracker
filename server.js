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
  || process.env.RENDER_EXTERNAL_URL
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
    CREATE TABLE IF NOT EXISTS drivers (
      phone      TEXT PRIMARY KEY,
      name       TEXT NOT NULL,
      active     BOOLEAN DEFAULT true,
      created_at TIMESTAMPTZ DEFAULT NOW()
    );
    CREATE TABLE IF NOT EXISTS partners (
      phone           TEXT PRIMARY KEY,
      name            TEXT NOT NULL,
      pickup_address  TEXT NOT NULL,
      pickup_lat      DOUBLE PRECISION NOT NULL,
      pickup_lng      DOUBLE PRECISION NOT NULL,
      default_package TEXT DEFAULT 'entrega',
      created_at      TIMESTAMPTZ DEFAULT NOW()
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
    orders.set(r.order_id, { orderId: r.order_id, phone: r.phone, establishmentName: r.customer_name, pickupAddress: r.pickup_address, deliveryAddress: r.delivery_address, distanceKm: r.distance_km, freightPrice: r.freight_price, status: r.status, requestId: r.request_id, deliveryId: r.delivery_id, driverName: r.driver_name, packageType: r.package_type, note: r.note, createdAt: r.created_at?.toISOString?.() || r.created_at });
  const driverRows = await db.query('SELECT * FROM drivers ORDER BY created_at DESC');
  for (const r of driverRows.rows)
    drivers.set(r.phone, { phone: r.phone, name: r.name, active: r.active, createdAt: r.created_at?.toISOString?.() || r.created_at });
  const partnerRows = await db.query('SELECT * FROM partners ORDER BY created_at DESC');
  for (const r of partnerRows.rows)
    registerPartner({ phone: r.phone, name: r.name, pickupAddress: r.pickup_address, pickupCoords: { lat: r.pickup_lat, lng: r.pickup_lng }, defaultPackage: r.default_package, createdAt: r.created_at?.toISOString?.() || r.created_at });
  console.log(`✅ PostgreSQL conectado — ${rows.rows.length} estabelecimento(s), ${clientRows.rows.length} cliente(s), ${orderRows.rows.length} pedido(s), ${driverRows.rows.length} entregador(es), ${partnerRows.rows.length} parceiro(s)`);
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
const drivers         = new Map(); // phone → { name, phone, active, createdAt }
const partners        = new Map(); // phone → { name, pickupAddress, pickupCoords, defaultPackage }

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
        const isPhoto = !!msg.message?.imageMessage;
        const text    = msg.message?.conversation
          || msg.message?.extendedTextMessage?.text
          || msg.message?.imageMessage?.caption
          || '';
        waLog(`  jid=${jid} fromMe=${fromMe} type=${type} isPhoto=${isPhoto} text="${text.slice(0,30)}"`);
        if (fromMe) continue;
        if (jid.endsWith('@g.us')) continue;
        if (type !== 'notify') continue;
        if (!jid || (!text && !isPhoto)) continue;
        handleBotMessage(jid, text, { hasPhoto: isPhoto }).catch(async (e) => {
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

function partnerPhoneVariants(phone) {
  const d = String(phone).replace(/\D/g, '');
  const variants = new Set([d]);
  if (d.startsWith('55') && d.length >= 12) variants.add(d.slice(2)); // sem código BR
  if (!d.startsWith('55') && d.length >= 10) variants.add('55' + d);  // com código BR
  return variants;
}

function registerPartner(p) {
  for (const v of partnerPhoneVariants(p.phone)) partners.set(v, p);
}

function findPartner(jid) {
  const digits = jid.replace(/\D/g, '');
  for (const v of partnerPhoneVariants(digits)) {
    if (partners.has(v)) return partners.get(v);
  }
  return null;
}

async function handleBotMessage(phone, text, opts = {}) {
  const msg      = text.trim();
  const hasPhoto = opts.hasPhoto || false;
  let session = sessions.get(phone);

  // Se a sessão existe mas não é de parceiro, verifica se o número agora é parceiro
  if (session && !session.isPartner) {
    const p = findPartner(phone);
    if (p) {
      sessions.delete(phone);
      session = null;
    }
  }

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
    const partner = findPartner(phone);
    if (partner) {
      session = { state: 'collect_delivery', data: { name: partner.name, pickupAddress: partner.pickupAddress, pickupCoords: partner.pickupCoords, packageType: partner.defaultPackage }, isPartner: true };
      sessions.set(phone, session);
      if (!msg) {
        // Foto sem legenda ou mensagem vazia — pede o endereço
        const saudacao = `Olá, *${partner.name}*! 😊`;
        await sendWhatsApp(phone, hasPhoto
          ? `${saudacao}\n\nRecebi a foto! 📸 Qual é o endereço de entrega?`
          : `${saudacao}\n\nMande a foto do pedido com o endereço na legenda, ou só o endereço de entrega.`);
        return;
      }
      // Tem texto (legenda da foto ou mensagem direta) — cai no state machine abaixo
    } else {
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
        session.isReturning    = false;
        session._pendingPickup = { address: coords.display || rawAddress, coords };
        session.state          = 'confirm_pickup_addr';
        sessions.set(phone, session);
        await sendWhatsApp(phone, `📍 Encontrei este local de retirada:\n\n*${coords.display || rawAddress}*\n\nEstá correto? Responda *SIM* para confirmar ou *NÃO* para informar novamente.`);
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
      session._pendingPickup = { address: coords.display || rawAddress, coords };
      session.state          = 'confirm_pickup_addr';
      sessions.set(phone, session);
      await sendWhatsApp(phone, `📍 Encontrei este local de retirada:\n\n*${coords.display || rawAddress}*\n\nEstá correto? Responda *SIM* para confirmar ou *NÃO* para informar novamente.`);
      break;
    }

    case 'confirm_pickup_addr': {
      const parsed = await aiParse(msg, 'O cliente está confirmando (sim) ou negando (não)? Retorne: {"intent": "confirm" | "deny"}');
      const intent = parsed?.intent ?? (YES_RE.test(msg) ? 'confirm' : 'deny');
      if (intent === 'confirm') {
        session.data.pickupAddress = session._pendingPickup.address;
        session.data.pickupCoords  = session._pendingPickup.coords;
        delete session._pendingPickup;
        session.state = 'collect_delivery';
        sessions.set(phone, session);
        await sendWhatsApp(phone, `✅ Retirada confirmada!\n\nQual é o endereço de *entrega*?\nEx: _Av. Paulista, 1000, Bela Vista, São Paulo_`);
      } else {
        delete session._pendingPickup;
        session.state = 'collect_pickup';
        sessions.set(phone, session);
        await sendWhatsApp(phone, `Ok! Me informe o endereço de retirada com mais detalhes.\nEx: _Rua das Flores, 123, Centro, São Paulo_`);
      }
      break;
    }

    case 'collect_delivery': {
      if (!msg) {
        await sendWhatsApp(phone, `📸 Recebi a foto! Qual é o endereço de entrega?\nEx: _Rua das Flores, 123, Centro, Bragança Paulista_`);
        return;
      }
      const addrParsed = await parseAddress(msg);
      if (addrParsed && !addrParsed.complete) {
        const missing = addrParsed.missing?.join(' e ') || 'cidade';
        await sendWhatsApp(phone, `📍 Falta *${missing}* no endereço de entrega. Pode completar?\nEx: _Av. Paulista, 1000, Bela Vista, São Paulo_`);
        return;
      }
      const rawAddress = addrParsed?.address || msg;
      await sendWhatsApp(phone, `⏳ Verificando endereço de entrega...`);
      const dest = await geocode(rawAddress);
      if (!dest) {
        await sendWhatsApp(phone, `📍 Não encontrei esse endereço. Pode informar com mais detalhes?\nEx: _Av. Paulista, 1000, Bela Vista, São Paulo_`);
        return;
      }
      const km = await getRoadDistanceKm(session.data.pickupCoords, dest);
      const distanceKm  = Math.round(km * 10) / 10;
      const freightPrice = calcFreight(km);
      session._pendingDelivery = { address: dest.display || rawAddress, coords: dest, distanceKm, freightPrice };
      session.state = 'confirm_delivery_addr';
      sessions.set(phone, session);
      await sendWhatsApp(phone,
        `📍 Encontrei este local de entrega:\n\n*${dest.display || rawAddress}*\n\n` +
        `📏 Distância: ${distanceKm} km  |  💰 Frete: *R$ ${freightPrice.toFixed(2).replace('.', ',')}*\n\n` +
        `Está correto? Responda *SIM* para confirmar ou *NÃO* para informar novamente.`
      );
      break;
    }

    case 'confirm_delivery_addr': {
      const parsed = await aiParse(msg, 'O cliente está confirmando (sim) ou negando (não)? Retorne: {"intent": "confirm" | "deny"}');
      const intent = parsed?.intent ?? (YES_RE.test(msg) ? 'confirm' : 'deny');
      if (intent === 'confirm') {
        session.data.deliveryAddress = session._pendingDelivery.address;
        session.data.deliveryCoords  = session._pendingDelivery.coords;
        session.data.distanceKm      = session._pendingDelivery.distanceKm;
        session.data.freightPrice    = session._pendingDelivery.freightPrice;
        delete session._pendingDelivery;
        if (session.data.packageType) {
          session.state = 'collect_note';
          sessions.set(phone, session);
          await sendWhatsApp(phone, `✅ Entrega confirmada!\n\nAlguma observação para o entregador? (Ex: ligar na portaria, entregar na recepção...)\nOu responda *NÃO* para pular.`);
        } else {
          session.state = 'collect_package';
          sessions.set(phone, session);
          await sendWhatsApp(phone, `✅ Entrega confirmada!\n\nO que será entregue?\nEx: _documento, caixa pequena, roupa, eletrônico, remédio..._`);
        }
      } else {
        delete session._pendingDelivery;
        session.state = 'collect_delivery';
        sessions.set(phone, session);
        await sendWhatsApp(phone, `Ok! Me informe o endereço de entrega com mais detalhes.\nEx: _Av. Paulista, 1000, Bela Vista, São Paulo_`);
      }
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
        session.orderId = orderId;
        if (session.isPartner) {
          session.state = 'partner_more_orders';
          sessions.set(phone, session);
          await sendWhatsApp(phone, `✅ *Pedido #${orderId} registrado!* 🛵\n\n🔍 Buscando entregador... O link de rastreio chega assim que alguém aceitar.`);
          await sendWhatsApp(phone, `Tem mais pedidos? Mande a próxima foto com o endereço na legenda ou responda *NÃO* para encerrar.`);
        } else {
          session.state = 'waiting_driver';
          sessions.set(phone, session);
          await sendWhatsApp(phone, `✅ *Pedido #${orderId} registrado!*\n\n🔍 Buscando entregador disponível...\nVocê será avisado assim que um motoboy aceitar.\n\nPara cancelar, responda *CANCELAR*.`);
        }
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

    case 'partner_more_orders': {
      if (NO_RE.test(msg) && !hasPhoto) {
        sessions.delete(phone);
        await sendWhatsApp(phone, `✅ Pedidos registrados. Até logo! 😊`);
        break;
      }
      // Nova foto ou mensagem com endereço — reinicia como nova entrega
      const p = findPartner(phone);
      if (p) {
        session.data = { name: p.name, pickupAddress: p.pickupAddress, pickupCoords: p.pickupCoords, packageType: p.defaultPackage };
      } else {
        delete session.data.deliveryAddress;
        delete session.data.deliveryCoords;
        delete session.data.distanceKm;
        delete session.data.freightPrice;
      }
      delete session._pendingDelivery;
      session.state = 'collect_delivery';
      sessions.set(phone, session);
      if (!msg) {
        // Foto sem legenda
        await sendWhatsApp(phone, `📸 Recebi a foto! Qual é o endereço de entrega?`);
      } else {
        // Já veio com endereço (legenda da foto ou texto) — processa direto
        await handleBotMessage(phone, text, opts);
      }
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

  const orderRows = all.map(o => `<tr>
    <td><strong>#${o.orderId}</strong><br><small>${dt(o.createdAt)}</small></td>
    <td>${o.establishmentName || '—'}<br><small>${o.phone || ''}</small></td>
    <td class="addr">${o.pickupAddress || '—'}</td>
    <td class="addr">${o.deliveryAddress || '—'}</td>
    <td>${o.distanceKm ? o.distanceKm + ' km' : '—'}</td>
    <td>${fmt(o.freightPrice)}</td>
    <td><span class="badge s-${STATUS_CSS[o.status] || 'gray'}">${STATUS_LABEL[o.status] || o.status}</span></td>
  </tr>`).join('');

  const driverList = [...drivers.values()].map(d => `<tr>
    <td>${d.name}</td>
    <td>${d.phone}</td>
    <td>${dt(d.createdAt)}</td>
    <td><button class="btn-danger" onclick="removeDriver('${d.phone}')">Remover</button></td>
  </tr>`).join('');

  const partnerList = [...partners.values()].map(p => `<tr>
    <td>${p.name}</td>
    <td>${p.phone}</td>
    <td>${p.pickupAddress}</td>
    <td>${p.defaultPackage}</td>
    <td>${dt(p.createdAt)}</td>
    <td><button class="btn-danger" onclick="removePartner('${p.phone}')">Remover</button></td>
  </tr>`).join('');

  res.send(`<!DOCTYPE html>
<html lang="pt-BR"><head><meta charset="UTF-8">
<title>ZAP Entregas — Painel</title>
<style>
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:-apple-system,sans-serif;background:#f0f2f5}
header{background:#25D366;color:#fff;padding:14px 24px;display:flex;justify-content:space-between;align-items:center;gap:12px}
header h1{font-size:1rem;font-weight:700;flex:1}
header small{opacity:.8;font-size:.78rem}
.btn{padding:8px 14px;border:none;border-radius:8px;font-size:.8rem;font-weight:600;cursor:pointer}
.btn-green{background:#25D366;color:#fff}
.btn-red{background:#ef4444;color:#fff}
.btn-danger{background:#fee2e2;color:#991b1b;padding:4px 10px;border:none;border-radius:6px;font-size:.72rem;cursor:pointer}
.btn-danger:hover{background:#fca5a5}
.stats{display:flex;gap:12px;padding:20px 24px;flex-wrap:wrap}
.stat{background:#fff;border-radius:10px;padding:14px 20px;box-shadow:0 1px 3px rgba(0,0,0,.1);min-width:130px}
.stat .n{font-size:2rem;font-weight:700;line-height:1}
.stat .l{font-size:.75rem;color:#666;margin-top:4px}
.section{padding:0 24px 32px}
.section-title{font-size:.8rem;font-weight:700;color:#555;text-transform:uppercase;letter-spacing:.06em;margin-bottom:12px}
.card{background:#fff;border-radius:10px;box-shadow:0 1px 3px rgba(0,0,0,.1);overflow:hidden}
.card-body{padding:16px 20px}
form.inline{display:flex;gap:8px;flex-wrap:wrap}
form.inline input{padding:9px 12px;border:1px solid #ddd;border-radius:8px;font-size:.83rem;flex:1;min-width:140px}
table{width:100%;border-collapse:collapse}
th{padding:10px 14px;text-align:left;font-size:.72rem;color:#777;background:#fafafa;border-bottom:1px solid #eee;text-transform:uppercase;letter-spacing:.04em}
td{padding:10px 14px;border-top:1px solid #f0f0f0;font-size:.83rem;vertical-align:top}
.addr{max-width:160px;word-break:break-word}
small{color:#aaa;font-size:.73rem}
.badge{display:inline-block;padding:3px 9px;border-radius:20px;font-size:.72rem;font-weight:600}
.s-orange{background:#ffe5d0;color:#9c4a0a}
.s-blue{background:#cfe2ff;color:#0d47a1}
.s-green{background:#d1fae5;color:#065f46}
.s-red{background:#fee2e2;color:#991b1b}
.s-darkgreen{background:#bbf7d0;color:#14532d}
.empty{text-align:center;color:#bbb;padding:40px!important}
#toast{position:fixed;bottom:24px;right:24px;background:#1f2937;color:#fff;padding:10px 18px;border-radius:8px;font-size:.83rem;display:none;z-index:99}
</style></head><body>

<header>
  <h1>🛵 ZAP Entregas — Painel</h1>
  <small id="clock">${new Date().toLocaleString('pt-BR', { timeZone: 'America/Sao_Paulo' })}</small>
  <button class="btn btn-red" onclick="clearSessions()">🧹 Limpar Sessões</button>
  <button class="btn btn-red" onclick="clearAll()" style="background:#7f1d1d">🗑️ Limpar Tudo</button>
</header>

<div class="stats">
  <div class="stat"><div class="n">${stats.total}</div><div class="l">Total de pedidos</div></div>
  <div class="stat"><div class="n">${stats.aguardando}</div><div class="l">Aguardando motoboy</div></div>
  <div class="stat"><div class="n">${stats.entrega}</div><div class="l">Em entrega</div></div>
  <div class="stat"><div class="n">${stats.cancelados}</div><div class="l">Cancelados</div></div>
  <div class="stat"><div class="n">${sessions.size}</div><div class="l">Sessões ativas</div></div>
  <div class="stat"><div class="n">${drivers.size}</div><div class="l">Entregadores</div></div>
  <div class="stat"><div class="n">${partners.size}</div><div class="l">Parceiros</div></div>
</div>

<div class="section">
  <div class="section-title">Entregadores</div>
  <div class="card">
    <div class="card-body" style="border-bottom:1px solid #f0f0f0">
      <form class="inline" onsubmit="addDriver(event)">
        <input id="drv-name" placeholder="Nome" required>
        <input id="drv-phone" placeholder="WhatsApp (ex: 11999990000)" required>
        <button type="submit" class="btn btn-green">+ Cadastrar</button>
      </form>
    </div>
    <table><thead><tr><th>Nome</th><th>Telefone</th><th>Cadastrado em</th><th></th></tr></thead>
    <tbody id="drv-list">
${driverList || '<tr><td colspan="4" class="empty">Nenhum entregador cadastrado</td></tr>'}
    </tbody></table>
  </div>
</div>

<div class="section">
  <div class="section-title">Parceiros (fluxo simplificado)</div>
  <div class="card">
    <div class="card-body" style="border-bottom:1px solid #f0f0f0">
      <form class="inline" onsubmit="addPartner(event)">
        <input id="prt-name" placeholder="Nome (ex: Floricultura Bella Flores)" required>
        <input id="prt-phone" placeholder="WhatsApp (ex: 11999990000)" required>
        <input id="prt-addr" placeholder="Endereço de retirada" required style="min-width:220px">
        <input id="prt-pkg" placeholder="Tipo de entrega padrão (ex: flores)">
        <button type="submit" class="btn btn-green">+ Cadastrar</button>
      </form>
    </div>
    <table><thead><tr><th>Nome</th><th>Telefone</th><th>Endereço de retirada</th><th>Pacote padrão</th><th>Cadastrado em</th><th></th></tr></thead>
    <tbody>
${partnerList || '<tr><td colspan="6" class="empty">Nenhum parceiro cadastrado</td></tr>'}
    </tbody></table>
  </div>
</div>

<div class="section">
  <div class="section-title">Pedidos</div>
  <div class="card">
    <table><thead><tr>
      <th>Pedido</th><th>Cliente</th><th>Retirada</th><th>Entrega</th><th>Distância</th><th>Frete</th><th>Status</th>
    </tr></thead><tbody>
${orderRows || '<tr><td colspan="7" class="empty">Nenhum pedido ainda</td></tr>'}
    </tbody></table>
  </div>
</div>

<div id="toast"></div>

<script>
const KEY = ${JSON.stringify(key)};

function toast(msg, ok = true) {
  const t = document.getElementById('toast');
  t.textContent = msg;
  t.style.background = ok ? '#166534' : '#991b1b';
  t.style.display = 'block';
  setTimeout(() => t.style.display = 'none', 3000);
}

async function api(method, path, body) {
  const r = await fetch(path, {
    method,
    headers: { 'Content-Type': 'application/json', 'x-admin-key': KEY },
    body: body ? JSON.stringify(body) : undefined,
  });
  return r.json();
}

async function addDriver(e) {
  e.preventDefault();
  const name  = document.getElementById('drv-name').value.trim();
  const phone = document.getElementById('drv-phone').value.trim();
  const res   = await api('POST', '/admin/driver', { name, phone });
  if (res.ok) { toast('Entregador cadastrado!'); setTimeout(() => location.reload(), 800); }
  else toast(res.error || 'Erro', false);
}

async function removeDriver(phone) {
  if (!confirm('Remover este entregador?')) return;
  const res = await api('DELETE', '/admin/driver/' + phone);
  if (res.ok) { toast('Removido.'); setTimeout(() => location.reload(), 800); }
  else toast('Erro', false);
}

async function addPartner(e) {
  e.preventDefault();
  const name           = document.getElementById('prt-name').value.trim();
  const phone          = document.getElementById('prt-phone').value.trim();
  const pickupAddress  = document.getElementById('prt-addr').value.trim();
  const defaultPackage = document.getElementById('prt-pkg').value.trim() || 'entrega';
  const res = await api('POST', '/admin/partner', { name, phone, pickupAddress, defaultPackage });
  if (res.ok) { toast('Parceiro cadastrado!'); setTimeout(() => location.reload(), 800); }
  else toast(res.error || 'Erro ao geocodificar o endereço', false);
}

async function removePartner(phone) {
  if (!confirm('Remover este parceiro?')) return;
  const res = await api('DELETE', '/admin/partner/' + phone);
  if (res.ok) { toast('Removido.'); setTimeout(() => location.reload(), 800); }
  else toast('Erro', false);
}

async function clearSessions() {
  if (!confirm('Limpar todas as conversas ativas?')) return;
  const res = await api('POST', '/admin/clear-sessions');
  if (res.ok) toast('Sessões limpas — ' + res.cleared + ' removida(s).');
  else toast('Erro', false);
}

async function clearAll() {
  if (!confirm('⚠️ Isso apaga TODAS as sessões e TODO o histórico de pedidos do banco. Continuar?')) return;
  const res = await api('POST', '/admin/clear-all');
  if (res.ok) toast('Reset completo: ' + res.sessions + ' sessao(oes) e ' + res.orders + ' pedido(s) apagados.');
  else toast('Erro', false);
  setTimeout(() => location.reload(), 1500);
}

// Atualiza pedidos automaticamente sem reload completo
setInterval(async () => {
  try {
    const orders = await api('GET', '/admin/orders');
    // reconstrói apenas a tabela de pedidos
  } catch {}
}, 15000);
</script>
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

app.get('/admin/drivers', requireAdmin, (_req, res) =>
  res.json([...drivers.values()]));

app.post('/admin/driver', requireAdmin, async (req, res) => {
  const { phone, name } = req.body;
  if (!phone || !name) return res.status(400).json({ error: 'phone e name obrigatórios' });
  const driver = { phone: String(phone).replace(/\D/g, ''), name: name.trim(), active: true, createdAt: new Date().toISOString() };
  drivers.set(driver.phone, driver);
  if (db) await db.query(
    'INSERT INTO drivers(phone,name,active) VALUES($1,$2,true) ON CONFLICT(phone) DO UPDATE SET name=$2,active=true',
    [driver.phone, driver.name]
  ).catch(e => console.error('saveDriver:', e.message));
  res.json({ ok: true, driver });
});

app.delete('/admin/driver/:phone', requireAdmin, async (req, res) => {
  const phone = req.params.phone.replace(/\D/g, '');
  drivers.delete(phone);
  if (db) await db.query('DELETE FROM drivers WHERE phone=$1', [phone]).catch(e => console.error('deleteDriver:', e.message));
  res.json({ ok: true });
});

app.post('/admin/clear-sessions', requireAdmin, (_req, res) => {
  const count = sessions.size;
  sessions.clear();
  console.log(`🧹 Sessões limpas (${count} removidas)`);
  res.json({ ok: true, cleared: count });
});

app.post('/admin/clear-all', requireAdmin, async (_req, res) => {
  const sessionCount = sessions.size;
  const orderCount   = orders.size;
  const clientCount  = clients.size;
  sessions.clear();
  orders.clear();
  pendingRequests.clear();
  clients.clear();
  if (db) {
    await db.query('DELETE FROM pedidos').catch(e => console.error('clear-all pedidos:', e.message));
    await db.query('DELETE FROM clients').catch(e => console.error('clear-all clients:', e.message));
  }
  console.log(`🧹 Reset total — ${sessionCount} sessão(ões), ${orderCount} pedido(s), ${clientCount} cliente(s) removidos`);
  res.json({ ok: true, sessions: sessionCount, orders: orderCount, clients: clientCount });
});

app.get('/admin/partners', requireAdmin, (_req, res) =>
  res.json([...partners.values()]));

app.post('/admin/partner', requireAdmin, async (req, res) => {
  const { phone, name, pickupAddress } = req.body;
  if (!phone || !name || !pickupAddress) return res.status(400).json({ error: 'phone, name e pickupAddress obrigatórios' });
  const defaultPackage = req.body.defaultPackage || 'entrega';
  const coords = await geocode(pickupAddress);
  if (!coords) return res.status(400).json({ error: 'Não foi possível geocodificar o endereço de retirada' });
  const partner = { phone: String(phone).replace(/\D/g, ''), name: name.trim(), pickupAddress, pickupCoords: coords, defaultPackage, createdAt: new Date().toISOString() };
  registerPartner(partner);
  if (db) await db.query(
    'INSERT INTO partners(phone,name,pickup_address,pickup_lat,pickup_lng,default_package) VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT(phone) DO UPDATE SET name=$2,pickup_address=$3,pickup_lat=$4,pickup_lng=$5,default_package=$6',
    [partner.phone, partner.name, partner.pickupAddress, coords.lat, coords.lng, partner.defaultPackage]
  ).catch(e => console.error('savePartner:', e.message));
  res.json({ ok: true, partner });
});

app.delete('/admin/partner/:phone', requireAdmin, async (req, res) => {
  const phone = req.params.phone.replace(/\D/g, '');
  partners.delete(phone);
  if (db) await db.query('DELETE FROM partners WHERE phone=$1', [phone]).catch(e => console.error('deletePartner:', e.message));
  res.json({ ok: true });
});

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
      await sendWhatsApp(request.phone, `🛵 *Entregador a caminho!*\n\n👤 *${driverName}* está indo buscar seu pacote.`);
      await sendWhatsApp(request.phone, `📍 Acompanhe em tempo real:\n${trackingUrl}`);
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
        await sendWhatsApp(order.phone, `📦 *Pacote retirado!*\n\n${d.driverName || 'O entregador'} já coletou seu pacote e está a caminho do destino.`);
        await sendWhatsApp(order.phone, `📍 Acompanhe em tempo real:\n${trackingUrl}`);
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
