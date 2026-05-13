'use strict';

/**
 * Simulação de rota:
 *   Av. Hera das Vinhas, 286 (Quinta dos Vinhedos)  → (-22.934, -46.532)
 *   Residencial Euroville, Bragança Paulista         → (-22.972, -46.516)
 */

const http = require('http');

const CONFIG = {
  host:        'localhost',
  port:        3000,
  steps:       40,       // pontos ao longo da rota
  intervalMs:  1800,     // ms entre cada envio (~2 s)
  gpsNoise:    0.00015,  // ruído aleatório p/ simular GPS real
  deliveryId:  process.argv[2] || null, // pode passar ID via argumento
};

const ROUTE = {
  start: { lat: -22.934, lng: -46.532, label: 'Av. Hera das Vinhas, 286 — Quinta dos Vinhedos' },
  end:   { lat: -22.972, lng: -46.516, label: 'Residencial Euroville — Bragança Paulista' },
};

// ── Gera pontos da rota com leve curvatura e ruído GPS ──────────────────────

function generateRoute(start, end, steps) {
  const points = [];

  // Ponto de controle da curva de Bezier (simula desvio de rua)
  const mid = {
    lat: (start.lat + end.lat) / 2 + 0.003,
    lng: (start.lng + end.lng) / 2 - 0.006,
  };

  for (let i = 0; i <= steps; i++) {
    const t = i / steps;
    const u = 1 - t;

    // Curva de Bézier quadrática
    const lat = u * u * start.lat + 2 * u * t * mid.lat + t * t * end.lat;
    const lng = u * u * start.lng + 2 * u * t * mid.lng + t * t * end.lng;

    // Ruído de GPS
    const noise = CONFIG.gpsNoise;
    points.push({
      lat: lat + (Math.random() - 0.5) * noise,
      lng: lng + (Math.random() - 0.5) * noise,
    });
  }
  return points;
}

// ── HTTP helpers ─────────────────────────────────────────────────────────────

function post(path, body) {
  return new Promise((resolve, reject) => {
    const data = JSON.stringify(body);
    const req = http.request({
      hostname: CONFIG.host,
      port:     CONFIG.port,
      path,
      method:  'POST',
      headers: {
        'Content-Type':   'application/json',
        'Content-Length': Buffer.byteLength(data),
      },
    }, (res) => {
      let raw = '';
      res.on('data', c => raw += c);
      res.on('end', () => {
        try { resolve(JSON.parse(raw)); } catch { resolve(raw); }
      });
    });
    req.on('error', reject);
    req.write(data);
    req.end();
  });
}

// ── Progress bar ─────────────────────────────────────────────────────────────

function progressBar(current, total, width = 25) {
  const filled = Math.round((current / total) * width);
  return '█'.repeat(filled) + '░'.repeat(width - filled);
}

// ── Main ─────────────────────────────────────────────────────────────────────

async function run() {
  console.log('\n╔══════════════════════════════════════════════════════╗');
  console.log('║  🛵  Simulador de Entrega — Bragança Paulista         ║');
  console.log('╚══════════════════════════════════════════════════════╝');

  // 1. Criar (ou reusar) entrega
  let deliveryId = CONFIG.deliveryId;
  let trackingUrl;

  if (!deliveryId) {
    console.log('\n📦 Criando nova entrega...');
    let res;
    try {
      res = await post('/create-delivery', {});
    } catch (err) {
      console.error(`\n❌ Não foi possível conectar em http://${CONFIG.host}:${CONFIG.port}`);
      console.error('   Certifique-se de que o servidor está rodando: node server.js\n');
      process.exit(1);
    }
    deliveryId  = res.deliveryId;
    trackingUrl = res.trackingUrl;
  } else {
    trackingUrl = `http://${CONFIG.host}:${CONFIG.port}/track/${deliveryId}`;
  }

  console.log(`\n  Delivery ID  : ${deliveryId}`);
  console.log(`  Rastreio     : ${trackingUrl}`);
  console.log(`\n  Envie este link pelo WhatsApp para o cliente acompanhar! 👆`);

  console.log(`\n  Rota:`);
  console.log(`    📍 Origem  : ${ROUTE.start.label}`);
  console.log(`    🏁 Destino : ${ROUTE.end.label}`);
  console.log(`\n  ${CONFIG.steps} atualizações · ${CONFIG.intervalMs / 1000}s de intervalo\n`);

  // 2. Gerar pontos
  const route = generateRoute(ROUTE.start, ROUTE.end, CONFIG.steps);

  // 3. Enviar pontos em loop
  for (let i = 0; i < route.length; i++) {
    const point = route[i];

    try {
      await post('/update-location', { deliveryId, lat: point.lat, lng: point.lng });
    } catch (err) {
      console.error(`\n❌ Erro ao enviar ponto ${i}: ${err.message}`);
      break;
    }

    const bar     = progressBar(i + 1, route.length);
    const pct     = Math.round(((i + 1) / route.length) * 100);
    const coords  = `(${point.lat.toFixed(5)}, ${point.lng.toFixed(5)})`;
    process.stdout.write(`\r  [${bar}] ${pct}%  ponto ${i + 1}/${route.length}  ${coords}   `);

    if (i < route.length - 1) {
      await new Promise(r => setTimeout(r, CONFIG.intervalMs));
    }
  }

  console.log('\n\n  ✅ Motorista chegou ao destino!');
  console.log(`  Entrega #${deliveryId} concluída.\n`);
}

run().catch(err => {
  console.error('\n❌ Erro inesperado:', err.message);
  process.exit(1);
});
