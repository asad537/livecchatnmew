import http from 'node:http';
import path from 'node:path';
import fs from 'node:fs';
import express from 'express';
import compression from 'compression';
import cors from 'cors';
import { Server } from 'socket.io';
import { loadConfig } from './core/config.js';
import { createDb } from './core/db.js';
import { createPresence } from './core/presence.js';
import { createBlocklist } from './core/blocklist.js';
import { createCache } from './core/cache.js';
import type { AppDeps } from './core/deps.js';
import { ensureSeed } from './seed.js';
import { buildApiRouter } from './http/router.js';
import { attachRealtime } from './realtime/index.js';
import { startKnowledgeRefresher } from './features/knowledge/index.js';
import { BOT_SENDER } from './domain/messages.js';

async function main(): Promise<void> {
  const config = loadConfig();
  const db = await createDb(config);
  await ensureSeed(db, config);
  const presence = createPresence();
  const blocklist = await createBlocklist(db);
  const cache = await createCache(config);

  const app = express();
  app.set('trust proxy', true); // honor X-Forwarded-For (nginx/Cloudflare) for real client IPs
  app.use(compression()); // gzip/brotli responses (widget.js 102KB → ~32KB on the wire)
  app.use(cors());
  app.use(express.json({ limit: '1mb' }));

  const server = http.createServer(app);
  const io = new Server(server, {
    cors: { origin: '*', methods: ['GET', 'POST'] },
    // Tolerant enough for throttled background tabs, but a slept/killed
    // machine still reads offline within ~30s (+ the presence grace).
    pingInterval: 25_000,
    pingTimeout: 30_000,
  });

  // Optional Redis adapter → multi-node fan-out (production scaling)
  if (config.redisUrl) {
    try {
      const [{ createAdapter }, { default: Redis }] = await Promise.all([
        import('@socket.io/redis-adapter' as string),
        import('ioredis' as string),
      ]);
      const pub = new Redis(config.redisUrl);
      const sub = pub.duplicate();
      io.adapter(createAdapter(pub, sub));
      console.log('[redis] Socket.IO redis adapter enabled');
    } catch {
      console.warn('[redis] REDIS_URL set but ioredis/@socket.io/redis-adapter not installed — running single-node');
    }
  }

  const deps: AppDeps = { config, db, presence, io, blocklist, cache };
  BOT_SENDER.name = config.alex.name;
  console.log(`[cache] ${cache.kind} cache enabled`);

  app.use(buildApiRouter(deps));
  attachRealtime(deps);
  startKnowledgeRefresher(deps); // daily re-scan of AI website knowledge

  // Embeddable widget bundle + demo business sites
  const widgetDist = path.join(config.repoRoot, 'apps', 'widget', 'dist');
  const demoDir = path.join(config.repoRoot, 'apps', 'widget', 'demo');
  app.get('/widget.js', (_req, res) => {
    const file = path.join(widgetDist, 'widget.js');
    if (!fs.existsSync(file)) {
      res.status(404).type('text/plain').send('// widget not built yet — run: npm run build -w apps/widget');
      return;
    }
    // Cache 5 min in the browser (ETag revalidation after that) — keeps embed
    // pages fast without delaying widget updates for long.
    res.setHeader('Cache-Control', 'public, max-age=300, stale-while-revalidate=600');
    res.sendFile(file);
  });
  app.use('/demo', express.static(demoDir));

  // Production dashboard (built SPA) at /app — `npm run build -w apps/dashboard`
  const dashboardDist = path.join(config.repoRoot, 'apps', 'dashboard', 'dist');
  if (fs.existsSync(path.join(dashboardDist, 'index.html'))) {
    // index.html must always revalidate (or browsers keep serving a stale
    // bundle after deploys); hashed /assets/ files are safe to cache hard.
    app.use(
      '/app',
      express.static(dashboardDist, {
        setHeaders: (res, filePath) => {
          if (filePath.endsWith('index.html')) {
            res.setHeader('Cache-Control', 'no-cache');
          } else if (filePath.includes(`${path.sep}assets${path.sep}`)) {
            res.setHeader('Cache-Control', 'public, max-age=2592000, immutable');
          } else {
            res.setHeader('Cache-Control', 'public, max-age=300');
          }
        },
      }),
    );
    app.get('/app/*', (_req, res) => {
      res.setHeader('Cache-Control', 'no-cache');
      res.sendFile(path.join(dashboardDist, 'index.html'));
    });
  }

  app.get('/health', (_req, res) => res.json({ ok: true, db: db.dialect }));
  app.get('/', (_req, res) =>
    res.redirect(fs.existsSync(path.join(dashboardDist, 'index.html')) ? '/app/' : '/demo/acme.html'),
  );

  server.listen(config.port, () => {
    console.log(`[livechat] server    → ${config.publicUrl}`);
    console.log(`[livechat] demo shop → ${config.publicUrl}/demo/acme.html`);
    console.log(`[livechat] database  → ${db.dialect}`);
  });
}

main().catch((err) => {
  console.error('[livechat] fatal:', err);
  process.exit(1);
});
