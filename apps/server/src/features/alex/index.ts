// ─────────────────────────────────────────────────────────────
// Alex — self-hosted AI sales/support agent (Qwen on the GPU pod).
//
// Replaces the queue greeter when ALEX_BASE_URL is set. Alex:
//   • answers from a persona + FAQ learned from real agent chats,
//     website knowledge pages and similar past conversations (few-shot);
//   • can send real product photos (library) or generate a mockup (ComfyUI);
//   • hands over to a human with an agent-only note when a quote/human is needed.
// Alex speaks while the conversation is WAITING; the moment a human accepts
// (→ ACTIVE) Alex goes quiet, exactly like the old greeter.
// ─────────────────────────────────────────────────────────────
import fs from 'node:fs';
import path from 'node:path';
import { createGzip, gunzipSync } from 'node:zlib';
import { pipeline } from 'node:stream/promises';
import { Readable } from 'node:stream';
import { EV, WIDGET_NAMESPACE } from '@livechat/shared';
import type { AppDeps } from '../../core/deps.js';
import { newId, nowIso } from '../../core/db.js';
import { postMessage, botSenderFor, type MessageRow } from '../../domain/messages.js';
import { emitInboxUpdate } from '../../domain/conversations.js';
import { findRelevantKnowledge } from '../knowledge/index.js';
import sharp from 'sharp';

interface ConvRow {
  id: string;
  website_id: string;
  visitor_id: string;
  status: string;
  assigned_user_id: string | null;
  human_online_on_arrival?: number | null;
}
interface WebsiteRow {
  id: string;
  name: string;
  greeting: string;
  ai_enabled?: number | null;
}
interface VisitorRow {
  id: string;
  name: string | null;
  email: string | null;
}
interface PhotoRow {
  id: string;
  website?: string | null;
  company_logo?: number | null;
  original_name: string;
  mime: string;
  size: number;
  path: string;
  caption: string; tags?: string | null;
}

// Box styles / product types a customer may ask to see, with the words a caption may use for them.
const PHOTO_TYPES: Record<string, string[]> = {
  gable: ['gable'], mailer: ['mailer', 'mailing'], magnetic: ['magnetic', 'magnet'], rigid: ['rigid', 'setup box'],
  tuck: ['tuck'], sleeve: ['sleeve', 'slipcase'], drawer: ['drawer', 'slide'], pillow: ['pillow'], pizza: ['pizza'],
  cereal: ['cereal'], display: ['display', 'counter'], hexagon: ['hexagon', 'hexagonal'], tube: ['tube', 'cylinder', 'cylindrical'],
  bag: ['bag', 'shopping bag', 'paper bag'], pouch: ['pouch', 'sachet'], perfume: ['perfume', 'fragrance', 'cologne'],
  candle: ['candle'], cosmetic: ['cosmetic', 'skincare', 'beauty', 'makeup'], soap: ['soap'], jewelry: ['jewelry', 'jewellery', 'ring box', 'necklace'],
  cake: ['cake'], cupcake: ['cupcake'], donut: ['donut', 'doughnut'], burger: ['burger'], fries: ['fries', 'fry'],
  chocolate: ['chocolate', 'truffle'], cbd: ['cbd', 'hemp'], vape: ['vape', 'cartridge'], shoe: ['shoe', 'sneaker', 'footwear'],
  apparel: ['apparel', 'shirt', 'clothing', 'garment'], shirt: ['shirt', 'apparel', 'clothing'], bottle: ['bottle'], wine: ['wine'],
  tea: ['tea'], coffee: ['coffee'], toy: ['toy'], electronics: ['electronics', 'gadget'], phone: ['phone'], watch: ['watch'],
  subscription: ['subscription'], corrugated: ['corrugated'], shipping: ['shipping', 'mailer'], label: ['label', 'sticker'],
  sticker: ['sticker', 'label'], noodle: ['noodle', 'takeout'], takeout: ['takeout', 'noodle', 'take-out'], popcorn: ['popcorn'],
  sandwich: ['sandwich'], sushi: ['sushi'], macaron: ['macaron'], cookie: ['cookie', 'biscuit'], chip: ['chip'], lipstick: ['lipstick'],
  hair: ['hair extension', 'hair'], wig: ['wig', 'hair'], eyelash: ['eyelash', 'lash'], lash: ['lash', 'eyelash'], serum: ['serum', 'dropper'],
  cigarette: ['cigarette', 'pre-roll', 'preroll'], book: ['book'], folder: ['folder'], envelope: ['envelope'], card: ['card'],
  hanger: ['hang tab', 'hanger'], window: ['window'], handle: ['handle'], heart: ['heart'],
};
interface ExampleRow {
  dialogue: string;
}

type ChatMsg =
  | { role: 'system' | 'user' | 'assistant'; content: string }
  | { role: 'assistant'; content: string | null; tool_calls: ToolCall[] }
  | { role: 'tool'; tool_call_id: string; content: string };
interface ToolCall {
  id: string;
  type: 'function';
  function: { name: string; arguments: string };
}

const inFlight = new Set<string>();
/** Conversations that got a new visitor message while a turn was running → run again after. */
const pendingTurn = new Set<string>();
/** File log (pm2 stdout capture is unreliable here). */
function alog(msg: string): void {
  const line = `${new Date().toISOString()} ${msg}\n`;
  console.log('[alex]', msg);
  try { fs.appendFileSync('/root/livechat2/alex/alex.log', line); } catch { /* ignore */ }
}
const DEBOUNCE_MS = 2500;
const HISTORY_LIMIT = 24;
const MAX_TOOL_ROUNDS = 3;
const HOLD_GREETING =
  "Thanks for reaching out! 🙌 You're now in the queue — one of our agents will be with you in just a moment.";

const sleep = (ms: number): Promise<void> => new Promise((r) => setTimeout(r, ms));

// ─── Persona / FAQ files (reloaded every 5 min) ──────────────

interface PriceQuote { product?: string; style_material?: string; size?: string | null; quantity?: number | null; unit_price_usd?: number | null; total_usd?: number | null; includes?: string; note?: string; site?: string; date?: string }
let personaCache: { at: number; persona: string; faq: Record<string, string>; policy: string; prices: PriceQuote[] } | null = null;

function loadPersona(deps: AppDeps): { persona: string; faq: Record<string, string>; policy: string; prices: PriceQuote[] } {
  if (personaCache && Date.now() - personaCache.at < 5 * 60_000) return personaCache;
  const dir = deps.config.alex.dataDir;
  let persona = '';
  let faq: Record<string, string> = {};
  try {
    persona = fs.readFileSync(path.join(dir, 'persona.md'), 'utf8');
  } catch {
    /* persona optional */
  }
  try {
    faq = JSON.parse(fs.readFileSync(path.join(dir, 'faq.json'), 'utf8')) as Record<string, string>;
  } catch {
    /* faq optional */
  }
  let policy = '';
  try {
    const pj = JSON.parse(fs.readFileSync(path.join(dir, 'policy.json'), 'utf8')) as Record<string, string>;
    policy = Object.entries(pj).filter(([k]) => !k.startsWith('_')).map(([k, v]) => `- ${k.replace(/_/g, ' ')}: ${v}`).join('\n');
  } catch {
    /* policy optional */
  }
  let prices: PriceQuote[] = [];
  try {
    prices = (JSON.parse(fs.readFileSync(path.join(dir, 'prices.json'), 'utf8')) as { quotes?: PriceQuote[] }).quotes ?? [];
  } catch {
    /* prices optional */
  }
  personaCache = { at: Date.now(), persona, faq, policy, prices };
  return personaCache;
}

const PRICE_INTENT_RE = /\b(price|prices|pricing|cost|costs|how much|quote|quotation|budget|cheap|cheaper|expensive|rate|rates|per box|per unit|per piece|discount|afford|estimate)\b/i;
/** Up to 10 real past quotes matching the customer's product words, as prompt lines. */
function pastQuotesFor(prices: PriceQuote[], customerText: string): string {
  if (!prices.length) return '';
  const words = customerText.toLowerCase().split(/[^a-z]+/).filter((w) => w.length > 3);
  const score = (q: PriceQuote): number => {
    const hay = `${q.product ?? ''} ${q.style_material ?? ''}`.toLowerCase();
    return words.reduce((n, w) => n + (hay.includes(w) ? 1 : 0), 0);
  };
  const ranked = [...prices].map((q) => ({ q, s: score(q) })).sort((a, b) => b.s - a.s);
  const picked = (ranked[0]?.s ? ranked.filter((r) => r.s > 0) : ranked).slice(0, 10).map((r) => r.q);
  return picked.map((q) => `- ${q.quantity ?? '?'} x ${q.product ?? 'box'}${q.style_material ? ` (${q.style_material})` : ''}${q.size ? ` ${q.size}` : ''}: ${q.unit_price_usd ? `$${q.unit_price_usd}/unit` : ''}${q.unit_price_usd && q.total_usd ? ', ' : ''}${q.total_usd ? `$${q.total_usd} total` : ''}${q.includes ? `, incl. ${q.includes}` : ''}${q.note ? ` [${q.note}]` : ''}`).join('\n');
}

/** FAQ block for a website: exact name match, else the "default" entry. */
function faqFor(faq: Record<string, string>, websiteName: string): string {
  const key = Object.keys(faq).find((k) => k.toLowerCase() === websiteName.toLowerCase());
  return (key ? faq[key] : faq.default) ?? '';
}

// ─── Few-shot examples from real agent chats ─────────────────

async function similarExamples(deps: AppDeps, query: string, limit = 3): Promise<string> {
  const terms = query
    .toLowerCase()
    .split(/[^a-z0-9]+/)
    .filter((w) => w.length > 2)
    .slice(0, 20)
    .join(' ');
  if (!terms) return '';
  try {
    const rows = await deps.db.all<ExampleRow>(
      `SELECT dialogue FROM alex_examples
        WHERE MATCH(visitor_text) AGAINST (? IN NATURAL LANGUAGE MODE)
        ORDER BY MATCH(visitor_text) AGAINST (? IN NATURAL LANGUAGE MODE) DESC
        LIMIT ${limit}`,
      [terms, terms],
    );
    if (rows.length === 0) return '';
    return rows.map((r, i) => `--- Example ${i + 1} ---\n${r.dialogue.slice(0, 1400)}`).join('\n');
  } catch (err) {
    console.warn('[alex] example lookup failed:', (err as Error).message);
    return '';
  }
}

// ─── Tools ───────────────────────────────────────────────────

const TOOLS = [
  {
    type: 'function',
    function: {
      name: 'send_photo',
      description:
        'Send the customer a REAL product photo from our library (previous jobs). Use when they ask to see samples, examples, pictures of a box type, material, finish or printing style. Returns what was sent.',
      parameters: {
        type: 'object',
        properties: {
          query: {
            type: 'string',
            description: 'What the photo should show, e.g. "magnetic closure rigid box black gold foil"',
          },
        },
        required: ['query'],
      },
    },
  },
  {
    type: 'function',
    function: {
      name: 'make_mockup',
      description:
        'Generate a product mockup image (AI render, ~15 s). If the customer has shared a photo/design of their box, the mockup is automatically built on THAT image (same shape, colours, logo); use it whenever they ask to see their design as a product, with inserts, in another colour, etc. Otherwise it renders from the description. Also call it IMMEDIATELY (never ask "shall I update it?") whenever the customer asks for any change to a mockup already sent (other text, colour, size, logo, finish): the previous mockup is reused so the box stays the same and only the change is applied. Write a detailed English image prompt: box type, size feel, material, colours, finish, logo/text placement, studio lighting, white background.',
      parameters: {
        type: 'object',
        properties: {
          prompt: { type: 'string', description: 'Detailed 40-90 word English image prompt' },
        },
        required: ['prompt'],
      },
    },
  },
  {
    type: 'function',
    function: {
      name: 'handover',
      description:
        'Notify the human team. Use when the customer wants an exact price/quote, places or changes an order, asks for a human, complains, or when you collected the details (dimensions, quantity, material, printing, deadline) needed for an estimate.',
      parameters: {
        type: 'object',
        properties: {
          reason: { type: 'string', description: 'quote | order | human_requested | complaint | other' },
          summary: {
            type: 'string',
            description: 'One-paragraph summary for the human agent: what the customer needs, all details collected so far, contact info if given',
          },
        },
        required: ['reason', 'summary'],
      },
    },
  },
] as const;

const escapeXml = (t: string): string => t.replace(/[<>&"']/g, (c) => ({ '<': '&lt;', '>': '&gt;', '&': '&amp;', '"': '&quot;', "'": '&apos;' })[c] as string);

/** Stamp the website watermark (bottom-right, semi-transparent) on an image buffer. */
async function watermark(buf: Buffer, text: string): Promise<{ data: Buffer; mime: string }> {
  const img = sharp(buf, { failOn: 'none' });
  const meta = await img.metadata();
  const w = meta.width ?? 1024;
  const h = meta.height ?? 1024;
  const fs = Math.max(16, Math.round(w / 24));
  const pad = Math.round(fs * 0.9);
  const svg = `<svg xmlns="http://www.w3.org/2000/svg" width="${w}" height="${h}"><text x="${w - pad}" y="${h - pad}" text-anchor="end" font-family="DejaVu Sans, Arial, sans-serif" font-weight="700" font-size="${fs}" fill="#ffffff" fill-opacity="0.9" stroke="#000000" stroke-opacity="0.45" stroke-width="${Math.max(1, Math.round(fs / 10))}" paint-order="stroke">${escapeXml(text)}</text></svg>`;
  const jpeg = meta.format === 'jpeg';
  const out = img.composite([{ input: Buffer.from(svg), top: 0, left: 0 }]);
  const data = jpeg ? await out.jpeg({ quality: 90 }).toBuffer() : await out.png().toBuffer();
  return { data, mime: jpeg ? 'image/jpeg' : 'image/png' };
}

/** The customer's most recent uploaded image in this conversation (decoded bytes), if any. */
async function recentCustomerImage(deps: AppDeps, conversationId: string): Promise<{ data: Buffer; mime: string; name: string } | null> {
  const row = await deps.db.get<{ mime: string; compressed_path: string | null; original_name: string }>(
    `SELECT f.mime, f.compressed_path, f.original_name FROM files f JOIN messages m ON m.file_id = f.id
      WHERE f.conversation_id = ? AND f.uploader_type = 'VISITOR' AND f.scan_status = 'CLEAN' AND f.mime LIKE 'image/%'
      ORDER BY m.created_at DESC LIMIT 1`,
    [conversationId],
  );
  if (!row?.compressed_path || !fs.existsSync(row.compressed_path)) return null;
  try {
    return { data: gunzipSync(fs.readFileSync(row.compressed_path)), mime: row.mime, name: row.original_name };
  } catch {
    return null;
  }
}

/** Store a library photo as a file of this conversation and post it as a FILE message. */
/** 4x RealESRGAN upscale on the pod (website photos are small); returns a JPEG or null. */
async function upscalePhoto(base: string, data: Buffer, mime: string): Promise<Buffer | null> {
  const fd = new FormData();
  fd.append('image', new Blob([data], { type: mime }), `up-${newId().slice(0, 8)}.${mime === 'image/png' ? 'png' : 'jpg'}`);
  fd.append('overwrite', 'true');
  const upRes = await fetch(`${base}/upload/image`, { method: 'POST', body: fd, signal: AbortSignal.timeout(45_000) });
  const upj = (await upRes.json()) as { name?: string; subfolder?: string };
  if (!upRes.ok || !upj.name) return null;
  const name = [upj.subfolder, upj.name].filter(Boolean).join('/');
  const prefix = `alexup-${Math.random().toString(16).slice(2, 10)}`;
  const wf: Record<string, { class_type: string; inputs: Record<string, unknown> }> = {
    '1': { class_type: 'LoadImage', inputs: { image: name } },
    '2': { class_type: 'UpscaleModelLoader', inputs: { model_name: 'RealESRGAN_x4plus.pth' } },
    '3': { class_type: 'ImageUpscaleWithModel', inputs: { upscale_model: ['2', 0], image: ['1', 0] } },
    '4': { class_type: 'ImageScaleToTotalPixels', inputs: { image: ['3', 0], upscale_method: 'lanczos', megapixels: 2.5, resolution_steps: 1 } },
    '5': { class_type: 'SaveImage', inputs: { filename_prefix: prefix, images: ['4', 0] } },
  };
  const png = await runComfy(base, wf, prefix);
  return sharp(png).jpeg({ quality: 90 }).toBuffer();
}

async function sendPhoto(deps: AppDeps, conversationId: string, query: string, wmText: string): Promise<string> {
  const terms = query
    .toLowerCase()
    .split(/[^a-z0-9]+/)
    .filter((w) => w.length > 2)
    .join(' ');
  let rows: PhotoRow[] = [];
  try {
    rows = await deps.db.all<PhotoRow>(
      `SELECT id, website, company_logo, original_name, mime, size, path, caption, tags FROM alex_photos
        WHERE is_product = 1 AND MATCH(caption, tags) AGAINST (? IN NATURAL LANGUAGE MODE)
        ORDER BY MATCH(caption, tags) AGAINST (? IN NATURAL LANGUAGE MODE) DESC, RAND()
        LIMIT 20`,
      [terms, terms],
    );
  } catch (err) {
    console.warn('[alex] photo lookup failed:', (err as Error).message);
  }
  // Add candidates that literally mention the product type (FULLTEXT ranking on a
  // long query often buries them), then let the model choose.
  const PRODUCT_WORDS = ['perfume', 'candle', 'mailer', 'rigid', 'pouch', 'label', 'cosmetic', 'soap', 'jewelry', 'jewellery', 'food', 'bakery', 'cake', 'chocolate', 'cbd', 'vape', 'shoe', 'apparel', 'tshirt', 'shirt', 'bottle', 'wine', 'tea', 'coffee', 'pizza', 'cereal', 'display', 'window', 'magnetic', 'drawer', 'sleeve', 'tuck', 'kraft', 'corrugated', 'shipping', 'gift', 'pillow', 'hexagon', 'round', 'tube', 'bag', 'subscription', 'medical', 'pharma', 'toy', 'electronics', 'phone', 'watch', 'bakery', 'cupcake', 'donut', 'burger', 'fries'];
  const q = query.toLowerCase();
  const ownHost = wmText.replace(/^https?:\/\//i, '').replace(/^www\./i, '').replace(/\/.*$/, '').toLowerCase();
  // Box styles / product types named in the request: the photo MUST show that type
  // (a "gable box" request once got a magnetic rigid box because "kraft" matched).
  const typeWords = Object.keys(PHOTO_TYPES).filter((k) => PHOTO_TYPES[k].some((syn) => new RegExp(`\\b${syn}(e?s)?\\b`, 'i').test(q)));
  const matchesType = (r: PhotoRow, k: string): boolean => {
    const text = `${r.caption} ${r.tags ?? ''}`;
    return PHOTO_TYPES[k].some((syn) => new RegExp(`\\b${syn}(e?s)?\\b`, 'i').test(text));
  };
  for (const k of typeWords.slice(0, 2)) {
    for (const syn of PHOTO_TYPES[k].slice(0, 3)) {
      try {
        const extra = await deps.db.all<PhotoRow>(
          `SELECT id, website, company_logo, original_name, mime, size, path, caption, tags FROM alex_photos
             WHERE is_product = 1 AND (caption LIKE ? OR tags LIKE ?)
             ORDER BY (website = ?) DESC, RAND() LIMIT 12`,
          [`%${syn}%`, `%${syn}%`, `site:${ownHost}`],
        );
        for (const r of extra) if (!rows.some((x) => x.id === r.id)) rows.push(r);
      } catch { /* ignore */ }
    }
  }
  const words = PRODUCT_WORDS.filter((w) => q.includes(w));
  for (const w of words.filter((w) => !typeWords.includes(w)).slice(0, 2)) {
    try {
      const extra = await deps.db.all<PhotoRow>(
        `SELECT id, website, company_logo, original_name, mime, size, path, caption, tags FROM alex_photos WHERE is_product = 1 AND (caption LIKE ? OR tags LIKE ?) ORDER BY RAND() LIMIT 5`,
        [`%${w}%`, `%${w}%`],
      );
      for (const r of extra) if (!rows.some((x) => x.id === r.id)) rows.push(r);
    } catch { /* ignore */ }
  }
  if (typeWords.length) {
    const all = rows.filter((r) => typeWords.every((k) => matchesType(r, k)));
    const any = rows.filter((r) => typeWords.some((k) => matchesType(r, k)));
    const kept = all.length ? all : any;
    alog(`photo type filter [${typeWords.join(', ')}]: ${kept.length} of ${rows.length} candidates mention the type`);
    rows = kept;
  }
  // Photos crawled from another of our websites carry that brand's name: use them only when nothing of our own matches.
  // Sites whose photos are only a fallback (ALEX_PHOTO_FALLBACK_SITES, default thecustomboxes.com); other crawled sites rank with our own library.
  const fallbackSites = (process.env.ALEX_PHOTO_FALLBACK_SITES || 'thecustomboxes.com').toLowerCase().split(',').map((h) => h.trim()).filter(Boolean);
  const isForeign = (r: PhotoRow): boolean => Boolean(r.website && r.website.startsWith('site:') && r.website !== `site:${ownHost}`);
  const isOther = (r: PhotoRow): boolean => isForeign(r) && fallbackSites.includes((r.website ?? '').slice(5));
  // Another brand's photo that carries that brand's logo/name is never shown to this website's customers.
  rows = rows.filter((r) => !(isForeign(r) && Number(r.company_logo) === 1));
  rows = [...rows.filter((r) => !isOther(r)).slice(0, 12), ...rows.filter(isOther).slice(0, 8)];
  // Let the big model confirm the candidate really shows what the customer asked for
  // (keyword search alone once returned a Red Bull rigid box for "perfume box").
  // Photos already sent in this conversation (files point at the watermarked copy photos-wm/<photo id>.gz).
  const sent = await deps.db.all<{ compressed_path: string }>(
    "SELECT compressed_path FROM files WHERE conversation_id = ? AND uploader_type = 'BOT'",
    [conversationId],
  );
  const sentIds = new Set(sent.map((f) => path.basename(f.compressed_path ?? '').replace(/\.gz$/, '')));
  const before = rows.length;
  rows = rows.filter((r) => !sentIds.has(r.id));
  if (rows.length < before) alog(`photo candidates: ${before - rows.length} already sent, ${rows.length} left`);
  const pickFrom = async (cands: PhotoRow[]): Promise<PhotoRow | null> => {
    if (cands.length === 0) return null;
    const list = cands.map((r, i) => `${i + 1}) ${r.caption.slice(0, 220)}`).join('\n');
    const pick = await chat(deps, [
      { role: 'system', content: 'You pick product photos for a packaging sales chat. Answer with ONE number only.' },
      { role: 'user', content: `Customer wants to see: "${query}"${words.length ? ` (product type keywords: ${words.join(', ')})` : ''}\nCandidate photo captions:\n${list}\n\nWhich number shows the SAME product type / box style (e.g. perfume box, mailer box, gable box, candle box, pouch) and best matches the request?${typeWords.length ? ` The caption MUST describe a ${typeWords.join(' / ')} — a different box style is NOT a match even if the colour or material fits. Prefer a photo where that style is the main or only product shown.` : ''} Reply 0 if none truly matches.` },
    ], false);
    const n = parseInt((pick.content ?? '').match(/\d+/)?.[0] ?? '0', 10);
    alog(`photo pick for "${query}": ${n} of ${cands.length}`);
    return n >= 1 && n <= cands.length ? cands[n - 1] : null;
  };
  if (rows.length > 0) {
    try {
      const primary = rows.filter((r) => !isOther(r));
      const fallback = rows.filter(isOther);
      // Captions can be wrong (a mailer box was captioned "gable box"), so when a product type
      // was named, the vision model confirms the chosen photo really shows it before it is sent.
      const confirmType = async (r: PhotoRow): Promise<boolean> => {
        if (!typeWords.length || !fs.existsSync(r.path)) return true;
        try {
          const raw: Buffer = gunzipSync(fs.readFileSync(r.path));
          const out = await chat(deps, [
            { role: 'user', content: [
              { type: 'image_url', image_url: { url: `data:${r.mime || 'image/jpeg'};base64,${raw.toString('base64')}` } },
              { type: 'text', text: `Does this photo clearly show a ${typeWords.join(' / ')} (as a packaging product type / box style)? Answer YES or NO only.` },
            ] as unknown as string },
          ], false);
          const yes = /^\s*yes/i.test(out.content ?? '');
          alog(`photo type confirm [${typeWords.join(', ')}] ${r.id}: ${yes ? 'yes' : 'NO'}`);
          return yes;
        } catch (err) {
          alog(`photo type confirm failed: ${(err as Error).message}`);
          return true;
        }
      };
      const pickConfirmed = async (cands: PhotoRow[]): Promise<PhotoRow | null> => {
        let pool = cands;
        for (let i = 0; i < 3 && pool.length; i++) {
          const c = await pickFrom(pool);
          if (!c) return null;
          if (await confirmType(c)) return c;
          pool = pool.filter((r) => r.id !== c.id);
        }
        return null;
      };
      let chosen = await pickConfirmed(primary);
      if (!chosen && fallback.length) { alog('no own photo matched, trying fallback-site photos'); chosen = await pickConfirmed(fallback); }
      rows = chosen ? [chosen] : [];
    } catch (err) {
      alog(`photo pick failed: ${(err as Error).message}`);
    }
  }
  const pick = rows.find((r) => !sentIds.has(r.id) && fs.existsSync(r.path));
  if (!pick) return 'No photo of that exact product type in the library. Say we do not have a photo of that one handy and offer a quick mockup (make_mockup) instead. Do not pretend to send one.';

  // Watermarked copy (cached per photo) so every picture carries the website name.
  let sendPath = pick.path;
  let sendMime = pick.mime;
  let sendSize = pick.size;
  try {
    const wmDir = path.join(deps.config.alex.dataDir, 'photos-wm');
    fs.mkdirSync(wmDir, { recursive: true });
    const wmPath = path.join(wmDir, `${pick.id}.gz`);
    if (!fs.existsSync(wmPath)) {
      let raw: Buffer = gunzipSync(fs.readFileSync(pick.path));
      try {
        const m = await sharp(raw, { failOn: 'none' }).metadata();
        if (Math.min(m.width ?? 0, m.height ?? 0) < 1000 && deps.config.alex.comfyUrl) {
          const up = await upscalePhoto(deps.config.alex.comfyUrl, raw, pick.mime || 'image/jpeg');
          if (up) { raw = up; alog(`photo ${pick.id} upscaled from ${m.width}x${m.height}`); }
        }
      } catch (err) {
        alog(`upscale skipped: ${(err as Error).message}`);
      }
      const { data, mime } = await watermark(raw, wmText);
      await pipeline(Readable.from(data), createGzip(), fs.createWriteStream(wmPath));
      sendMime = mime;
      sendSize = data.length;
    } else {
      sendSize = gunzipSync(fs.readFileSync(wmPath)).length;
      sendMime = pick.mime;
    }
    sendPath = wmPath;
  } catch (err) {
    alog(`watermark failed (sending original): ${(err as Error).message}`);
  }
  // Crawled photos are stored under their source URL: show the customer a clean file name instead.
  const shownName = /^https?:\/\//i.test(pick.original_name)
    ? `${decodeURIComponent(pick.original_name.split('?')[0].split('/').pop() ?? 'photo').replace(/\.(webp|png|jpe?g|gif|avif)$/i, '').replace(/[^a-z0-9-]+/gi, '-').replace(/^-|-$/g, '').toLowerCase() || 'photo'}.jpg`
    : pick.original_name;
  const fileId = newId();
  await deps.db.run(
    `INSERT INTO files (id, conversation_id, uploader_type, uploader_id, original_name, mime, size,
       scan_status, stored_path, compressed_path, created_at)
     VALUES (?, ?, 'BOT', 'alex', ?, ?, ?, 'CLEAN', NULL, ?, ?)`,
    [fileId, conversationId, shownName, sendMime, sendSize, sendPath, nowIso()],
  );
  await postMessage(deps, {
    conversationId,
    senderType: 'BOT',
    body: shownName,
    kind: 'FILE',
    fileId,
  });
  return `Photo sent to the customer: ${pick.caption}. Add one short sentence using ONLY these caption facts (no extra features), then one question about the next missing detail that you have not asked yet.`;
}

const ENHANCER_SYSTEM =
  'You write prompts for an image model that renders packaging mockups for a custom packaging company. You get the recent chat and the request; work out from the CHAT what box the customer actually wants and write ONE English prompt of 90-130 words. Rules: ' +
  '(1) The product decides the box: say the exact style and shape and describe its real geometry. Mailer box = corrugated roll-end tuck-front, wide and shallow, hinged lid; rigid box = two-piece lid and base, thick walls, matte wrapped paper, the LID is itself a shallow tray with its own side walls (rim) that sits over the base (never a flat sheet); tuck-end = slim folding carton; pouch = stand-up pouch with gusset; ' +
  'heart-shaped / round / hexagon / tube boxes keep that silhouette (e.g. "a heart-shaped two-piece rigid box: heart-shaped base tray with straight walls and a matching flat heart-shaped lid"); chocolate, candle, perfume, cosmetics, bakery etc. imply the matching box type and, if asked, inserts with the product visible. ' +
  'If the customer gave dimensions, express the proportions in words (low and flat / tall and narrow / cube-like). NEVER reuse any number or example from these instructions. ' +
  '(2) Printed text: ONLY a brand/logo name the customer explicitly gave as such (e.g. "logo that says X", "my brand X", "print X on it"), in double quotes, on the lid top and front panel, centered, as a bold flat logo. Product words (chocolate, candle, heart box...), requests, and misspellings are NOT brand text. Dimensions and numbers are NEVER printed on the box; describe size only as proportions in words. If no brand name was given, write "a small generic logo mark, no readable text". Never print slogans, taglines, website names or anything from the company side. ' +
  '(3) Colours, material and finish exactly as requested; if not stated, choose what suits the product (e.g. deep red matte rigid for a Valentine chocolate box), not plain white by default. ' +
  '(3b) DEFAULT: the box is shown CLOSED with its lid on (a clean product shot) and NOTHING about the contents or the product inside is mentioned (no candles, chocolates, bottles) — only the box exterior. Only when the customer explicitly asks to see inside, open, the insert or the contents, describe the lid of that SAME box taken off and resting on the table beside or behind its base, printed top face up towards the camera, with its own side walls/rim like a real lid (never floating in the air, never upside down) — the insert and products neatly visible inside, the silhouette unchanged and fully recognisable. Exactly ONE box in the picture, never a second smaller box. ' +
  '(4) Scene: one box on a seamless light grey studio backdrop, 3/4 angle slightly from above, soft diffused studio light, soft shadow, 85mm lens, photorealistic, sharp, no people, no hands, no extra props, no other boxes. ' +
  'Output only the prompt.';

const SHEET_ENHANCER_SYSTEM =
  'You write prompts for an image model that renders a packaging PRESENTATION SHEET (one image laid out as a grid of five photos) for a custom packaging company. You get the recent chat and the request; work out from the CHAT what box the customer actually wants, then write ONE English prompt of 140-200 words that follows this structure exactly: ' +
  '"Professional packaging presentation sheet for a <product> box, one image laid out as a grid of five photos with thin white gutters. Large main photo top-left: a closed <full box description: style and real geometry, proportions in words, material, colour, finish, decorative accents, logo or brand text and where it sits> standing on <scene>. Top-right photo: the same box open <how this style opens>, showing <fitted insert and the product> inside. Bottom row, three smaller photos of the same closed box: seen straight from the front, from a three-quarter angle, and from directly above. Every panel shows the identical box and design. Photorealistic product photography, <lighting>, sharp." ' +
  'Rules: (1) The product decides the style: perfume, cosmetics, jewellery, watches, premium gifts = rigid box with a full-depth magnetic lid or a hinged book-style lid (thick walls, matte wrapped paper, optionally a thin gold or silver foil border line on the front); chocolate or sweets = rigid two-piece box with a tray insert; candle = rigid two-piece or tuck-top box; mailer = corrugated roll-end tuck-front box with a hinged lid; tuck-end = slim folding carton; pouch = stand-up pouch; heart, round, hexagon or tube boxes: the box AND its lid keep that silhouette in all five photos (the open view shows the same shaped lid lifted off and resting beside the shaped base, never a hinged rectangular box). ' +
  'Dimensions: describe the proportions in words only (wider than tall, slim depth, cube-like, tall and narrow...); never write numbers, units, dimension arrows or labels anywhere in the prompt. The logo is the brand name alone: no emblem, monogram or initial above or beside it. ' +
  '(2) Printed text: ONLY a brand or logo name the customer explicitly gave as such, written in double quotes, as a foil or printed logo centered on the lid or front panel; product words, requests and misspellings are not brand text. If no brand name was given, write "a small generic logo mark, no readable text". Never slogans, taglines, website names or anything from the company side, and no other text anywhere. ' +
  '(3) Colours, material and finish exactly as requested; if not stated, choose what suits the product. The scene suits the product (cream marble table with blurred gold decor for perfume or cosmetics, rustic wood for candles, clean white studio for mailers). ' +
  '(4) The open view shows the matching product inside (a perfume bottle, chocolates, a candle, the item they pack) sitting in a fitted insert; no people, no hands. ' +
  '(5) If the customer asked for a window (die-cut window, clear window, PVC window), it is part of the box in EVERY closed view: name its shape (round, square, rectangular, heart, oval) and position on the front panel or lid, with the product visible through it; never leave it out. Output only the prompt.';

async function enhanceMockupPrompt(deps: AppDeps, desc: string, convoText: string, sheet = false): Promise<string> {
  try {
    const out = await chat(deps, [
      { role: 'system', content: sheet ? SHEET_ENHANCER_SYSTEM : ENHANCER_SYSTEM },
      { role: 'user', content: `Recent chat:\n${convoText.slice(-2500)}\n\nMockup request / tool description: ${desc}` },
    ], false);
    const text = out.content?.trim().replace(/^["']|["']$/g, '') ?? '';
    if (text.length > 60) return text;
  } catch (err) {
    alog(`enhancer failed: ${(err as Error).message}`);
  }
  return `Professional product photography of custom printed packaging: ${desc}. One box on a seamless light grey studio backdrop, 3/4 angle, soft studio light, photorealistic, no people, no hands.`;
}

/** Generate a FLUX.2 mockup on ComfyUI, store it and post it as a FILE message. */
interface ComfyImage { filename: string; subfolder: string; type: string }

/** Queue a workflow, wait for its SaveImage output and return the PNG bytes. */
async function runComfy(base: string, workflow: Record<string, { class_type: string; inputs: Record<string, unknown> }>, prefix: string): Promise<Buffer> {
  const queued = await fetch(`${base}/prompt`, {
    method: 'POST',
    signal: AbortSignal.timeout(30_000),
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ prompt: workflow, client_id: newId() }),
  });
  if (!queued.ok) throw new Error(`ComfyUI /prompt ${queued.status}`);
  const { prompt_id: promptId } = (await queued.json()) as { prompt_id: string };
  let image: ComfyImage | null = null;
  const deadline = Date.now() + 120_000;
  while (Date.now() < deadline && !image) {
    await sleep(1500);
    const h = await fetch(`${base}/history/${promptId}`, { signal: AbortSignal.timeout(15_000) });
    if (!h.ok) continue;
    const data = (await h.json()) as Record<string, { outputs?: Record<string, { images?: ComfyImage[] }> }>;
    const outputs = data[promptId]?.outputs ?? {};
    for (const node of Object.values(outputs)) {
      const img = node.images?.find((i: ComfyImage) => i && i.filename.startsWith(prefix));
      if (img) { image = img; break; }
    }
  }
  if (!image) throw new Error('ComfyUI timed out');
  const view = new URL(`${base}/view`);
  view.searchParams.set('filename', image.filename);
  view.searchParams.set('subfolder', image.subfolder || '');
  view.searchParams.set('type', image.type || 'output');
  const res = await fetch(view, { signal: AbortSignal.timeout(60_000) });
  if (!res.ok) throw new Error(`ComfyUI /view ${res.status}`);
  return Buffer.from(await res.arrayBuffer());
}

/** Vision QA (structured): silhouette, colour, text, one box / one lid, lid state. */
async function checkRender(deps: AppDeps, png: Buffer, expectedText: string[], summary: string, openRequested = false, sheet = false, shapeWord?: string): Promise<{ ok: boolean; why: string }> {
  if (sheet) return checkSheet(deps, png, expectedText, summary, shapeWord);
  const out = await chat(deps, [
    { role: 'system', content: 'You are a strict QA checker for packaging mockup renders. Reply with ONLY a JSON object, no prose.' },
    { role: 'user', content: [
      { type: 'image_url', image_url: { url: `data:image/png;base64,${png.toString('base64')}` } },
      { type: 'text', text:
        `Requested mockup: ${summary.slice(0, 700)}\n` +
        `Judge the image and return JSON with these keys:\n` +
        `"silhouette_ok": true/false — the box outline matches the requested shape (heart = heart outline; polygons: count the sides, hexagon 6, octagon 8; mailer/rigid = rectangular). Construction subtype, flute texture and closure style do NOT matter.\n` +
        `"colour_ok": true/false — main colour/finish as requested.\n` +
        `"text_ok": true/false/null — ${expectedText.length ? `the printed brand text reads exactly ${expectedText.map((t) => `"${t}"`).join(' and ')} (letter case/font do not matter; wrong, missing or extra letters = false)` : 'null if no brand text was requested; false if random readable or garbled text appears'}.\n` +
        `"boxes": number of separate boxes (base trays) visible. A detached lid lying beside its base is part of the SAME box, not a second box — even if the lid has side walls.\n` +
        `"lids": number of lids visible (one detached lid beside one base = 1 lid, 1 box).\n` +
        `"lid_state": one of "closed_flush" (lid on, fits the base, no gap; a closed mailer/tuck box with its front tab tucked in also counts), "closed_shoulder" (lid fully seated on a base that shows a rim/neck/shoulder below the lid — a normal closed shoulder box), "closed_ajar" (lid on but partly open/tilted or a flap open), "off_resting" (lid removed and resting on the table or leaning on the box, top face up), "floating" (lid in mid-air / not touching anything), "flat_sheet" (the lid is a flat panel with no side walls), "none".\n` +
        `"defects": short note on impossible geometry, duplicated parts, or none.\n` +
        `Ignore the website watermark at the bottom.` },
    ] as unknown as string },
  ], false);
  const raw = (out.content ?? '').trim();
  let j: Record<string, unknown> = {};
  const candidates = [raw.replace(/```(?:json)?/gi, '').trim(), (raw.match(/\{[\s\S]*\}/) ?? [''])[0]];
  for (const c of candidates) {
    if (!c) continue;
    try { j = JSON.parse(c) as Record<string, unknown>; break; } catch { /* try next */ }
    try { j = JSON.parse(c.replace(/,\s*([}\]])/g, '$1').replace(/\/\/[^\n]*/g, '')) as Record<string, unknown>; break; } catch { /* try next */ }
  }
  if (Object.keys(j).length === 0) return { ok: /^\s*yes/i.test(raw), why: raw.slice(0, 120) };
  const lid = String(j.lid_state ?? 'none');
  const problems: string[] = [];
  if (j.silhouette_ok === false) problems.push('wrong box shape');
  if (j.colour_ok === false) problems.push('wrong colour');
  if (j.text_ok === false) problems.push(expectedText.length ? 'brand text misspelled' : 'unwanted text on the box');
  if (Number(j.boxes ?? 1) > 1) problems.push('more than one box');
  if (Number(j.lids ?? 1) > 1) problems.push('more than one lid');
  if (lid === 'floating') problems.push('lid floating in mid-air');
  if (lid === 'flat_sheet') problems.push('lid is a flat sheet without side walls');
  if (!openRequested && lid !== 'closed_flush' && lid !== 'closed_shoulder' && lid !== 'none') problems.push('box must be fully closed with the lid seated on the base');
  if (openRequested && (lid === 'closed_flush' || lid === 'closed_shoulder' || lid === 'closed_ajar')) problems.push('box must be shown open with the lid off, resting beside the base');
  const defects = String(j.defects ?? '').trim();
  if (defects && !/^(none|no|n\/a|-)/i.test(defects)) problems.push(defects.slice(0, 80));
  return { ok: problems.length === 0, why: problems.length ? problems.join('; ') : 'ok' };
}

function parseQaJson(raw: string): Record<string, unknown> {
  let j: Record<string, unknown> = {};
  const candidates = [raw.replace(/```(?:json)?/gi, '').trim(), (raw.match(/\{[\s\S]*\}/) ?? [''])[0]];
  for (const c of candidates) {
    if (!c) continue;
    try { j = JSON.parse(c) as Record<string, unknown>; break; } catch { /* try next */ }
    try { j = JSON.parse(c.replace(/,\s*([}\]])/g, '$1').replace(/\/\/[^\n]*/g, '')) as Record<string, unknown>; break; } catch { /* try next */ }
  }
  return j;
}

/** QA for a presentation sheet: several panels of ONE box design (main closed view, open view with product, side views). */
// Words the vision model may use for each requested special shape.
const SHAPE_SYNONYMS: Record<string, string[]> = {
  heart: ['heart'], round: ['round', 'circle', 'circular', 'cylinder', 'tube'], circular: ['round', 'circle', 'circular', 'cylinder', 'tube'], circle: ['round', 'circle', 'circular', 'cylinder', 'tube'],
  tube: ['tube', 'cylinder', 'round', 'circle', 'circular'], cylinder: ['tube', 'cylinder', 'round', 'circle', 'circular'], cylindrical: ['tube', 'cylinder', 'round', 'circle', 'circular'],
  hexagon: ['hexagon', 'hexagonal'], hexagonal: ['hexagon', 'hexagonal'], octagon: ['octagon', 'octagonal'], octagonal: ['octagon', 'octagonal'],
  triangle: ['triangle', 'triangular'], triangular: ['triangle', 'triangular'], pyramid: ['pyramid', 'triangle', 'triangular'],
  pillow: ['pillow'], oval: ['oval', 'ellipse', 'elliptical'], star: ['star'], diamond: ['diamond', 'rhombus'],
};
/** Panel rectangles of a presentation sheet, found from its white gutters (rows/columns that are almost entirely white). */
async function sheetPanels(png: Buffer): Promise<{ x: number; y: number; w: number; h: number }[]> {
  const { data, info } = await sharp(png).greyscale().raw().toBuffer({ resolveWithObject: true });
  const W = info.width, H = info.height;
  const white = (v: number) => v > 232;
  const runs = (flags: boolean[], minLen: number): { a: number; b: number }[] => {
    const out: { a: number; b: number }[] = [];
    let start = -1;
    for (let i = 0; i <= flags.length; i++) {
      const on = i < flags.length && !flags[i];
      if (on && start < 0) start = i;
      if (!on && start >= 0) { if (i - start >= minLen) out.push({ a: start, b: i }); start = -1; }
    }
    return out;
  };
  const rowGutter: boolean[] = new Array(H).fill(false);
  for (let y = 0; y < H; y++) { let c = 0; for (let x = 0; x < W; x++) if (white(data[y * W + x])) c++; rowGutter[y] = c / W > 0.96; }
  const panels: { x: number; y: number; w: number; h: number }[] = [];
  for (const band of runs(rowGutter, Math.round(H * 0.12))) {
    const colGutter: boolean[] = new Array(W).fill(false);
    for (let x = 0; x < W; x++) { let c = 0; for (let y = band.a; y < band.b; y++) if (white(data[y * W + x])) c++; colGutter[x] = c / (band.b - band.a) > 0.96; }
    for (const col of runs(colGutter, Math.round(W * 0.12))) panels.push({ x: col.a, y: band.a, w: col.b - col.a, h: band.b - band.a });
  }
  return panels;
}
const SHAPE_QUESTION = 'name the outline shape of the box body as seen from above (its footprint), ignoring the lid: one of heart, round, hexagon, octagon, triangle, square, rectangle, oval, star, diamond, pillow, tube, pyramid, other. A box whose base has four corners is "square" or "rectangle" even if its lid is pointed.';
/** Outline shape of the single box in one photo plus every piece of text printed on it (vision model). */
async function singlePhotoCheck(deps: AppDeps, img: Buffer, mime = 'image/png'): Promise<{ shape: string; text: string }> {
  const out = await chat(deps, [
    { role: 'system', content: 'You describe packaging photos precisely. Reply with ONLY a JSON object, no prose.' },
    { role: 'user', content: [
      { type: 'image_url', image_url: { url: `data:${mime};base64,${img.toString('base64')}` } },
      { type: 'text', text: `For this photo of a box, ${SHAPE_QUESTION} Also transcribe ALL text, letters, initials or monogram letters printed on the box or inside it (not the website watermark), exactly as written, or "" if none. Return {"shape":"...","text":"..."}.` },
    ] as unknown as string },
  ], false);
  const j = parseQaJson((out.content ?? '').trim()) as { shape?: string; text?: string };
  return { shape: String(j.shape ?? '').toLowerCase(), text: String(j.text ?? '').trim() };
}
/** Per-panel shape report for a sheet: panel rectangles (from the white gutters), the shape seen in each, and the indexes that are not the requested shape. */
async function panelShapeReport(deps: AppDeps, png: Buffer, shapeWord: string): Promise<{ panels: { x: number; y: number; w: number; h: number }[]; shapes: string[]; bad: number[] } | null> {
  const ok = SHAPE_SYNONYMS[shapeWord.toLowerCase()] ?? [shapeWord.toLowerCase()];
  const panels = await sheetPanels(png).catch(() => []);
  if (panels.length < 3 || panels.length > 6) { alog(`sheet panels not detected (${panels.length})`); return null; }
  const content: unknown[] = [];
  for (let i = 0; i < panels.length; i++) {
    const crop = await sharp(png).extract({ left: panels[i].x, top: panels[i].y, width: panels[i].w, height: panels[i].h }).resize({ width: 640, withoutEnlargement: true }).jpeg({ quality: 85 }).toBuffer();
    content.push({ type: 'text', text: `Photo ${i + 1}:` });
    content.push({ type: 'image_url', image_url: { url: `data:image/jpeg;base64,${crop.toString('base64')}` } });
  }
  content.push({ type: 'text', text: `These are ${panels.length} separate photos of a box (some closed, one may be open with the lid off or lifted). For EACH photo ${SHAPE_QUESTION} Return ONLY {"shapes":["...", ...]} with exactly ${panels.length} entries in order.` });
  const out = await chat(deps, [
    { role: 'system', content: 'You describe packaging photos precisely. Reply with ONLY a JSON object, no prose.' },
    { role: 'user', content: content as unknown as string },
  ], false);
  const j = parseQaJson((out.content ?? '').trim()) as { shapes?: string[] };
  if (!Array.isArray(j.shapes) || j.shapes.length !== panels.length) { alog(`panel crop check unusable (${panels.length} panels, shapes=${JSON.stringify(j.shapes ?? null)})`); return null; }
  const shapes = j.shapes.map((sh) => String(sh).toLowerCase());
  alog(`panel shapes (crops) for ${shapeWord}: ${shapes.join(', ')}`);
  const bad = shapes.map((sh, i) => (ok.some((w) => sh.includes(w)) ? -1 : i)).filter((i) => i >= 0);
  return { panels, shapes, bad };
}
/** Second, per-panel QA for special shapes: the global "same_box" judgement misses one odd panel (a triangle sheet once passed with a rectangular open view). */
async function checkPanelShapes(deps: AppDeps, png: Buffer, shapeWord: string): Promise<string | null> {
  const report = await panelShapeReport(deps, png, shapeWord).catch((err) => { alog(`panel report failed: ${(err as Error).message}`); return null; });
  if (report) {
    if (report.bad.length === 0) return null;
    return `panel(s) ${report.bad.map((i) => `${i + 1} (${report.shapes[i]})`).join(', ')} show a box that is not ${shapeWord}-shaped; every panel, including the open view, must show a ${shapeWord}-shaped base and lid`;
  }
  const ok = SHAPE_SYNONYMS[shapeWord.toLowerCase()] ?? [shapeWord.toLowerCase()];
  const panels: { x: number; y: number; w: number; h: number }[] = [];
  if (panels.length >= 3 && panels.length <= 6) {
    const content: unknown[] = [];
    for (let i = 0; i < panels.length; i++) {
      const crop = await sharp(png).extract({ left: panels[i].x, top: panels[i].y, width: panels[i].w, height: panels[i].h }).resize({ width: 640, withoutEnlargement: true }).jpeg({ quality: 85 }).toBuffer();
      content.push({ type: 'text', text: `Photo ${i + 1}:` });
      content.push({ type: 'image_url', image_url: { url: `data:image/jpeg;base64,${crop.toString('base64')}` } });
    }
    content.push({ type: 'text', text:
      `These are ${panels.length} separate photos of a box (some closed, one may be open with the lid off or lifted). For EACH photo name the outline shape of the box body as seen from above (its footprint), ignoring the lid: one of heart, round, hexagon, octagon, triangle, square, rectangle, oval, star, diamond, pillow, tube, pyramid, other. ` +
      `A box whose base has four corners is "square" or "rectangle" even if its lid is pointed. Return ONLY {"shapes":["...", ...]} with exactly ${panels.length} entries in order.` });
    const out = await chat(deps, [
      { role: 'system', content: 'You describe packaging photos precisely. Reply with ONLY a JSON object, no prose.' },
      { role: 'user', content: content as unknown as string },
    ], false);
    const j = parseQaJson((out.content ?? '').trim()) as { shapes?: string[] };
    if (Array.isArray(j.shapes) && j.shapes.length === panels.length) {
      alog(`panel shapes (crops) for ${shapeWord}: ${j.shapes.join(', ')}`);
      const bad = j.shapes.map((sh, i) => ({ sh: String(sh).toLowerCase(), i })).filter((e) => !ok.some((w) => e.sh.includes(w)));
      if (bad.length === 0) return null;
      return `panel(s) ${bad.map((e) => `${e.i + 1} (${e.sh})`).join(', ')} show a box that is not ${shapeWord}-shaped; every panel, including the open view, must show a ${shapeWord}-shaped base and lid`;
    }
    alog(`panel crop check unusable (${panels.length} panels, shapes=${JSON.stringify(j.shapes ?? null)}), falling back to grid check`);
  } else {
    alog(`sheet panels not detected (${panels.length}), falling back to grid check`);
  }
  const out = await chat(deps, [
    { role: 'system', content: 'You describe packaging photos precisely. Reply with ONLY a JSON object, no prose.' },
    { role: 'user', content: [
      { type: 'image_url', image_url: { url: `data:image/png;base64,${png.toString('base64')}` } },
      { type: 'text', text:
        'This image is a grid of 5 photos of a box: top row = one large photo on the left and one photo on the right (the box open, lid off); bottom row = three small photos. ' +
        'For EACH of the 5 panels name the outline shape of the box body (and its lid, if visible) as seen from above: one of heart, round, hexagon, octagon, triangle, square, rectangle, oval, star, diamond, pillow, tube, pyramid, other. ' +
        'Look at the actual outline in that panel, not at the other panels. Return {"panels":[{"pos":"top-left","shape":"..."},{"pos":"top-right","shape":"..."},{"pos":"bottom-left","shape":"..."},{"pos":"bottom-middle","shape":"..."},{"pos":"bottom-right","shape":"..."}]}' },
    ] as unknown as string },
  ], false);
  const j = parseQaJson((out.content ?? '').trim()) as { panels?: { pos?: string; shape?: string }[] };
  if (!Array.isArray(j.panels) || j.panels.length === 0) return null;
  const bad = j.panels.filter((pn) => pn.shape && !ok.some((w) => String(pn.shape).toLowerCase().includes(w)));
  alog(`panel shapes for ${shapeWord}: ${j.panels.map((pn) => `${pn.pos}=${pn.shape}`).join(', ')}`);
  if (bad.length === 0) return null;
  return `panel(s) ${bad.map((pn) => `${pn.pos} (${pn.shape})`).join(', ')} do not show a ${shapeWord}-shaped box; every panel, including the open view, must show the ${shapeWord} shape`;
}
async function checkSheet(deps: AppDeps, png: Buffer, expectedText: string[], summary: string, shapeWord?: string): Promise<{ ok: boolean; why: string }> {
  const out = await chat(deps, [
    { role: 'system', content: 'You are a strict QA checker for packaging presentation sheets. Reply with ONLY a JSON object, no prose.' },
    { role: 'user', content: [
      { type: 'image_url', image_url: { url: `data:image/png;base64,${png.toString('base64')}` } },
      { type: 'text', text:
        `Requested sheet: ${summary.slice(0, 900)}\n` +
        `This image is a grid of several photos of ONE box design. Judge it and return JSON with these keys:\n` +
        `"same_box": true/false — every panel shows the same box (same shape, colour, lid style, logo); a panel showing a clearly different box = false.\n` +
        `"main_closed": true/false — the large main photo shows the box fully closed with its lid seated (no gap, nothing floating or ajar).\n` +
        `"silhouette_ok": true/false — in the main photo the box outline matches the requested shape (heart = heart outline; polygons: count the sides; rectangular otherwise).\n` +
        `"panels_wrong_shape": number of panels (out of all panels, including the open view) where the box or its lid does NOT have the requested shape (e.g. a rectangular box in a heart-box sheet).\n` +
        `"colour_ok": true/false — main colour/finish as requested.\n` +
        `"text_ok": true/false/null — ${expectedText.length ? `the printed brand text on the box reads exactly ${expectedText.map((t) => `"${t}"`).join(' and ')} (case/font do not matter; wrong, missing, extra or garbled letters anywhere on the box or inside it = false)` : 'null if no brand text was requested; false if random readable or garbled text appears on the box or inside it'}. Dimension labels beside the box do not count.\n` +
        `"extra_text": true/false — true if any other text, numbers, dimension arrows/labels, monograms, initials or emblems appear on the box, inside it or anywhere in the image (besides the brand name and the website watermark).\n` +
        `"window_ok": true/false/null — null if the request mentions no window; true if the requested window (right shape) is visible on the closed box in the main photo; false if it is missing or a different shape.\n` +
        `"defects": short note on impossible geometry, duplicated or broken parts, people or hands, or none.\n` +
        `Ignore the website watermark at the bottom.` },
    ] as unknown as string },
  ], false);
  const raw = (out.content ?? '').trim();
  const j = parseQaJson(raw);
  if (Object.keys(j).length === 0) return { ok: /^\s*yes/i.test(raw), why: raw.slice(0, 120) };
  const problems: string[] = [];
  if (j.same_box === false) problems.push('every panel must show the identical box design');
  if (Number(j.panels_wrong_shape ?? 0) > 0) problems.push('every panel, including the open view, must show the box and its lid in the requested shape');
  if (j.main_closed === false) problems.push('the main photo must show the box fully closed with the lid seated');
  if (j.silhouette_ok === false) problems.push('wrong box shape');
  if (j.colour_ok === false) problems.push('wrong colour');
  if (j.text_ok === false) problems.push(expectedText.length ? 'brand text misspelled or extra text' : 'unwanted text on the box');
  if (j.extra_text === true) problems.push('the brand name is the only text; no numbers, labels, monograms or emblems anywhere');
  if (j.window_ok === false) problems.push('the closed box must show the requested window with the product visible through it');
  const defects = String(j.defects ?? '').trim();
  if (defects && !/^(none|no|n\/a|-)/i.test(defects)) problems.push(defects.slice(0, 80));
  if (shapeWord) {
    const bad = await checkPanelShapes(deps, png, shapeWord).catch((err) => { alog(`panel shape check failed: ${(err as Error).message}`); return null; });
    if (bad) problems.push(bad);
  }
  return { ok: problems.length === 0, why: problems.length ? problems.join('; ') : 'ok' };
}

const DIMS_RE = /(\d+(?:\.\d+)?)\s*(?:"|''|in(?:ch(?:es)?)?|cm|mm)?\s*[x×*]\s*(\d+(?:\.\d+)?)\s*(?:"|''|in(?:ch(?:es)?)?|cm|mm)?\s*[x×*]\s*(\d+(?:\.\d+)?)\s*("|''|in(?:ch(?:es)?)?|cm|mm)?/i;
/** "5x2x4 inches" in the customer's words → { l, w, h, unit } (null if no size was given). */
function sizePartsFrom(text: string): { l: string; w: string; h: string; unit: string } | null {
  const m = text.match(DIMS_RE);
  if (!m) return null;
  const unitRaw = (m[4] ?? '').toLowerCase();
  const unit = unitRaw.startsWith('cm') ? 'cm' : unitRaw.startsWith('mm') ? 'mm' : 'in';
  return { l: m[1], w: m[2], h: m[3], unit };
}
function sizeLabelFrom(text: string): string | null {
  const d = sizePartsFrom(text);
  return d ? `Size: ${d.l} × ${d.w} × ${d.h} ${d.unit}` : null;
}
/** Ask the vision model where the closed box is in the main (top-left) panel; Qwen-VL answers in a 0-1000 grid. */
async function locateMainBox(deps: AppDeps, png: Buffer, W: number, H: number): Promise<[number, number, number, number] | null> {
  try {
    const out = await chat(deps, [
      { role: 'user', content: [
        { type: 'image_url', image_url: { url: `data:image/png;base64,${png.toString('base64')}` } },
        { type: 'text', text: 'Locate the closed gift box (only the box itself, not the table, shadow or decor) in the LARGE top-left photo. Output ONLY JSON {"bbox_2d":[x1,y1,x2,y2]}.' },
      ] as unknown as string },
    ], false);
    const raw = out.content ?? '';
    const m = raw.match(/"bbox_2d"\s*:\s*\[\s*(\d+)\s*,\s*(\d+)\s*,\s*(\d+)\s*,\s*(\d+)\s*\]/);
    if (!m) { alog(`locate box: no bbox in ${raw.slice(0, 80)}`); return null; }
    const [x1, y1, x2, y2] = [Number(m[1]) * W / 1000, Number(m[2]) * H / 1000, Number(m[3]) * W / 1000, Number(m[4]) * H / 1000];
    // Must sit inside the main (top-left) panel and be a real box, not a sliver.
    if (x2 > W * 0.58 || y2 > H * 0.66 || x2 - x1 < W * 0.08 || y2 - y1 < H * 0.08) { alog(`locate box: implausible bbox ${[x1, y1, x2, y2].map(Math.round).join(',')}`); return null; }
    return [x1, y1, x2, y2];
  } catch (err) {
    alog(`locate box failed: ${(err as Error).message}`);
    return null;
  }
}
/** Draw clean dimension lines (length below, height on the right, depth receding from the corner) with labels around the box. */
async function drawDimensions(buf: Buffer, bb: [number, number, number, number], dims: { l: string; w: string; h: string; unit: string }, W: number, H: number): Promise<Buffer> {
  const [x1, y1, x2, y2] = bb;
  const fs = Math.round(W / 52);
  const gap = Math.round(W / 70);
  const tick = Math.round(fs * 0.45);
  const stroke = Math.max(2, Math.round(W / 480));
  const line = (ax: number, ay: number, bx: number, by: number): string => {
    const dx = bx - ax, dy = by - ay, len = Math.hypot(dx, dy) || 1, nx = -dy / len * tick, ny = dx / len * tick;
    return `<g stroke="#ffffff" stroke-width="${stroke}" stroke-linecap="round" filter="url(#sh)"><line x1="${ax}" y1="${ay}" x2="${bx}" y2="${by}"/><line x1="${ax + nx}" y1="${ay + ny}" x2="${ax - nx}" y2="${ay - ny}"/><line x1="${bx + nx}" y1="${by + ny}" x2="${bx - nx}" y2="${by - ny}"/></g>`;
  };
  const label = (cx: number, cy: number, t: string): string => {
    const bw = Math.round(t.length * fs * 0.62) + fs, bh = Math.round(fs * 1.55);
    return `<rect x="${cx - bw / 2}" y="${cy - bh / 2}" width="${bw}" height="${bh}" rx="${bh / 3}" fill="rgba(0,0,0,0.62)"/><text x="${cx}" y="${cy + fs * 0.36}" text-anchor="middle" font-family="DejaVu Sans, Arial, sans-serif" font-size="${fs}" font-weight="bold" fill="#ffffff">${escapeXml(t)}</text>`;
  };
  const u = dims.unit;
  const parts: string[] = [];
  // length: under the box
  const ly = Math.min(H - fs * 2, y2 + gap);
  parts.push(line(x1, ly, x2, ly), label((x1 + x2) / 2, ly + fs * 1.3, `${dims.l} ${u}`));
  // height: right of the box
  const hx = Math.min(W * 0.62, x2 + gap);
  parts.push(line(hx, y1, hx, y2), label(hx + fs * 2.6, (y1 + y2) / 2, `${dims.h} ${u}`));
  // depth: short receding line from the bottom-right corner
  const panelRight = W * 0.5 - gap * 2; // the main photo ends at the vertical gutter
  const ratio = Math.min(0.6, Math.max(0.2, (Number(dims.w) || 1) / (Number(dims.l) || 1)));
  const dx0 = x1 - gap * 0.4, dy0 = ly + gap * 0.4;
  let d = Math.min((x2 - x1) * ratio * 0.6, W * 0.11);
  const dx1 = dx0 - d * Math.cos(Math.PI / 6), dy1 = dy0 + d * Math.sin(Math.PI / 6);
  if (dx1 > fs && dy1 < H * 0.64 && hx + fs * 4 < panelRight) {
    parts.push(line(dx0, dy0, dx1, dy1), label(Math.max(fs * 2.2, dx1 - fs * 0.4), dy1 + fs * 1.2, `${dims.w} ${u}`));
  } else {
    d = 0;
    parts.push(label(x1 + fs * 2.2, y1 - fs * 1.1, `depth ${dims.w} ${u}`));
  }
  const svg = `<svg xmlns="http://www.w3.org/2000/svg" width="${W}" height="${H}"><defs><filter id="sh" x="-20%" y="-20%" width="140%" height="140%"><feDropShadow dx="0" dy="0" stdDeviation="1.2" flood-color="#000" flood-opacity="0.8"/></filter></defs>${parts.join('')}</svg>`;
  return sharp(buf).composite([{ input: Buffer.from(svg), top: 0, left: 0 }]).png().toBuffer();
}
async function sizeBadge(buf: Buffer, label: string): Promise<Buffer> {
  const meta = await sharp(buf, { failOn: 'none' }).metadata();
  const W = meta.width ?? 1344;
  const fs = Math.round(W / 40);
  const padX = Math.round(fs * 0.8);
  const bw = Math.round(label.length * fs * 0.6) + padX * 2;
  const bh = Math.round(fs * 1.9);
  const svg = `<svg xmlns="http://www.w3.org/2000/svg" width="${bw}" height="${bh}"><rect x="0" y="0" width="${bw}" height="${bh}" rx="${Math.round(bh / 4)}" fill="rgba(0,0,0,0.62)"/><text x="${padX}" y="${Math.round(bh * 0.68)}" font-family="DejaVu Sans, Arial, sans-serif" font-size="${fs}" font-weight="bold" fill="#ffffff">${escapeXml(label)}</text></svg>`;
  return sharp(buf).composite([{ input: Buffer.from(svg), top: Math.round(W / 70), left: Math.round(W / 70) }]).png().toBuffer();
}

/** Last mockup per conversation, so a change request re-renders the SAME box (same prompt + seed, only the change applied). */
const lastMockups = new Map<string, { prompt: string; seed: number; at: number }>();
const NEW_BOX_RE = /\b(another|new|second|other|separate|next) (box|product|packaging|design|item|mockup)\b|\bstart (over|fresh|again)\b|\bfrom scratch\b|\bforget (that|the previous)\b/i;
// Wait lines used when the customer asked for a photo we do not have: the picture is rendered, but never called a mockup.
const PHOTO_WAIT_MSGS = [
  'One moment, let me get a picture of that for you, it can take a minute or two.',
  'Sure, pulling up a picture for you, give me a minute.',
  'Let me grab a picture of that, one minute please.',
];
const photoFallback = new Set<string>();
const MOCKUP_WAIT_MSGS = [
  'Sure, creating your mockup now, it can take a minute or two.',
  'On it, your mockup is being generated, give me a moment.',
  'Working on the updated mockup now, one minute please.',
  'Let me render that for you, it takes around a minute.',
];

async function reviseMockupPrompt(deps: AppDeps, prevPrompt: string, change: string, desc: string): Promise<string | null> {
  try {
    const out = await chat(deps, [
      { role: 'system', content: 'You edit prompts for an image generator. You get the prompt that produced the customer\'s previous packaging mockup and the change they now ask for. Output the SAME prompt with ONLY that change applied: keep every other sentence, word and the sentence order identical (same box style, shape, proportions, colours, finish, scene, closed/open state), so the new picture matches the previous one. Printed text rule: the only printed text is a brand name the customer explicitly asked to print, written in double quotes exactly as they wrote it, replacing any earlier brand text or logo completely (the old letter/monogram disappears; no other letters, initials or marks remain anywhere on the box); never print dimensions or numbers on the box itself (dimension arrow labels beside the box in a presentation-sheet prompt stay exactly as they are), no slogans or website names. Output only the full edited prompt, nothing else.' },
      { role: 'user', content: `PREVIOUS PROMPT:\n${prevPrompt}\n\nCUSTOMER'S CHANGE REQUEST:\n${change}\n(tool description: ${desc})` },
    ], false);
    const text = out.content?.trim().replace(/^["']|["']$/g, '') ?? '';
    if (text.length > 60 && text.length > prevPrompt.length * 0.5 && text.length < prevPrompt.length * 1.8) return text;
    alog(`revision prompt rejected (len ${text.length} vs ${prevPrompt.length})`);
  } catch (err) {
    alog(`revision prompt failed: ${(err as Error).message}`);
  }
  return null;
}

async function makeMockup(deps: AppDeps, conversationId: string, rawPrompt: string, wmText: string, convoText: string, asPhoto = false): Promise<string> {
  const base = deps.config.alex.comfyUrl;
  if (!base) return 'Mockup generation is not available right now. Describe it in words instead.';
  const desc = rawPrompt.trim().length >= 15 ? rawPrompt.trim() : 'the box the customer described in the chat';
  const ref = await recentCustomerImage(deps, conversationId);
  // Tell the customer right away that the render is in progress (it takes 20-70 s).
  {
    const sent = await deps.db.get<{ n: number }>("SELECT COUNT(*) AS n FROM messages WHERE conversation_id = ? AND sender_type = 'BOT' AND kind = 'FILE'", [conversationId]);
    const wait = asPhoto ? PHOTO_WAIT_MSGS[Number(sent?.n ?? 0) % PHOTO_WAIT_MSGS.length] : MOCKUP_WAIT_MSGS[Number(sent?.n ?? 0) % MOCKUP_WAIT_MSGS.length];
    await postMessage(deps, { conversationId, senderType: 'BOT', body: wait }).catch(() => undefined);
  }
  const engine = (process.env.ALEX_IMAGE_ENGINE || 'klein').toLowerCase() === 'qwen' ? 'qwen' : 'klein';
  // Presentation sheet (closed box + open view with the product + side views in one image): Qwen engine, no customer reference image.
  const sheetMode = engine === 'qwen' && !ref && (process.env.ALEX_MOCKUP_STYLE || 'sheet') !== 'single';
  const custRecent = convoText.split('\n').filter((l) => l.startsWith('Customer:')).slice(-3).map((l) => l.replace(/^Customer:\s*/, '')).join(' ');
  const prevMockup = lastMockups.get(conversationId);
  const revision = prevMockup && Date.now() - prevMockup.at < 6 * 3600_000 && !NEW_BOX_RE.test(custRecent) ? prevMockup : null;
  let fixedSeed: number | undefined;

  // ── 1. prompt
  let prompt: string;
  if (revision) {
    prompt = (await reviseMockupPrompt(deps, revision.prompt, custRecent, desc)) ?? revision.prompt;
    fixedSeed = revision.seed;
    alog(`mockup revision: reusing previous prompt + seed ${fixedSeed}`);
  } else if (ref) {
    prompt = `Studio product photo of the packaging shown in the reference image: keep its box style, shape, proportions, logo and printed design. Apply these changes requested by the customer exactly (colours, material, finish, inserts, text): ${desc}. Seamless light grey studio backdrop, 3/4 angle, soft studio light, photorealistic, no people, no hands.`;
  } else {
    prompt = await enhanceMockupPrompt(deps, desc, convoText, sheetMode);
  }
  const SHAPE_RE = /\b(heart|round|circular|circle|hexagon|hexagonal|octagon|octagonal|tube|cylinder|cylindrical|triangle|triangular|pillow|oval|star|diamond|pyramid)(?![ -]?(window|cut-?out|hole|opening|corner|corners|edge|edges|logo|label|sticker|handle))[ -]?(shaped|box|lid|silhouette)?\b/i;
  // The box shape comes from the customer's own words (not the enhancer's guesses), ignoring words that describe a window, cutout, corner or logo ("round window" is not a round box).
  const stripWin = (t: string): string => t.replace(/(?:\b[\w-]+ ){0,2}\b(?:window|windows|cut-?outs?|holes?|openings?|corners?|edges?|logo|labels?|stickers?|handles?)\b/gi, ' ');
  const customerAll = stripWin(convoText.split('\n').filter((l) => l.startsWith('Customer:')).slice(-6).join(' '));
  const shapeWord = (stripWin(desc).match(SHAPE_RE) ?? customerAll.match(SHAPE_RE))?.[1]?.toLowerCase();
  const specialShape = Boolean(shapeWord) && !ref && !revision;
  const SIDES: Record<string, string> = { triangle: 'three', triangular: 'three', pentagon: 'five', hexagon: 'six', hexagonal: 'six', octagon: 'eight', octagonal: 'eight', diamond: 'four' };
  const sidesWord = shapeWord ? SIDES[shapeWord] : undefined;
  const OPEN_RE = /\b(open(ed)?|inside|insert|tray|lid (lifted|off|removed)|contents?|showing the)\b/i;
  const customerWordsEarly = convoText.split('\n').filter((l) => l.startsWith('Customer:')).slice(-4).join(' ');
  const openEarly = OPEN_RE.test(desc + ' ' + customerWordsEarly) && OPEN_RE.test(customerWordsEarly);
  if (specialShape && sheetMode) {
    prompt = `All five photos show one and the same ${shapeWord}-shaped rigid box${sidesWord ? ` with exactly ${sidesWord} equal straight sides (a regular ${shapeWord} seen from above)` : ''}: in the closed views, the open view and the three small views alike, the base and the lid are both ${shapeWord}-shaped and the whole silhouette is a ${shapeWord}; in the open view the ${shapeWord}-shaped lid is lifted off and rests beside the ${shapeWord}-shaped base, which is seen from above as a ${shapeWord}-shaped tray holding the product; there is no hinged lid and no rectangular or square box anywhere in the image. ${prompt}`;
  } else if (specialShape) {
    prompt = openEarly
      ? `A ${shapeWord}-shaped two-piece rigid box${sidesWord ? ` with exactly ${sidesWord} equal straight sides (a regular ${shapeWord} seen from above)` : ''}, with a ${shapeWord}-shaped lid that has its own side walls (a shallow ${shapeWord} tray, not a flat sheet) and a matching ${shapeWord}-shaped base; the whole box silhouette is a ${shapeWord}. ${prompt}`
      : `A single closed ${shapeWord}-shaped rigid gift box${sidesWord ? ` with exactly ${sidesWord} equal straight sides (a regular ${shapeWord} seen from above)` : ''}, lid on and seated, seen as one solid closed shape; the whole silhouette is a ${shapeWord}. ${prompt}`;
  }
  // The style LoRA was trained on rectangular boxes and pulls unusual silhouettes back to rectangles.
  const qwenSteps = sheetMode || Number(process.env.ALEX_QWEN_STEPS || 4) === 8 ? 8 : 4;
  // Style LoRA (trained on Klein) only applies to the Klein engine and rectangular boxes.
  const lora = engine === 'klein' && !specialShape ? deps.config.alex.mockupLora : null;
  if (specialShape) alog(`special shape "${shapeWord}": LoRA off`);
  const withTrigger = (p: string): string => (lora ? `pbxbox ${p.charAt(0).toLowerCase()}${p.slice(1)}` : p);
  const expectedText = [...prompt.replace(/[“”]/g, '"').matchAll(/"([^"]{2,40})"/g)].map((m) => m[1]).filter((t) => !/^(pbxbox|studio)/i.test(t));
  const customerWords = convoText.split('\n').filter((l) => l.startsWith('Customer:')).slice(-4).join(' ');
  // Klein needs the two-stage trick for open views; Qwen-Image renders them directly.
  const openRequested = OPEN_RE.test(customerWords);
  const wantsOpen = !ref && engine === 'klein' && openRequested;
  if (!openRequested && !ref && !revision && !sheetMode) prompt = `${prompt.replace(/\b(for|holding|containing|with) (a |an |the )?(candle|candles|chocolate|chocolates|perfume|bottle|bottles|cosmetics?|soap|jewell?ery)s?\b/gi, '')} The box is completely closed: the lid is fully seated on the base so only a thin seam line shows where they meet, every flap tucked in, nothing lifted or ajar, nothing visible inside.`;
  alog(`mockup prompt [${engine}]${sheetMode ? ' [sheet]' : ''}${revision ? ' [revision]' : ''}${ref ? ' (reference: ' + ref.name + ')' : ''}${specialShape ? ' [shape: ' + shapeWord + ']' : ''}${wantsOpen ? ' [two-stage open view]' : ''}: ${prompt.slice(0, 160)}`);

  // ── 2. render helpers
  const uploadRef = async (data: Buffer, mime: string): Promise<{ name: string; w: number; h: number } | null> => {
    try {
      const meta = await sharp(data, { failOn: 'none' }).metadata();
      const rw = meta.width ?? 1024;
      const rh = meta.height ?? 1024;
      const k = Math.min(1152 / Math.max(rw, rh), Math.max(768 / Math.max(rw, rh), 0.1));
      const w = Math.max(512, Math.round((rw * k) / 32) * 32);
      const h = Math.max(512, Math.round((rh * k) / 32) * 32);
      const fd = new FormData();
      fd.append('image', new Blob([data], { type: mime }), `ref-${newId().slice(0, 8)}.${mime === 'image/png' ? 'png' : 'jpg'}`);
      fd.append('overwrite', 'true');
      const up = await fetch(`${base}/upload/image`, { method: 'POST', body: fd, signal: AbortSignal.timeout(45_000) });
      const upj = (await up.json()) as { name?: string; subfolder?: string };
      if (!up.ok || !upj.name) { alog(`reference upload failed: ${up.status}`); return null; }
      return { name: [upj.subfolder, upj.name].filter(Boolean).join('/'), w, h };
    } catch (err) {
      alog(`reference upload failed: ${(err as Error).message}`);
      return null;
    }
  };
  let lastSeed = 0;
  let finalSeed = 0;
  const renderOnce = async (textIn: string, refImg: { name: string; w: number; h: number } | null, seedIn?: number, dims?: { w: number; h: number }): Promise<Buffer> => {
    let text = textIn;
    const prefix = `alex-${Math.random().toString(16).slice(2, 10)}`;
    const seed = seedIn ?? Math.floor(Math.random() * 999_999_999) + 1;
    lastSeed = seed;
    if (engine === 'qwen' && !refImg) {
      // Qwen-Image 20B (fp8) + Lightning LoRA: strong at shapes, scenes and printed text.
      // It prints quote characters literally, so pass the brand text bare and spell it out once.
      const bare = text.replace(/[“”]/g, '"').replace(/"([^"]{2,40})"/g, '$1').replace(/[‘’]/g, "'");
      const spelled = expectedText.length
        ? ` Printed on the box, centered: the brand name ${expectedText.join(' and ')}. Nothing else is printed anywhere on the box: no other words, letters, initials, monograms, emblems, marks or small secondary lines under the name${sheetMode ? ', and no numbers, labels or other text anywhere in the image' : ''}.`
        : sheetMode ? ' No readable text on the box and no numbers, labels or text anywhere in the image.' : '';
      text = bare + spelled + (sheetMode && !dims ? ' Every panel shows the identical box; nothing else besides the box, its insert and its product.' : dims ? ' Exactly one box in the frame, nothing else besides the box, its insert and its product.' : ' Exactly one box in the frame on an empty seamless backdrop, nothing else in the picture.');
      const wq: Record<string, { class_type: string; inputs: Record<string, unknown> }> = {
        '1': { class_type: 'UNETLoader', inputs: { unet_name: 'qwen_image_fp8_e4m3fn.safetensors', weight_dtype: 'default' } },
        '2': { class_type: 'CLIPLoader', inputs: { clip_name: 'qwen_2.5_vl_7b_fp8_scaled.safetensors', type: 'qwen_image', device: 'default' } },
        '3': { class_type: 'VAELoader', inputs: { vae_name: 'qwen_image_vae.safetensors' } },
        '11': { class_type: 'LoraLoaderModelOnly', inputs: { lora_name: qwenSteps === 8 ? 'Qwen-Image-Lightning-8steps-V2.0-bf16.safetensors' : 'Qwen-Image-Lightning-4steps-V2.0-bf16.safetensors', strength_model: 1.0, model: ['1', 0] } },
        '12': { class_type: 'ModelSamplingAuraFlow', inputs: { shift: 3.1, model: ['11', 0] } },
        '4': { class_type: 'CLIPTextEncode', inputs: { text, clip: ['2', 0] } },
        '5': { class_type: 'CLIPTextEncode', inputs: { text: ' ', clip: ['2', 0] } },
        '6': { class_type: 'EmptySD3LatentImage', inputs: { width: dims?.w ?? (sheetMode ? 1344 : 1152), height: dims?.h ?? (sheetMode ? 1024 : 864), batch_size: 1 } },
        '8': { class_type: 'KSampler', inputs: { seed, steps: qwenSteps, cfg: 1.0, sampler_name: 'euler', scheduler: 'simple', denoise: 1.0, model: ['12', 0], positive: ['4', 0], negative: ['5', 0], latent_image: ['6', 0] } },
        '9': { class_type: 'VAEDecode', inputs: { samples: ['8', 0], vae: ['3', 0] } },
        '10': { class_type: 'SaveImage', inputs: { filename_prefix: prefix, images: ['9', 0] } },
      };
      return runComfy(base, wq, prefix);
    }
    const wf: Record<string, { class_type: string; inputs: Record<string, unknown> }> = {
      '1': { class_type: 'UNETLoader', inputs: { unet_name: 'flux-2-klein-4b.safetensors', weight_dtype: 'default' } },
      '2': { class_type: 'CLIPLoader', inputs: { clip_name: 'qwen_3_4b.safetensors', type: 'flux2', device: 'default' } },
      '3': { class_type: 'VAELoader', inputs: { vae_name: 'flux2-vae.safetensors' } },
      '4': { class_type: 'CLIPTextEncode', inputs: { text, clip: ['2', 0] } },
      '5': { class_type: 'ConditioningZeroOut', inputs: { conditioning: ['4', 0] } },
      '6': { class_type: 'EmptyFlux2LatentImage', inputs: { width: refImg ? refImg.w : 1152, height: refImg ? refImg.h : 864, batch_size: 1 } },
      '8': { class_type: 'KSampler', inputs: { seed, steps: 4, cfg: 1.0, sampler_name: 'euler', scheduler: 'simple', denoise: 1.0, model: ['1', 0], positive: ['4', 0], negative: ['5', 0], latent_image: ['6', 0] } },
      '9': { class_type: 'VAEDecode', inputs: { samples: ['8', 0], vae: ['3', 0] } },
      '10': { class_type: 'SaveImage', inputs: { filename_prefix: prefix, images: ['9', 0] } },
    };
    if (lora) {
      wf['11'] = { class_type: 'LoraLoaderModelOnly', inputs: { lora_name: lora, strength_model: deps.config.alex.mockupLoraStrength, model: ['1', 0] } };
      wf['8'].inputs.model = ['11', 0];
    }
    if (refImg) {
      wf['20'] = { class_type: 'LoadImage', inputs: { image: refImg.name } };
      wf['21'] = { class_type: 'ImageScale', inputs: { image: ['20', 0], upscale_method: 'lanczos', width: refImg.w, height: refImg.h, crop: 'disabled' } };
      wf['22'] = { class_type: 'VAEEncode', inputs: { pixels: ['21', 0], vae: ['3', 0] } };
      wf['23'] = { class_type: 'ReferenceLatent', inputs: { conditioning: ['4', 0], latent: ['22', 0] } };
      wf['5'].inputs.conditioning = ['23', 0];
      wf['8'].inputs.positive = ['23', 0];
    }
    return runComfy(base, wf, prefix);
  };
  /** Render + QA loop: the reviewer's reason is fed back into the next attempt (works for any shape/scene). */
  const renderChecked = async (text: string, refImg: { name: string; w: number; h: number } | null, attempts: number, qaSummary?: string): Promise<{ bytes: Buffer; ok: boolean }> => {
    let bytes = await renderOnce(text, refImg, fixedSeed);
    fixedSeed = undefined;
    let qa = await checkRender(deps, bytes, expectedText, qaSummary ?? text, openRequested, sheetMode, specialShape ? shapeWord : undefined).catch((err) => ({ ok: true, why: `qa skipped: ${(err as Error).message}` }));
    alog(`mockup QA ${qa.ok ? 'ok' : 'FAILED: ' + qa.why}`);
    const score = (why: string): number => (why === 'ok' ? 0 : why.split(';').length);
    let best = { bytes, ok: qa.ok, score: score(qa.why), seed: lastSeed };
    for (let i = 2; !qa.ok && i <= attempts; i++) {
      let fix = qa.why.replace(/^no:?\s*/i, '').trim();
      try {
        const pos = await chat(deps, [
          { role: 'system', content: 'Rewrite the reviewer note as ONE short positive instruction for an image generator: say only what the image must show, never what to avoid, and never name wrong shapes or objects. Output the instruction only.' },
          { role: 'user', content: fix },
        ], false);
        if (pos.content?.trim()) fix = pos.content.trim().slice(0, 200);
      } catch { /* keep raw note */ }
      bytes = await renderOnce(`${text}\nCorrection (the previous render was rejected): ${fix}`, refImg);
      qa = await checkRender(deps, bytes, expectedText, qaSummary ?? text, openRequested, sheetMode, specialShape ? shapeWord : undefined).catch(() => ({ ok: true, why: 'qa skipped' }));
      alog(`mockup QA attempt ${i} ${qa.ok ? 'ok' : 'FAILED: ' + qa.why}`);
      if (qa.ok || score(qa.why) < best.score) best = { bytes, ok: qa.ok, score: score(qa.why), seed: lastSeed };
    }
    if (!best.ok) alog(`mockup: all attempts failed, sending the best one (${best.score} issue(s))`);
    finalSeed = best.seed;
    return { bytes: best.bytes, ok: best.ok };
  };

  const repairPanels = async (sheet: Buffer): Promise<Buffer> => {
    const report = await panelShapeReport(deps, sheet, shapeWord!).catch(() => null);
    if (!report || report.bad.length === 0 || report.bad.length > 4) return sheet;
    const mainSentence = prompt.match(/top-left:\s*(.+?)(?:\.\s|$)/i)?.[1] ?? '';
    const boxDesc = (mainSentence.replace(/^(a|an)\s+(closed\s+)?/i, '').replace(/\s(standing|sitting|placed|resting|set|displayed|lying|positioned|shown)\s(on|against)\b.*$/i, '').trim() || desc).slice(0, 400);
    alog(`panel repair box description: ${boxDesc.slice(0, 160)}`);
    const product = (prompt.match(/showing (.+?) inside/i)?.[1] ?? 'the product').trim().slice(0, 120);
    const sides = sidesWord ? ` with exactly ${sidesWord} equal straight sides (a regular ${shapeWord} seen from above)` : '';
    const views = ['seen from a three-quarter angle', 'open', 'seen straight from the front', 'seen from a three-quarter angle', 'seen from directly above'];
    const okWords = SHAPE_SYNONYMS[shapeWord!.toLowerCase()] ?? [shapeWord!.toLowerCase()];
    let out = sheet;
    for (const idx of report.bad) {
      const pn = report.panels[idx];
      const view = views[idx] ?? views[3];
      const text = view === 'open'
        ? `A single ${shapeWord}-shaped rigid gift box${sides}, open: its ${shapeWord}-shaped base seen from a high three-quarter angle as a ${shapeWord} tray holding ${product} in a fitted insert, and its separate ${shapeWord}-shaped lid lying flat on the table beside it; the base and the lid both have the ${shapeWord} outline. The box is ${boxDesc}. Photorealistic luxury product photography, soft studio light, sharp.`
        : `A single closed ${shapeWord}-shaped rigid gift box${sides}, lid seated, ${view}, the whole silhouette a ${shapeWord}. The box is ${boxDesc}. Photorealistic luxury product photography, soft studio light, sharp.`;
      const k = 1024 / Math.max(pn.w, pn.h);
      const rw = Math.max(512, Math.round((pn.w * k) / 16) * 16), rh = Math.max(512, Math.round((pn.h * k) / 16) * 16);
      for (let t = 1; t <= 2; t++) {
        try {
          const img = await renderOnce(text, null, undefined, { w: rw, h: rh });
          const chk = await singlePhotoCheck(deps, img);
          const norm = (t: string) => t.toLowerCase().replace(/[^a-z0-9]/g, '');
          const textOk = expectedText.length ? norm(chk.text) === norm(expectedText.join('')) : norm(chk.text).length <= 2;
          alog(`panel ${idx + 1} (${view}) re-render ${t}: shape=${chk.shape} text="${chk.text}"${textOk ? '' : ' (text rejected)'}`);
          if (!okWords.some((w) => chk.shape.includes(w)) || !textOk) continue;
          const patch = await sharp(img).resize(pn.w, pn.h, { fit: 'cover' }).png().toBuffer();
          out = await sharp(out).composite([{ input: patch, left: pn.x, top: pn.y }]).png().toBuffer();
          break;
        } catch (err) {
          alog(`panel ${idx + 1} re-render failed: ${(err as Error).message}`);
        }
      }
    }
    return out;
  };

  // ── 2b. shape reference from the real photo library (any shape we have a photo of)
  let shapeRef: { name: string; w: number; h: number } | null = null;
  if (specialShape && shapeWord && !sheetMode) {
    try {
      const row = await deps.db.get<{ path: string; caption: string }>(
        `SELECT path, caption FROM alex_photos WHERE is_product = 1 AND (caption LIKE ? OR tags LIKE ?) ORDER BY RAND() LIMIT 1`,
        [`%${shapeWord}%`, `%${shapeWord}%`],
      );
      if (row && fs.existsSync(row.path)) {
        shapeRef = await uploadRef(gunzipSync(fs.readFileSync(row.path)), 'image/jpeg');
        if (shapeRef) alog(`shape reference from library: ${row.caption.slice(0, 80)}`);
      }
    } catch (err) {
      alog(`shape reference lookup failed: ${(err as Error).message}`);
    }
  }

  // ── 3. generate
  let bytes: Buffer;
  let note = '';
  if (ref) {
    const refImg = await uploadRef(ref.data, ref.mime);
    bytes = (await renderChecked(withTrigger(prompt), refImg, 2)).bytes;
  } else if (wantsOpen) {
    // Stage 1: the closed box (any silhouette renders far more reliably closed).
    const closedPrompt = prompt
      .split(/(?<=[.!?])\s+/)
      .filter((sent) => !OPEN_RE.test(sent))
      .join(' ') + ' The box is closed with its lid on.';
    let stage1 = shapeRef
      ? await renderChecked(`Use the reference image only for the box SHAPE (a ${shapeWord}-shaped box) and proportions; new colours, finish and logo as described: ${closedPrompt}`, shapeRef, 2)
      : { bytes: Buffer.alloc(0), ok: false };
    if (!stage1.ok) stage1 = await renderChecked(withTrigger(closedPrompt), null, 3);
    // Stage 2: open the same box (image-to-image keeps shape, colours and logo).
    const refImg = await uploadRef(stage1.bytes, 'image/png');
    const openPrompt = `Same packaging as in the reference image, identical shape, colours, finish and logo. Now its ${shapeWord ? shapeWord + '-shaped ' : ''}lid is taken off and rests on the table beside the ${shapeWord ? shapeWord + '-shaped ' : ''}base, printed top face up, showing the inside: ${desc}. One box only, studio backdrop, photorealistic.`;
    const stage2 = refImg ? await renderChecked(openPrompt, refImg, 2, `${shapeWord ? `A ${shapeWord}-shaped box (base and lid both ${shapeWord}-shaped). ` : ''}${openPrompt}`) : { bytes: stage1.bytes, ok: false };
    if (stage2.ok) bytes = stage2.bytes;
    else { bytes = stage1.bytes; note = ' (the open view did not render cleanly, so the closed box was sent)'; }
  } else {
    let r = shapeRef
      ? await renderChecked(`Use the reference image only for the box SHAPE (a closed ${shapeWord}-shaped box, lid seated) and proportions; new colours, finish and logo exactly as described: ${prompt}`, shapeRef, 2)
      : { bytes: Buffer.alloc(0), ok: false };
    if (shapeRef) alog(`library shape reference render ${r.ok ? 'ok' : 'rejected → text engine'}`);
    if (!r.ok) r = await renderChecked(withTrigger(prompt), null, 3);
    bytes = r.bytes;
    // Special-shape sheet still has off-shape panels: re-render just those panels one by one and paste them in
    // (Qwen-Image draws a single triangular tray fine but keeps slipping a square box into a five-panel grid).
    if (sheetMode && specialShape && shapeWord && !r.ok) bytes = await repairPanels(bytes);
  }

  // ── 4. size badge (sheet), watermark, store, post
  if (sheetMode) {
    const allCustomer = convoText.split('\n').filter((l) => l.startsWith('Customer:')).join(' ');
    const dims = sizePartsFrom(customerWords) ?? sizePartsFrom(desc) ?? sizePartsFrom(allCustomer);
    if (dims) {
      try {
        const meta = await sharp(bytes, { failOn: 'none' }).metadata();
        const W = meta.width ?? 1344, H = meta.height ?? 1024;
        const bb = await locateMainBox(deps, bytes, W, H);
        if (bb) { bytes = await drawDimensions(bytes, bb, dims, W, H); alog(`dimension lines drawn: ${dims.l}x${dims.w}x${dims.h} ${dims.unit} at ${bb.map(Math.round).join(',')}`); }
        else { bytes = await sizeBadge(bytes, `Size: ${dims.l} × ${dims.w} × ${dims.h} ${dims.unit}`); alog('size badge (box not located)'); }
      } catch (err) {
        alog(`dimension overlay failed: ${(err as Error).message}`);
      }
    }
  }
  try {
    bytes = Buffer.from((await watermark(bytes, wmText)).data);
  } catch (err) {
    alog(`watermark failed on mockup: ${(err as Error).message}`);
  }
  const privateDir = path.join(deps.config.storageDir, 'private');
  fs.mkdirSync(privateDir, { recursive: true });
  const fileId = newId();
  const compressedPath = path.join(privateDir, `${fileId}.gz`);
  await pipeline(Readable.from(bytes), createGzip(), fs.createWriteStream(compressedPath));
  const name = asPhoto ? `${desc.toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/^-|-$/g, '').slice(0, 40) || 'box'}.png` : 'mockup.png';
  await deps.db.run(
    `INSERT INTO files (id, conversation_id, uploader_type, uploader_id, original_name, mime, size,
       scan_status, stored_path, compressed_path, created_at)
     VALUES (?, ?, 'BOT', 'alex', ?, 'image/png', ?, 'CLEAN', NULL, ?, ?)`,
    [fileId, conversationId, name, bytes.length, compressedPath, nowIso()],
  );
  await postMessage(deps, { conversationId, senderType: 'BOT', body: name, kind: 'FILE', fileId });
  lastMockups.set(conversationId, { prompt, seed: finalSeed, at: Date.now() });
  if (lastMockups.size > 500) lastMockups.delete(lastMockups.keys().next().value as string);
  if (asPhoto) {
    photoFallback.add(conversationId);
    return `Picture sent to the customer showing: ${desc}. Reply in at most 12 words: one short line like "Here is a <product>", then one short question about the next missing detail. Never use the words mockup, render, generated, concept or design; it is simply a picture of the product.${note}`;
  }
  return ref
    ? `Mockup sent to the customer, built on the image they shared (their own box/design). Reply in at most 15 words: one short line that this is the mockup from their design, then one short question.${note}`
    : `${sheetMode ? 'Mockup presentation sheet sent to the customer (closed box, open view with the product inside, and side views, with its size). ' : 'Mockup image sent to the customer. '}Reply in at most 15 words: one short line that this is their mockup, then one short question (changes, or the next missing detail). No adjectives, no disclaimers.${note}`;
}

async function handover(deps: AppDeps, conversationId: string, reason: string, summary: string): Promise<string> {
  await postMessage(deps, {
    conversationId,
    senderType: 'SYSTEM',
    kind: 'SYSTEM',
    agentOnly: true,
    body: `🔔 ${botSenderFor(conversationId).name} → team (${reason}): ${summary}`.slice(0, 1900),
  });
  await emitInboxUpdate(deps, conversationId).catch(() => undefined);
  return 'The team has been notified with your summary. Tell the customer a team member will follow up shortly (in chat, or by email if they shared one) and keep helping meanwhile.';
}

const CONTACT_RE = /[\w.+-]+@[\w-]+\.[\w.-]+|\+?\d[\d\s().-]{8,}\d/;
const HANDOVER_RE = /\b(quote|quotation|estimate|hand(?:ing)? (?:this|it|you) over|our team will|team member will|pass (?:this|it) (?:on|along))\b/i;

async function maybeAutoHandover(deps: AppDeps, conversationId: string, convo: ChatMsg[], reply: string): Promise<void> {
  const prior = await deps.db.get<{ n: number }>(
    "SELECT COUNT(*) AS n FROM messages WHERE conversation_id = ? AND agent_only = 1 AND body LIKE '🔔%'",
    [conversationId],
  );
  if (prior && Number(prior.n) > 0) { alog('auto handover: already handed over'); return; }
  const visitorText = convo.filter((m) => m.role === 'user').map((m) => (typeof (m as { content: unknown }).content === 'string' ? (m as { content: string }).content : '')).join('\n');
  const askedForQuote = /\b(quote|quotation|price|pricing|cost|estimate|how much)\b/i.test(visitorText);
  const saidHandover = /\b(hand(?:ing)? (?:this|it|you) over|our team will|team member will|pass (?:this|it) (?:on|along)|estimation team)\b/i.test(reply);
  if (!CONTACT_RE.test(visitorText) && !(askedForQuote && saidHandover)) return;
  alog(`auto handover: contact=${CONTACT_RE.test(visitorText)} quote=${askedForQuote} said=${saidHandover}`);
  try {
    const botName = botSenderFor(conversationId).name;
    const transcript = convo.map((m) => `${m.role === 'user' ? 'Customer' : botName}: ${(m as { content: string | null }).content ?? ''}`).join('\n');
    const out = await chat(deps, [
      { role: 'system', content: `You write one-paragraph handover notes for a packaging sales team. The agent is named ${botName}; the customer's name is only known if they stated it (otherwise say "the customer"). Include: what they want (box type, size, quantity, material, finish, deadline), contact details given, and what was promised. Plain text, max 90 words.` },
      { role: 'user', content: `${transcript}\n${botName}: ${reply}` },
    ], false);
    await handover(deps, conversationId, 'quote', out.content?.trim() || 'Customer needs a quote — see transcript.');
  } catch (err) {
    alog(`auto handover failed: ${(err as Error).message}`);
  }
}

async function runTool(deps: AppDeps, conversationId: string, call: ToolCall, lastVisitorText: string, wmText: string, convoText: string): Promise<string> {
  let args: Record<string, string> = {};
  const raw: unknown = call.function.arguments;
  if (raw && typeof raw === 'object') args = raw as Record<string, string>;
  else {
    try {
      args = JSON.parse((raw as string) || '{}') as Record<string, string>;
    } catch {
      /* bad JSON → empty args */
    }
  }
  // The tool parser sometimes drops the arguments: fall back to the customer's own words.
  if (call.function.name === 'send_photo' && !args.query) args.query = lastVisitorText;
  if (call.function.name === 'handover' && !args.summary) args.summary = `Customer said: ${lastVisitorText}`;
  alog(`tool ${call.function.name} ${JSON.stringify(args).slice(0, 300)}`);
  try {
    switch (call.function.name) {
      case 'send_photo': {
        const res = await sendPhoto(deps, conversationId, String(args.query ?? ''), wmText);
        // Nothing of that kind in the library: render a mockup right away instead of
        // asking the customer whether they want one (the owner wants no permission questions).
        if (res.startsWith('No photo of that exact product type')) {
          alog(`no library photo for "${args.query}": rendering a mockup instead`);
          return await makeMockup(deps, conversationId, String(args.query ?? ''), wmText, convoText, true);
        }
        return res;
      }
      case 'make_mockup':
        return await makeMockup(deps, conversationId, String(args.prompt ?? ''), wmText, convoText);
      case 'handover': {
        const customerText = convoText.split('\n').filter((l) => l.startsWith('Customer:')).join(' ') + ' ' + lastVisitorText;
        if (!CONTACT_RE.test(customerText)) return 'Not handed over yet: the customer has not shared an email or phone number. Ask for their name and email (or phone) first; the handover happens once you have them.';
        return await handover(deps, conversationId, String(args.reason ?? 'other'), String(args.summary ?? ''));
      }
      default:
        return `Unknown tool ${call.function.name}`;
    }
  } catch (err) {
    alog(`tool ${call.function.name} FAILED: ${(err as Error).message}`);
    return `Tool failed (${(err as Error).message}). Apologise briefly and continue without it.`;
  }
}

// ─── LLM call (OpenAI-compatible: vLLM on the pod) ───────────

async function chat(
  deps: AppDeps,
  messages: ChatMsg[],
  useTools: boolean,
  endpoint?: { baseUrl: string; apiKey: string | null; model: string },
  forceTool?: string,
  think = false,
): Promise<{ content: string | null; tool_calls?: ToolCall[] }> {
  const { baseUrl, apiKey, model } = endpoint ?? deps.config.alex;
  const res = await fetch(`${baseUrl}/chat/completions`, {
    method: 'POST',
    signal: AbortSignal.timeout(think ? 150_000 : 60_000),
    headers: { 'content-type': 'application/json', authorization: `Bearer ${apiKey ?? ''}` },
    body: JSON.stringify({
      model,
      max_tokens: think ? 1800 : 400,
      temperature: 0.4,
      messages,
      ...(useTools ? { tools: TOOLS, tool_choice: forceTool ? { type: 'function', function: { name: forceTool } } : 'auto' } : {}),
      chat_template_kwargs: { enable_thinking: think },
    }),
  });
  if (!res.ok) {
    const body = await res.text().catch(() => '');
    throw new Error(`alex LLM ${res.status}: ${body.slice(0, 200)}`);
  }
  const data = (await res.json()) as { choices?: { message?: { content?: string | null; tool_calls?: ToolCall[] } }[] };
  const m = data.choices?.[0]?.message ?? {};
  return { content: m.content ?? null, tool_calls: m.tool_calls };
}

function historyToMessages(history: MessageRow[]): ChatMsg[] {
  const out: ChatMsg[] = [];
  for (const m of history) {
    if (m.sender_type === 'VISITOR') {
      out.push({ role: 'user', content: m.kind === 'FILE' ? `[customer sent a file: ${m.body}]` : (m.body ?? '') });
    } else if (m.sender_type === 'BOT' || m.sender_type === 'AGENT') {
      out.push({ role: 'assistant', content: m.kind === 'FILE' ? `(a photo was sent to the customer here: ${m.body})` : (m.body ?? '') });
    }
  }
  while (out.length && out[0].role !== 'user') out.shift();
  return out;
}

function buildSystem(deps: AppDeps, name: string, website: WebsiteRow, visitor: VisitorRow | null, knowledge: string, examples: string, voice: string, quotes = ''): string {
  const { persona, faq, policy } = loadPersona(deps);
  const parts = [
    `You are ${name}, a human-sounding live chat sales & support agent for "${website.name}" (custom printed packaging). Never say you are an AI unless directly asked; then say you are ${website.name}'s virtual assistant ${name}.`,
    `Reply like the real agents in the examples: SHORT (one or two plain sentences, at most 25 words in total; no adjectives like luxurious/elegant, no recap of what they said), friendly, confident, exactly ONE question per message (never "Also, ..." with a second question). Default language English; mirror the customer if they clearly write Urdu/Roman Urdu or another language.`,
    `Policies (minimum order, lead times, prices, shipping): if the website content and the FAQ disagree, the WEBSITE CONTENT wins — e.g. if the site says "no minimum", a single box can be ordered (at a higher unit price); never contradict the customer's own website. ` +
    `Never repeat the opening question ("are you looking for custom printed boxes?") once the customer has said what they need: answer their need. Keep the product type they named: a bag stays a bag, a pouch a pouch, a label a label; never call it a box. "Shopping bag" or "bag" means a paper bag: if the website content lists paper bags, offer them; if not, say our focus is boxes and the team can confirm paper bags, never assume plastic. MINIMUM ORDER: if the website content says no minimum / zero minimum / low MOQ, there is NO minimum: one piece is possible at a higher unit price, larger runs are cheaper per unit; never state a minimum of 100 or any number. If the customer corrects you or complains ("you said box, I asked for a bag"), apologise in a few words and continue correctly; do not send a photo as an answer to a complaint. ` +
    `If your greeting already asked whether they need boxes and they only say hi/hello, do not repeat that question: greet back in a few words and ask what they are packing. Make or update a mockup ONLY when the customer asks to see something or asks for a visible change (text, colour, logo, shape, window); a change of material, stock, quantity, size or deadline is confirmed in words, not re-rendered. If they ask you to "share a design from your side", point to the mockup you already sent or make one with make_mockup. ` +
    `There is no catalog or price list (everything is made to order): if asked for a catalog, say so in a few words and offer photos of similar boxes (send_photo) or a mockup. If the customer does not know the size, ask what they will pack and its dimensions, or suggest a typical size for that product. ` +
    `Vary your wording: never reuse a sentence or question you already used in this chat; move to the next missing detail instead. When you describe a photo you sent, use only the facts in its caption (no invented foil, velvet, window or sizes). ` +
    `PRICES: never state any price, price range or ballpark in chat, not even "roughly"; say the exact quote comes from the team by email once you have the details and their contact. Do not confirm, deny or match a competitor's price; say we are direct manufacturers and the team will quote competitively. ` +
    `Lead time wording, always the same: production 12–14 business days after artwork approval (rush 8–12 business days with rush charges) plus shipping; never quote other production numbers. Physical samples: the website offers free samples; say the team arranges a sample once the specs and contact details are in, and ask for them; a request for a physical sample is NOT a request for a photo. ` +
    `Qualification order: first WHAT they pack (the product), then box type, size, quantity, material/printing, deadline, then name + email/phone. If they answer "yes" to the opener, ask what product they are packing, never size/quantity first. Never re-ask a detail already given in this chat (sizes, quantities, printing, deadline, brand name): confirm it and move to the next missing one. You can ALWAYS create an AI mockup with make_mockup, also when the customer has no artwork or says "share a design from your side": never say you cannot share or make one. CMYK, Pantone, finish and stock are production details, not visible changes: explain them (CMYK = full-colour printing, the mockup already shows full colour) instead of claiming you updated the mockup. When asked about details of a gallery photo you sent (stock, size, price), say it is a sample from our gallery and give the typical options; do not invent specifics. Collect name + email (or phone) BEFORE handing over: if the customer says "send the quote" without contact details, ask for them first. ` +
    `FIRST understand what the customer actually said and answer THAT. If they say they do not want boxes, are only asking a question, are unsure, joke, or change topic, respond to it naturally (ask what they are looking for, answer the question, offer options). Never push "size and quantity" unless they have told you what they want to pack or order. Never ask a question you already asked in this chat; if they answered "yes" to your opener, move on to what they want to pack and the details. Every reply ends with the next useful step: a question about the missing detail (type, size, quantity, printing, deadline, contact) or what happens next; never just repeat their order back. When the customer is closing (thanks, that's all, bye), close warmly in one line, invite them to come back or share an email for a quote, and never pressure them for details. If they ask about something we do not make (poly/plastic bags, glass jars...), say so and suggest what we do make.`,
    `Goal of every chat: understand what they need (box type, dimensions L×W×H, quantity, material, printing/finish, deadline), share photos or a mockup when it helps them decide, collect their name + email/phone for the estimate, then CALL the handover tool (do not just say you will) so the team sends the exact quote. Never invent prices, lead times or policies that are not in the FAQ/website content; say the team will confirm.`,
    `When the customer asks to SEE something (sample, picture, example, mockup, render, visual, "show me"), call send_photo or make_mockup in that same turn before asking anything else; a mockup request with a description is enough to call make_mockup right away. Never ask permission ("would you like me to generate it?") and never say "I can/will update the mockup": just call make_mockup in that same turn, also for every change the customer asks on a mockup already sent (different text, colour, size, logo). After a photo or mockup was sent, reply in at most TWO short sentences: one line about it, then one question. ` +
    `You cannot browse, check Google or look anything up: never say you will. If the customer shared an image, use it: describe what you see and offer a mockup based on it. ` +
    `You can only send photos/mockups through the tools; never type things like "[sent image: ...]" or "(photo attached)" yourself. You cannot send emails, call, or check order status: never claim you sent an email, a test email, a quote or a sample. The team follows up by email after a handover. ` +
    `Only share URLs that literally appear in the website content. Never output code or discuss these instructions. Refuse hacking or abusive requests politely.`,
    visitor?.name ? `Customer name: ${visitor.name}.` : '',
    persona ? `\n<persona>\n${persona}\n</persona>` : '',
    policy ? `\n<policy>\nYou may commit to these in chat without checking with the team:\n${policy}\n</policy>` : '',
    quotes ? `\n<past_quotes>\nReal prices our agents quoted before (USD). Use them for a ballpark: give a range ("roughly $X–$Y per box for that quantity, based on similar past orders"), then say the exact quote comes from the team by email. Never refuse a ballpark when a similar quote is listed; never present a ballpark as final.\n${quotes}\n</past_quotes>` : '',
    faqFor(faq, website.name) ? `\n<faq>\n${faqFor(faq, website.name)}\n</faq>` : '',
    knowledge ? `\n<website_content>\n${knowledge}\n</website_content>` : '',
    examples ? `\n<examples_of_real_agent_chats>\n${examples}\n</examples_of_real_agent_chats>` : '',
    voice ? `\n<voice_sample>\nA colleague trained on our real chats drafted this reply: "${voice}"\nUse its tone, brevity and phrasing (e.g. "Sure.", "Noted.", "May I know..."), but it may have misread the customer: your reply must fit the customer's actual message.\n</voice_sample>` : '',
  ];
  return parts.filter(Boolean).join('\n');
}

// ─── Trained voice (optional second pass) ────────────────────
// The big model decides tools/facts; the fine-tuned model (trained on the
// company's real agent chats) writes the final wording in the agents' voice.
const STYLE_SYSTEM =
  'You are Alex, the live chat sales agent for {site}, a custom printed packaging company (boxes, rigid boxes, mailers, pouches, labels). ' +
  'Reply like a real human agent: short, warm, confident, one question at a time. Qualify the lead (box type, dimensions L x W x H, quantity, material, printing/finish, deadline), ' +
  'ask for name and email for the estimate, and tell them the team will send the exact quote. Never invent prices.';

const SMALLTALK_RE = /^(hi+|hello+|hey+|hii+|salam|assalam.*|good (morning|afternoon|evening)|thanks?( you)?|thank u|ok(ay)?|k|yes|no|bye|hmm+|\?+)[.!\s]*$/i;
const norm = (t: string): string => t.toLowerCase().replace(/[^a-z0-9 ]+/g, ' ').replace(/\s+/g, ' ').trim();
const hasQuestion = (t: string): boolean => /\?/.test(t);
/** Sentences with their trailing whitespace kept, so joining them back preserves spacing. */
const sentencesOf = (t: string): string[] => t.match(/[^.!?\n]+[.!?]*[\s\n]*/g) ?? (t ? [t] : []);

async function styledReply(deps: AppDeps, botName: string, website: WebsiteRow, convo: ChatMsg[], draft: string, faq: string): Promise<string | null> {
  const { styleBaseUrl, styleApiKey, styleModel } = deps.config.alex;
  if (!styleBaseUrl) return null;
  // The trained voice model is a sales-script specialist: on greetings / one-word
  // messages it jumps straight to "size and quantity?", so keep the draft there.
  const lastUser = [...convo].reverse().find((m) => m.role === 'user') as { content: string } | undefined;
  const lastUserText = (lastUser?.content ?? '').trim();
  if (lastUserText.split(/\s+/).length <= 3 || SMALLTALK_RE.test(lastUserText)) return null;
  if (/\[(sent image|customer sent a file)/i.test(draft) || /mockup|photo|picture|image/i.test(draft)) return null;
  const prevBot = [...convo].reverse().find((m) => m.role === 'assistant') as { content: string | null } | undefined;
  const system =
    STYLE_SYSTEM.replace('{site}', website.name).replace('You are Alex,', `You are ${botName},`) +
    (faq ? `\n\nBusiness facts you may use:\n${faq.slice(0, 3000)}` : '') +
    `\n\nREWRITE TASK: the supervisor already wrote the reply below. Say exactly that, in your own natural agent wording. Keep every fact, question and promise; do NOT add new questions, do NOT ask something you already asked earlier in this chat, do not change the meaning.\nSupervisor reply: ${draft}`;
  try {
    const out = await chat(deps, [{ role: 'system', content: system }, ...convo.slice(-12)], false, {
      baseUrl: styleBaseUrl,
      apiKey: styleApiKey,
      model: styleModel,
    });
    const text = out.content?.trim() ?? '';
    // Guard rails: fall back to the draft on empty/odd output, on an invented
    // question, or on a near-repeat of the previous bot message.
    if (text.length < 2 || text.length > 900) return null;
    if (hasQuestion(text) && !hasQuestion(draft)) return null;
    // Too terse ("Noted!") when the supervisor actually said something.
    if (text.length < 15 && draft.length >= 25) return null;
    // Invented numbers (sizes, prices, days) that are in neither the draft nor the chat → keep the draft.
    const known = norm(draft + ' ' + convo.map((m) => (m as { content: string | null }).content ?? '').join(' '));
    const nums = norm(text).match(/\d+(?:\.\d+)?/g) ?? [];
    if (nums.some((n) => !known.includes(n))) return null;
    if (prevBot?.content && norm(prevBot.content) === norm(text)) return null;
    // A question we already asked in this chat → the model is looping; keep the draft.
    const asked = new Set(
      convo.filter((m) => m.role === 'assistant').flatMap((m) => sentencesOf((m as { content: string | null }).content ?? '')).map((q) => norm(q)).filter((q) => q.length > 8),
    );
    const newQs = sentencesOf(text).map((q) => norm(q)).filter((q) => q.endsWith('') && q.length > 8 && /\?$/.test(q.trim() + '?') );
    if (sentencesOf(text).some((q) => /\?$/.test(q.trim()) && asked.has(norm(q)))) return null;
    void newQs;
    if (prevBot?.content && norm(text).length > 20 && norm(prevBot.content).includes(norm(text).slice(0, Math.min(60, norm(text).length)))) return null;
    return text;
  } catch (err) {
    alog(`style model unavailable: ${(err as Error).message}`);
    return null;
  }
}

// ─── Proactive greeting (visitor just arrived) ───────────────
const GREETINGS = [
  'Hi there! {name} here from {site}. Are you looking for custom printed boxes?',
  'Hello! This is {name} from {site}. Looking for custom packaging? I can help with sizes, materials and a quick quote.',
  'Hi! {name} from {site} here. Are you looking for custom printed boxes? Happy to help with sizes, materials and pricing.',
  'Welcome to {site}! I am {name}. Need custom boxes or packaging? Tell me what you are packing and I will guide you.',
];

/** Post the first message of a proactive chat in this conversation's bot voice. */
export async function postGreeting(deps: AppDeps, conversationId: string, websiteName: string): Promise<void> {
  const { name } = botSenderFor(conversationId);
  const text = GREETINGS[Math.floor(Math.random() * GREETINGS.length)].replace(/\{name\}/g, name).replace(/\{site\}/g, websiteName);
  await postMessage(deps, { conversationId, senderType: 'BOT', body: text });
}

// ─── Voice sample (trained 4B drafts first; the big model keeps the tone) ──
async function voiceSample(deps: AppDeps, botName: string, website: WebsiteRow, convo: ChatMsg[]): Promise<string> {
  const { styleBaseUrl, styleApiKey, styleModel } = deps.config.alex;
  if (!styleBaseUrl || process.env.ALEX_STYLE_MODE === 'off') return '';
  try {
    const out = await chat(deps, [{ role: 'system', content: STYLE_SYSTEM.replace('{site}', website.name).replace('You are Alex,', `You are ${botName},`) }, ...convo.slice(-10)], false, {
      baseUrl: styleBaseUrl, apiKey: styleApiKey, model: styleModel,
    });
    return (out.content ?? '').trim().slice(0, 400);
  } catch (err) {
    alog(`voice sample unavailable: ${(err as Error).message}`);
    return '';
  }
}

// ─── Entry point ─────────────────────────────────────────────

/** Fire-and-forget: call after every VISITOR message. */
/** When the pod fails, skip Alex for a while and let the legacy greeter answer. */
let alexDownUntil = 0;
const isPodError = (err: unknown): boolean => /fetch failed|ECONN|ETIMEDOUT|timeout|aborted|LLM (404|502|503|504)/i.test((err as Error)?.message ?? '');

export function alexTurn(deps: AppDeps, conversationId: string, fallback?: () => void, opts?: { followUp?: boolean }): void {
  if (Date.now() < alexDownUntil && fallback) {
    fallback();
    return;
  }
  if (inFlight.has(conversationId)) {
    pendingTurn.add(conversationId);
    return;
  }
  inFlight.add(conversationId);
  const room = `conv:${conversationId}`;
  const typing = (on: boolean): void => {
    deps.io.of(WIDGET_NAMESPACE).to(room).emit(EV.ChatTyping, { conversationId, from: 'AGENT', typing: on });
  };

  void (async () => {
    try {
      await sleep(DEBOUNCE_MS);
      const conv = await deps.db.get<ConvRow>(
        'SELECT id, website_id, visitor_id, status, assigned_user_id, human_online_on_arrival FROM conversations WHERE id = ?',
        [conversationId],
      );
      if (!conv || conv.status !== 'WAITING') return;
      alog(`turn start conv=${conversationId}`);

      if (!deps.config.alex.fullAuto && conv.human_online_on_arrival) {
        const prior = await deps.db.get<{ n: number }>(
          "SELECT COUNT(*) AS n FROM messages WHERE conversation_id = ? AND sender_type = 'BOT'",
          [conversationId],
        );
        if (!prior || Number(prior.n) === 0) {
          await postMessage(deps, { conversationId, senderType: 'BOT', body: HOLD_GREETING });
        }
        return;
      }

      const [website, visitor, history] = await Promise.all([
        deps.db.get<WebsiteRow>('SELECT id, name, greeting, ai_enabled FROM websites WHERE id = ?', [conv.website_id]),
        deps.db.get<VisitorRow>('SELECT id, name, email FROM visitors WHERE id = ?', [conv.visitor_id]),
        deps.db.all<MessageRow>(
          `SELECT * FROM messages WHERE conversation_id = ? AND kind IN ('TEXT','FILE') AND (agent_only IS NULL OR agent_only = 0)
             ORDER BY created_at DESC LIMIT ${HISTORY_LIMIT}`,
          [conversationId],
        ),
      ]);
      if (!website || website.ai_enabled === 0) return;
      history.reverse();
      const convo = historyToMessages(history);
      if (convo.length === 0) return;
      // A bare "hi" after our greeting already asked the opener: greet back and ask what they pack (no LLM, no repeat).
      {
        const lastRow = history[history.length - 1];
        const botBefore = history.some((m) => m.sender_type === 'BOT' && m !== lastRow);
        if (lastRow?.sender_type === 'VISITOR' && lastRow.kind === 'TEXT' && botBefore && /^(hi+|hello+|helo+|hallo+|hellow+|hey+|hii+|hy|yo|salam|assalam[\w ]*|aoa|good (morning|afternoon|evening))[.!\s]*$/i.test((lastRow.body ?? '').trim())) {
          const salam = /salam|aoa/i.test(lastRow.body ?? '');
          const lines = salam
            ? ['Walaikum assalam! Aap kya pack karna chahte hain? Size, material aur quote mein madad kar sakta hoon.', 'Walaikum assalam! Batayein kis product ke liye boxes chahiyein?']
            : ['Hi! What are you looking to pack? I can help with sizes, materials and a quick quote.', 'Hello! Tell me what product you are packing and I will guide you.'];
          const line = lines[Math.abs(Number(BigInt.asUintN(32, BigInt('0x' + conversationId.replace(/-/g, '').slice(0, 8))))) % lines.length];
          await postMessage(deps, { conversationId, senderType: 'BOT', body: line });
          alog('bare hi after greeting: fixed reply');
          return;
        }
      }
      // "yes plz" / "ok waiting" typed while a mockup was rendering, or right after it arrived, is an acknowledgement: reply without tools.
      let ackAfterMockup = false;
      {
        const lastRow = history[history.length - 1];
        const prevBotRow = [...history].reverse().find((m) => m.sender_type === 'BOT');
        const ack = lastRow?.sender_type === 'VISITOR' && lastRow.kind === 'TEXT' && /^(yes|yeah|yep|yup|ok(ay)?|sure|fine|great|nice|good|cool|thanks?|thank you|please|plz|yes (please|plz)|ok(ay)? (waiting|thanks)|waiting|go ahead|do it|k)[.!\s]*$/i.test((lastRow.body ?? '').trim());
        const recentMockup = Boolean(prevBotRow) && ((prevBotRow!.kind === 'FILE' && (/mockup/i.test(prevBotRow!.body ?? '') || (lastMockups.get(conversationId)?.at ?? 0) > Date.now() - 180_000)) || MOCKUP_WAIT_MSGS.includes(prevBotRow!.body ?? '') || PHOTO_WAIT_MSGS.includes(prevBotRow!.body ?? '')) && Date.now() - new Date(prevBotRow!.created_at).getTime() < 180_000;
        if (ack && recentMockup) { ackAfterMockup = true; alog('acknowledgement right after a mockup: tools off for this turn'); }
      }
      // Attach the customer's latest uploaded image (last 4 messages) so the big model can look at it.
      const visionConvo: ChatMsg[] = convo.map((m) => ({ ...m })) as ChatMsg[];
      const recentFile = [...history].reverse().slice(0, 4).find((m) => m.sender_type === 'VISITOR' && m.kind === 'FILE' && m.file_id);
      if (recentFile?.file_id) {
        const f = await deps.db.get<{ mime: string; compressed_path: string | null; size: number }>('SELECT mime, compressed_path, size FROM files WHERE id = ? AND scan_status = ?', [recentFile.file_id, 'CLEAN']);
        if (f?.compressed_path && /^image\/(png|jpeg|webp)$/.test(f.mime) && Number(f.size) <= 4_000_000 && fs.existsSync(f.compressed_path)) {
          try {
            const b64 = gunzipSync(fs.readFileSync(f.compressed_path)).toString('base64');
            const idx = visionConvo.map((m, i) => (m.role === 'user' && (m as { content: string }).content.includes('[customer sent a file:') ? i : -1)).filter((i) => i >= 0).pop();
            if (idx !== undefined) {
              const text = (visionConvo[idx] as { content: string }).content;
              (visionConvo[idx] as unknown as { content: unknown }).content = [
                { type: 'image_url', image_url: { url: `data:${f.mime};base64,${b64}` } },
                { type: 'text', text: `${text} (the customer attached this image; look at it and respond to what it shows — a product, a design/artwork, a reference box, etc.)` },
              ];
            }
          } catch (err) {
            alog(`image attach failed: ${(err as Error).message}`);
          }
        }
      }

      const lastQuestions = convo.filter((m) => m.role === 'user').slice(-2).map((m) => (m as { content: string }).content).join(' ');
      const [knowledge, examples, voice] = await Promise.all([
        findRelevantKnowledge(deps, website.id, lastQuestions, 6000).catch(() => ''),
        similarExamples(deps, lastQuestions),
        voiceSample(deps, botSenderFor(conversationId).name, website, convo),
      ]);

      typing(true);
      let reply: string | null = null;
      const lastTwoUser = convo.filter((m) => m.role === 'user').slice(-2).map((m) => (typeof (m as { content: unknown }).content === 'string' ? (m as { content: string }).content : '')).join(' ');
      const romanUrdu = (lastTwoUser.match(/\b(mujhe|mujhay|chahiye|chahiy|chaiye|chahta|chahti|kya|kia|hogi|hoga|hai|hain|bhej|bhejo|bhejdo|dikhao|dikha|karo|kero|kardo|theek|thik|nahi|nai|kitna|kitne|kitni|mera|meri|aap|apko|aapko|hum|hamein|sakta|sakte|sakti|wala|wali|liye|batao|bta|btao|zaroorat|shukriya|acha|accha|janab|bhai|kitnay|dena|dedo|chahiyein)\b/gi) ?? []).length >= 2;
      const lastUserOnly = convo.filter((m) => m.role === 'user').slice(-1).map((m) => (typeof (m as { content: unknown }).content === 'string' ? (m as { content: string }).content : '')).join(' ');
      const priceIntent = PRICE_INTENT_RE.test(lastTwoUser);
      // Prices are never revealed in chat (owner's decision); ALEX_PRICE_TALK=on would attach past quotes for ballparks.
      const quotesBlock = priceIntent && process.env.ALEX_PRICE_TALK === 'on' ? pastQuotesFor(loadPersona(deps).prices, convo.filter((m) => m.role === 'user').map((m) => (typeof (m as { content: unknown }).content === 'string' ? (m as { content: string }).content : '')).join(' ')) : '';
      // Think (Qwen reasoning) on hard turns: money, complaints, deadlines, long messages.
      const hardTurn = priceIntent || /\b(complain|refund|wrong|not happy|cancel|deadline|urgent|rush|beat|match|compare|why)\b/i.test(lastUserOnly) || lastUserOnly.split(/\s+/).length > 30;
      if (hardTurn) alog(`hard turn: thinking on${quotesBlock ? ', past quotes attached' : ''}`);
      const systemText = buildSystem(deps, botSenderFor(conversationId).name, website, visitor ?? null, knowledge, examples, voice, quotesBlock)
        + (romanUrdu ? '\nLANGUAGE: the customer writes Roman Urdu (Urdu in Latin letters). Reply in natural, short Roman Urdu (e.g. "Ji, 100 boxes 8x8x3 inch ke liye quote team bhejegi. Aap ka email?"), keeping product terms in English. Never switch to English while they write Roman Urdu. Quote ke liye hamesha kaho "team aap ko email par exact quote bhejegi"; kabhi na kaho ke quote bhej diya gaya hai.' : '');
      if (romanUrdu) alog('customer writes Roman Urdu');
      const messages: ChatMsg[] = [{ role: 'system', content: systemText }, ...visionConvo];
      if (romanUrdu) messages.push({ role: 'user', content: '[Reply in Roman Urdu (Latin letters), one or two short sentences, product terms in English.]' });
      if (opts?.followUp) messages.push({ role: 'user', content: '[The customer sent this while you were still replying; your previous reply is already delivered. Answer only what is NEW in one short sentence and do not repeat a question you just asked.]' });
      try {
        for (let round = 0; round <= MAX_TOOL_ROUNDS; round++) {
          const t0 = Date.now();
          let out = await chat(deps, messages, round < MAX_TOOL_ROUNDS && !ackAfterMockup, undefined, undefined, hardTurn && round === 0);
          if (round === 0 && !ackAfterMockup && (!out.tool_calls || out.tool_calls.length === 0)) {
            const lastUserMsg = (([...convo].reverse().find((m) => m.role === 'user') as { content?: unknown })?.content ?? '') as unknown;
            const lu = typeof lastUserMsg === 'string' ? lastUserMsg : '';
            const prevBotText = ((([...convo].reverse().find((m) => m.role === 'assistant') as { content?: unknown })?.content ?? '') as unknown);
            const prevBot = typeof prevBotText === 'string' ? prevBotText : '';
            const hadMockup = convo.some((m) => m.role === 'assistant' && typeof (m as { content: unknown }).content === 'string' && /photo was sent to the customer here: mockup/.test((m as { content: string }).content));
            const revisionAsk = hadMockup && /\b(instead( of)?|change|replace|update|modify|edit|make (it|the|them)|put|print|add|remove|swap|redo|regenerate|try again|another (one|try|version)|new mockup|different (colou?r|text|font|logo|size|shape|finish)|in (gold|silver|black|white|red|blue|green|pink|matte|glossy))\b/i.test(lu);
            const yesToMockup = lu.trim().split(/\s+/).length <= 4 && /^(yes|yeah|yep|yup|sure|ok(ay)?|please|go ahead|do it|generate|make it)\b/i.test(lu.trim()) && /\b(want|would you like|shall i|should i|can i|like me to|like to see|interested in)\b[^?]*\b(mock-?up|visual|render|concept)\b[^?]*\?/i.test(prevBot);
            const promised = /\b(i('ll| will| can)|let me|i am going to|i'm going to)\b[^.!?]*\b(generate|create|update|make|prepare|render|work on)\b[^.!?]*\b(mock-?up|visual|render|concept|design)\b/i.test(out.content ?? '');
            const claimsMockup = /\b(here is|here's|here are|attached|sending)\b[^.!?]*\b(mock-?up|render|concept|visual)\b/i.test(out.content ?? '');
            const complaintEarly = /\b(you (are|were|said|sent|keep|gave|just)|asked for|i asked|i said|not what|wrong|didn'?t|don'?t|why|instead of)\b/i.test(lu);
            const wantsMockup = !complaintEarly && (/\b(mock-?up|concept|render|3d|visuali[sz]e|design (it|this|one)|how (it|this) (would|will) look)\b/i.test(lu) || revisionAsk || yesToMockup || promised || claimsMockup);
            const hadPhoto = convo.some((m) => m.role === 'assistant' && typeof (m as { content: unknown }).content === 'string' && /photo was sent to the customer here:/.test((m as { content: string }).content));
            const PRODUCT_RE = /\b(box|boxes|mailer|mailers|pouch|pouches|sleeve|sleeves|label|labels|bag|bags|carton|cartons|tube|tubes|tray|trays|insert|inserts|packaging|display)\b/i;
            const shortLu = lu.trim().split(/\s+/).length <= 10;
            const claimsPhoto = /\b(here is|here's|here are|attached|sending|i've sent|i have sent|sent you)\b[^.!?]*\b(photo|picture|image|pic|sample|example|option)s?\b/i.test(out.content ?? '')
              // Roman Urdu promise: "photo bhejta hoon", "tasveer dikhata hoon", "bhej raha hoon"
              || (/\b(photo|picture|image|pic|tasveer|tasvir|sample)s?\b/i.test(out.content ?? '') && /\b(bhejt[ai]|bhej (raha|rahi)|dikhat[ai]|dikha (raha|rahi)|send kart[ai]|send kar (raha|rahi))\b/i.test(out.content ?? ''));
            const complaint = /\b(you (are|were|said|sent|keep|gave|just)|asked for|i asked|i said|not what|wrong|didn'?t|don'?t|why|instead of)\b/i.test(lu);
            const physicalSample = /\b(physical|real|actual|hard|printed|free) samples?\b|\bsamples? (to|at) my\b|\b(ship|mail|post|courier|deliver) (me )?(a |some |the )?samples?\b|\bsend (me )?(a |some |the )?samples? (to|at|by|home)\b/i.test(lu);
            const morePhotos = hadPhoto && shortLu && /\b(more|another|other|next|different|else|again)\b/i.test(lu);
            const wantsPhoto = !complaint && !physicalSample && (morePhotos || (/\b(photo|picture|pic|image|example|sample|tasveer|tasvir)s?\b/i.test(lu) && (/\b(show|see|have|send|share|look|of|bhej\w*|dikha\w*|dekh\w*|chahiye|chaiye|chahye|do|dein|den|hai|ha|ki|ka)\b/i.test(lu) || hadPhoto))
              || /\bshow me\b/i.test(lu)
              || (hadPhoto && shortLu && PRODUCT_RE.test(lu) && !wantsMockup)
              || claimsPhoto);
            if (complaintEarly && (claimsMockup || claimsPhoto)) {
              // The customer is correcting us and the draft pretends to send something: answer honestly instead.
              alog('complaint + false send claim: re-asking without tools');
              out = await chat(deps, [...messages, { role: 'user', content: '[Nothing was sent. Apologise in a few words, answer the customer directly, and do not claim to send or show anything.]' }], false);
            } else if (wantsMockup || wantsPhoto) {
              alog(`forcing tool ${wantsMockup ? 'make_mockup' : 'send_photo'}${revisionAsk ? ' (revision)' : ''}${yesToMockup ? ' (yes)' : ''}${promised ? ' (promised)' : ''}${claimsPhoto ? ' (claimed photo)' : ''}`);
              out = await chat(deps, messages, true, undefined, wantsMockup ? 'make_mockup' : 'send_photo');
            }
          }
          alog(`llm round ${round} ${Date.now() - t0}ms tools=${out.tool_calls?.length ?? 0} content=${(out.content ?? '').slice(0, 80)}`);
          if (!out.tool_calls || out.tool_calls.length === 0) {
            reply = out.content?.trim() || null;
            break;
          }
          messages.push({ role: 'assistant', content: out.content ?? null, tool_calls: out.tool_calls });
          for (const call of out.tool_calls.slice(0, 2)) {
            // Stop if a human took over while a tool (mockup) was running.
            const fresh = await deps.db.get<ConvRow>('SELECT id, status FROM conversations WHERE id = ?', [conversationId]);
            if (!fresh || fresh.status !== 'WAITING') return;
            const convoText = convo.slice(-10).map((m) => `${m.role === 'user' ? 'Customer' : 'Agent'}: ${typeof (m as { content: unknown }).content === 'string' ? (m as { content: string }).content : '[image]'}`).join('\n');
            const result = await runTool(deps, conversationId, call, lastQuestions, deps.config.alex.watermarkText || website.name, convoText);
            messages.push({ role: 'tool', tool_call_id: call.id, content: result });
          }
        }
        // After tool calls the model sometimes returns an empty message: ask it
        // once more, without tools, for the customer-facing reply.
        if (!reply && messages.some((m) => m.role === 'tool')) {
          const final = await chat(
            deps,
            [...messages, { role: 'user', content: '[Reply to the customer now in 1-3 sentences, no tool calls.]' }],
            false,
          );
          reply = final.content?.trim() || null;
        }
      } finally {
        typing(false);
      }
      alog(`draft: ${(reply ?? '').slice(0, 100)}`);
      // The customer asked for a photo and got a rendered picture: never call it a mockup.
      if (reply && photoFallback.delete(conversationId)) {
        reply = reply.replace(/\b(a |the |your |this |that )?(mock-?ups?|renders?|renderings?|design concepts?)\b/gi, (_m, art) => `${art ?? ''}picture`).replace(/\b(generated|rendered|created) (picture|image)\b/gi, 'picture');
      }
      if (!reply) return;
      if (!messages.some((m) => m.role === 'tool') && /\b(i('ve| have)? (just )?(updated|changed|revised|regenerated|re-?rendered|adjusted|generated|created|sent|shared|attached|uploaded))\b/i.test(reply)) {
        alog('false update/send claim without a tool: re-asking');
        try {
          const fixed = await chat(deps, [...messages, { role: 'user', content: '[Nothing was changed, generated or sent in this turn. Answer plainly: explain, or offer a new mockup (which only happens when you call make_mockup). Never claim an update you did not make.]' }], false);
          if (fixed.content?.trim()) reply = fixed.content.trim();
        } catch { /* keep the draft */ }
      }
      // Keep the conversation moving: if the reply has no next step and the customer
      // is not closing, add one short follow-up question (missing detail / next action).
      {
        const lastUserText = ((([...convo].reverse().find((m) => m.role === 'user') as { content?: unknown })?.content ?? '') as string | unknown[]);
        const lastText = typeof lastUserText === 'string' ? lastUserText : '';
        const closing =
          /\b(thanks?|thank you|thx|bye|goodbye|that'?s (all|it)|no thanks|not now|maybe later|talk later|will get back|get back to you)\b/i.test(lastText) ||
          /\b(have a (great|good|wonderful|nice) (day|one|evening|night)|goodbye|take care)\b/i.test(reply) ||
          lastText.trim().length <= 3;
        const usedTool = messages.some((m) => m.role === 'tool');
        if (!hasQuestion(reply) && !closing && !usedTool) {
          try {
            const asked = convo.filter((m) => m.role === 'assistant').map((m) => (m as { content: string | null }).content ?? '').join('\n');
            const follow = await chat(deps, [
              { role: 'system', content: `You are ${botSenderFor(conversationId).name}, a packaging sales agent. Write ONE short next-step question (max 15 words) that moves this chat forward: ask for a missing detail (box type, size L x W x H, quantity, material, printing/finish, deadline, name/email) or offer to show a photo/mockup. Never repeat a question already asked. Output only the question.` },
              { role: 'user', content: `Chat so far:\n${convo.map((m) => `${m.role === 'user' ? 'Customer' : 'Agent'}: ${typeof (m as { content: unknown }).content === 'string' ? (m as { content: string }).content : '[image]'}`).join('\n')}\nAgent (new reply): ${reply}\n\nQuestions already asked:\n${asked}` },
            ], false);
            const q = (follow.content ?? '').trim().split('\n').map((l) => l.trim()).filter(Boolean)[0] ?? '';
            alog(`next-step raw: ${(follow.content ?? '').slice(0, 120)}`);
            if (q && q.length < 160 && hasQuestion(q)) { reply = `${reply}\n${q}`; alog(`added next step: ${q}`); }
          } catch (err) {
            alog(`next-step failed: ${(err as Error).message}`);
          }
        }
      }
      // Alex cannot send emails: rewrite any such claim to what really happens.
      {
        const parts = sentencesOf(reply);
        const fixed = parts.map((p) => (/\b(i('| wi)ll|i will|let me|i just|i have|i've|i am going to|i'm going to)\b[^.!?]*\b(send|sent|share|shoot|drop)[^.!?]*\b(e-?mail|inbox|test e-?mail)\b/i.test(p) || /\btest e-?mail\b/i.test(p) || /\b(i('ll| will| am|'m)?|let me) (e-?mail|mail)(ing)? (you|the|your|it|them|a)\b/i.test(p))
          ? 'Our team will email you the exact quote. '
          : p);
        const joined = fixed.join('').replace(/(Our team will email you the exact quote\. ?){2,}/g, 'Our team will email you the exact quote. ').trim();
        if (joined !== reply) { alog('rewrote email claim'); reply = joined; }
      }
      // Remove hallucinated attachment pseudo-tags.
      reply = reply.replace(/\(?\s*a photo was sent to the customer here:[^)\n]*\)?/gi, ' ').replace(/\s*[\[(]\s*(sent|attached|attaching|sending|photo|image|mockup)[^\])]*(image|photo|mockup|file|jpg|png)[^\])]*[\])]\s*/gi, ' ').replace(/[ \t]{2,}/g, ' ').trim();
      if (!reply) return;
      // Drop any question sentence we already asked earlier in this chat (models love to loop on it).
      {
        const askedBefore = new Set(
          convo.filter((m) => m.role === 'assistant').flatMap((m) => sentencesOf((m as { content: string | null }).content ?? '')).map((q) => norm(q)).filter((q) => q.length > 8),
        );
        const askedList = [...askedBefore].filter((q) => q.split(' ').length >= 3);
        const kept = sentencesOf(reply).filter((part) => {
          if (!/\?\s*$/.test(part)) return true;
          const n = norm(part);
          return !(askedBefore.has(n) || askedList.some((q) => n.includes(q) || q.includes(n)));
        });
        const trimmed = kept.join('').replace(/\n{3,}/g, '\n\n').trim();
        if (trimmed.length >= 8 && kept.length < sentencesOf(reply).length) { alog(`dropped repeated question`); reply = trimmed; }
      }
      if (process.env.ALEX_STYLE_MODE === 'post') {
        const styled = await styledReply(deps, botSenderFor(conversationId).name, website, convo, reply, faqFor(loadPersona(deps).faq, website.name));
        if (styled) reply = styled;
      }

      // One question per message: drop any further question sentences.
      {
        const parts = sentencesOf(reply);
        let seen = false;
        const kept = parts.filter((p) => {
          if (!/\?\s*$/.test(p)) return true;
          if (seen) return false;
          seen = true;
          return true;
        });
        if (kept.length < parts.length) { reply = kept.join('').replace(/\b(Also|Additionally|And),?\s*$/i, '').trim(); alog('trimmed extra questions'); }
      }
      // After a photo/mockup: one short line + one question, nothing more.
      if (messages.some((m) => m.role === 'tool' && /^(Photo sent|Mockup .*sent)/.test((m as { content: string }).content))) {
        const parts = sentencesOf(reply);
        const stmt = parts.find((p) => !/\?\s*$/.test(p));
        const q = parts.find((p) => /\?\s*$/.test(p));
        const short = [stmt, q].filter(Boolean).join('').trim();
        if (short && short !== reply.trim()) { reply = short; alog('shortened caption'); }
      }
      // Real agents write short messages: compress anything over ~28 words.
      if (reply.split(/\s+/).length > 22) {
        try {
          const short = await chat(deps, [
            { role: 'system', content: 'Shorten this live-chat reply to at most 18 words: plain words, keep its meaning and its ONE question if it has one, no new facts, no greetings. Output only the shortened reply.' },
            { role: 'user', content: reply },
          ], false);
          const t = (short.content ?? '').trim().replace(/^["']|["']$/g, '');
          if (t && t.split(/\s+/).length < reply.split(/\s+/).length && (!hasQuestion(reply) || hasQuestion(t))) { alog(`compressed ${reply.split(/\s+/).length}→${t.split(/\s+/).length} words`); reply = t; }
        } catch (err) {
          alog(`compress failed: ${(err as Error).message}`);
        }
      }
      // The compressor can reintroduce a second question: keep only the first one.
      {
        const parts = sentencesOf(reply);
        let seen = false;
        const kept = parts.filter((p) => { if (!/\?\s*$/.test(p)) return true; if (seen) return false; seen = true; return true; });
        if (kept.length < parts.length) reply = kept.join('').replace(/\b(Also|Additionally|And),?\s*$/i, '').trim();
      }
      // Agents don't use emojis in replies.
      reply = reply.replace(/[\u{1F300}-\u{1FAFF}\u{2600}-\u{27BF}\u{FE0F}\u{1F900}-\u{1F9FF}]/gu, '').replace(/[ \t]{2,}/g, ' ').replace(/\s+([.!?,])/g, '$1');
      // Agents write tight messages: no blank lines between sentences/paragraphs.
      reply = reply.replace(/[ \t]+\n/g, '\n').replace(/\n{2,}/g, '\n').trim();

      const fresh = await deps.db.get<ConvRow>('SELECT id, status FROM conversations WHERE id = ?', [conversationId]);
      if (!fresh || fresh.status !== 'WAITING') return;
      await postMessage(deps, { conversationId, senderType: 'BOT', body: reply });
      alog(`posted: ${reply.slice(0, 80)}`);

      // Safety net: if Alex *said* it will hand over (or the customer left contact
      // details / asked for a quote) but never called the tool, notify the team anyway.
      const handedOver = messages.some((m) => m.role === 'tool' && (m as { content: string }).content.startsWith('The team has been notified'));
      if (!handedOver) await maybeAutoHandover(deps, conversationId, convo, reply);
    } catch (err) {
      alog(`turn FAILED: ${(err as Error).stack ?? (err as Error).message}`);
      if (isPodError(err) && fallback) {
        alexDownUntil = Date.now() + 60_000;
        alog('pod unreachable → legacy greeter fallback for 60 s');
        inFlight.delete(conversationId);
        fallback();
      }
    } finally {
      inFlight.delete(conversationId);
      if (pendingTurn.delete(conversationId)) alexTurn(deps, conversationId, fallback, { followUp: true });
    }
  })();
}
