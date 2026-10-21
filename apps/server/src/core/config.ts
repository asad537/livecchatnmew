import path from 'node:path';
import { fileURLToPath } from 'node:url';

const serverRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../..');

export interface Config {
  port: number;
  publicUrl: string;
  jwtSecret: string;
  databaseUrl: string;      // 'file:...' → SQLite, 'postgres://...' → PostgreSQL
  redisUrl: string | null;
  dbPoolSize: number;
  dailyApiKey: string | null;
  anthropicApiKey: string | null;
  aiProvider: 'anthropic' | 'groq' | 'gemini' | 'builtin';
  aiApiKey: string | null;
  aiModel: string;
  aiGreeter: boolean;
  /** Alex — self-hosted AI agent on the GPU pod (OpenAI-compatible vLLM + ComfyUI). */
  alex: {
    baseUrl: string | null;
    apiKey: string | null;
    model: string;
    name: string;
    fullAuto: boolean;
    comfyUrl: string | null;
    /** Optional style LoRA (file name in ComfyUI models/loras) trained on the company's product photos. */
    mockupLora: string | null;
    mockupLoraStrength: number;
    /** Text stamped on every image Alex sends (defaults to the website name). */
    watermarkText: string | null;
    dataDir: string;
    /** Fine-tuned "voice" model (LoRA-trained on real agent chats); optional. */
    styleBaseUrl: string | null;
    styleApiKey: string | null;
    styleModel: string;
    /** Display names rotated per conversation (first = default). */
    names: string[];
    /** Greet visitors proactively a few seconds after they arrive. */
    proactive: boolean;
    proactiveDelayMs: number;
  };
  autoTranslate: boolean;   // cross-language chat: translate visitor↔agent (uses the AI engine)
  smtpHost: string | null;
  smtpPort: number;
  smtpUser: string | null;
  smtpPass: string | null;
  smtpFrom: string | null;
  s3Bucket: string | null;
  scanMode: 'permissive' | 'strict';
  storageDir: string;
  serverRoot: string;
  repoRoot: string;
}

/**
 * AI provider resolution:
 *  - AI_PROVIDER=groq|gemini|anthropic picks explicitly (AI_API_KEY for groq/gemini)
 *  - else ANTHROPIC_API_KEY → anthropic; else AI_API_KEY → groq; else builtin replies
 */
function resolveAi(env: NodeJS.ProcessEnv): {
  aiProvider: 'anthropic' | 'groq' | 'gemini' | 'builtin';
  aiApiKey: string | null;
  aiModel: string;
} {
  const raw = (env.AI_PROVIDER || '').toLowerCase();
  const aiApiKey = env.AI_API_KEY || null;
  const aiProvider =
    raw === 'groq' || raw === 'gemini' || raw === 'anthropic'
      ? raw
      : env.ANTHROPIC_API_KEY
        ? 'anthropic'
        : aiApiKey
          ? 'groq'
          : 'builtin';
  const defaultModel =
    aiProvider === 'groq'
      ? 'openai/gpt-oss-120b'
      : aiProvider === 'gemini'
        ? 'gemini-2.5-flash'
        : 'claude-opus-5';
  return { aiProvider, aiApiKey, aiModel: env.AI_MODEL || defaultModel };
}

export function loadConfig(): Config {
  // Load repo-root .env (Node 20.12+ built-in; no dotenv dependency needed)
  try {
    process.loadEnvFile(path.resolve(serverRoot, '../../.env'));
  } catch {
    /* .env is optional */
  }
  const env = process.env;
  return {
    port: Number(env.PORT || 4000),
    publicUrl: env.PUBLIC_URL || `http://localhost:${Number(env.PORT || 4000)}`,
    jwtSecret: env.JWT_SECRET || 'dev-secret-do-not-use-in-production',
    databaseUrl: env.DATABASE_URL || 'mysql://root@127.0.0.1:3306/livechat',
    redisUrl: env.REDIS_URL || null,
    // Sized for ~60 concurrent agents bursting at once (MySQL default
    // max_connections is 151, so 40 leaves plenty of headroom).
    dbPoolSize: Number(env.DB_POOL_SIZE || 40),
    dailyApiKey: env.DAILY_API_KEY || null,
    anthropicApiKey: env.ANTHROPIC_API_KEY || null,
    ...resolveAi(env),
    aiGreeter: env.AI_GREETER !== 'off',
    alex: {
      baseUrl: (env.ALEX_BASE_URL || '').replace(/\/+$/, '') || null,
      apiKey: env.ALEX_API_KEY || null,
      model: env.ALEX_MODEL || 'cyankiwi/Qwen3.5-35B-A3B-AWQ-4bit',
      name: env.ALEX_NAME || 'Alex',
      fullAuto: env.ALEX_FULL_AUTO !== 'off',
      comfyUrl: (env.ALEX_COMFY_URL || '').replace(/\/+$/, '') || null,
      mockupLora: env.ALEX_MOCKUP_LORA || null,
      mockupLoraStrength: Number(env.ALEX_MOCKUP_LORA_STRENGTH || 0.8),
      watermarkText: env.ALEX_WATERMARK_TEXT || null,
      dataDir: env.ALEX_DATA_DIR || path.resolve(serverRoot, '../../alex'),
      styleBaseUrl: (env.ALEX_STYLE_BASE_URL || '').replace(/\/+$/, '') || null,
      styleApiKey: env.ALEX_STYLE_API_KEY || null,
      styleModel: env.ALEX_STYLE_MODEL || 'alex-4b',
      names: (env.ALEX_NAMES || env.ALEX_NAME || 'Alex,Sara,Daniel,Emma').split(',').map((n) => n.trim()).filter(Boolean),
      proactive: env.ALEX_PROACTIVE !== 'off',
      proactiveDelayMs: Number(env.ALEX_PROACTIVE_DELAY_MS || 6000),
    },
    // Auto-translate is on by default (turns itself off when no AI engine is
    // configured — see features/translate). Set AUTO_TRANSLATE=off to disable.
    autoTranslate: env.AUTO_TRANSLATE !== 'off',
    smtpHost: env.SMTP_HOST || null,
    smtpPort: Number(env.SMTP_PORT || 587),
    smtpUser: env.SMTP_USER || null,
    smtpPass: env.SMTP_PASS || null,
    smtpFrom: env.SMTP_FROM || null,
    s3Bucket: env.S3_BUCKET || null,
    scanMode: env.SCAN_MODE === 'strict' ? 'strict' : 'permissive',
    storageDir: env.STORAGE_DIR || path.join(serverRoot, 'storage'),
    serverRoot,
    repoRoot: path.resolve(serverRoot, '../..'),
  };
}
