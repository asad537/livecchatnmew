import {
  AGENT_NAMESPACE,
  WIDGET_NAMESPACE,
  EV,
  type CallKind,
  type CallMeta,
  type CallProvider,
  type CallStatus,
  type ChatMessage,
  type FileMeta,
  type MessageKind,
  type ScanStatus,
  type SenderType,
} from '@livechat/shared';
import type { AppDeps } from '../core/deps.js';
import { newId, nowIso } from '../core/db.js';
import { emitInboxUpdate } from './conversations.js';
import { applyTranslation } from '../features/translate/index.js';

// ─── Row shapes (snake_case DB columns) ──────────────────────

/** snake_case columns of the `messages` table. */
export interface MessageRow {
  id: string;
  conversation_id: string;
  sender_type: SenderType;
  sender_user_id: string | null;
  body: string | null;
  kind: MessageKind;
  file_id: string | null;
  call_id: string | null;
  created_at: string;
  delivered_at: string | null;
  read_at: string | null;
  translated_body?: string | null;
  orig_lang?: string | null;
}

interface FileRow {
  id: string;
  conversation_id: string;
  original_name: string;
  mime: string;
  size: number;
  scan_status: ScanStatus;
}

interface CallRow {
  id: string;
  conversation_id: string;
  kind: CallKind;
  provider: CallProvider;
  status: CallStatus;
  room_url: string | null;
  started_by: string;
  created_at: string;
  ended_at: string | null;
}

interface SenderRow {
  id: string;
  name: string;
  avatar_color: string;
  avatar_url?: string | null;
}

interface ConversationRefRow {
  id: string;
  website_id: string;
  visitor_id: string;
  status: string;
  assigned_user_id: string | null;
}

export interface PostMessageInput {
  conversationId: string;
  senderType: SenderType;
  senderUserId?: string | null;
  body: string;
  kind?: MessageKind; // default 'TEXT'
  fileId?: string | null;
  callId?: string | null;
  tempId?: string;
  /** Override the stored timestamp — e.g. a join notice backdated a moment so
      it always sorts above the message that triggered it. */
  createdAt?: string;
  /** Agent-facing note (e.g. "Visitor left the site") — stored + shown to
      agents, but never broadcast to or loaded by the visitor's widget. */
  agentOnly?: boolean;
}

// ─── Helpers ─────────────────────────────────────────────────

function placeholders(count: number): string {
  return Array.from({ length: count }, () => '?').join(',');
}

function unique(values: (string | null)[]): string[] {
  return [...new Set(values.filter((v): v is string => !!v))];
}

function toFileMeta(row: FileRow): FileMeta {
  return {
    id: row.id,
    originalName: row.original_name,
    mime: row.mime,
    size: Number(row.size),
    scanStatus: row.scan_status,
    downloadUrl: `/api/files/${row.id}/download`,
  };
}

function toCallMeta(row: CallRow): CallMeta {
  return {
    id: row.id,
    conversationId: row.conversation_id,
    kind: row.kind,
    provider: row.provider,
    status: row.status,
    roomUrl: row.room_url,
    startedBy: row.started_by,
    createdAt: row.created_at,
    endedAt: row.ended_at,
  };
}

/** Display identity for AI-greeter messages (sender_type = 'BOT'). */
export const BOT_SENDER = { name: 'Assistant', avatarColor: '#16a34a' }; // name set from config.alex.names[0] at boot
/** All bot display names; a conversation keeps one name for its whole life (hash of its id). */
export const BOT_NAMES: string[] = [];
const BOT_COLORS = ['#16a34a', '#0ea5e9', '#f59e0b', '#8b5cf6', '#ef4444', '#14b8a6'];
export function botSenderFor(conversationId: string): { name: string; avatarColor: string } {
  if (BOT_NAMES.length === 0) return BOT_SENDER;
  // FNV-1a: evenly spread even for v4 UUIDs (a plain *31 hash was biased).
  let h = 0x811c9dc5;
  for (let i = 0; i < conversationId.length; i++) h = Math.imul(h ^ conversationId.charCodeAt(i), 0x01000193) >>> 0;
  const idx = h % BOT_NAMES.length;
  return { name: BOT_NAMES[idx], avatarColor: BOT_COLORS[idx % BOT_COLORS.length] };
}

// ─── Hydration ───────────────────────────────────────────────

/** Attach file / call / sender info to raw message rows → ChatMessage[]. */
export async function hydrateMessages(deps: AppDeps, rows: MessageRow[]): Promise<ChatMessage[]> {
  if (rows.length === 0) return [];

  const fileIds = unique(rows.map((r) => r.file_id));
  const callIds = unique(rows.map((r) => r.call_id));
  const senderIds = unique(rows.map((r) => r.sender_user_id));

  const [fileRows, callRows, senderRows] = await Promise.all([
    fileIds.length
      ? deps.db.all<FileRow>(
          `SELECT * FROM files WHERE id IN (${placeholders(fileIds.length)})`,
          fileIds,
        )
      : Promise.resolve([] as FileRow[]),
    callIds.length
      ? deps.db.all<CallRow>(
          `SELECT * FROM calls WHERE id IN (${placeholders(callIds.length)})`,
          callIds,
        )
      : Promise.resolve([] as CallRow[]),
    senderIds.length
      ? deps.db.all<SenderRow>(
          `SELECT id, name, avatar_color, avatar_url FROM users WHERE id IN (${placeholders(senderIds.length)})`,
          senderIds,
        )
      : Promise.resolve([] as SenderRow[]),
  ]);

  const files = new Map(fileRows.map((r) => [r.id, toFileMeta(r)]));
  const calls = new Map(callRows.map((r) => [r.id, toCallMeta(r)]));
  const senders = new Map(
    senderRows.map((r) => [
      r.id,
      { name: r.name, avatarColor: r.avatar_color, avatarUrl: r.avatar_url ?? null },
    ]),
  );

  return rows.map((row) => ({
    id: row.id,
    conversationId: row.conversation_id,
    senderType: row.sender_type,
    senderUserId: row.sender_user_id,
    body: row.body ?? '',
    kind: row.kind,
    fileId: row.file_id,
    callId: row.call_id,
    createdAt: row.created_at,
    deliveredAt: row.delivered_at,
    readAt: row.read_at,
    translatedBody: row.translated_body ?? null,
    origLang: row.orig_lang ?? null,
    file: row.file_id ? (files.get(row.file_id) ?? null) : null,
    call: row.call_id ? (calls.get(row.call_id) ?? null) : null,
    sender:
      row.sender_type === 'BOT'
        ? botSenderFor(row.conversation_id)
        : row.sender_user_id
          ? (senders.get(row.sender_user_id) ?? null)
          : null,
  }));
}

// ─── Posting ─────────────────────────────────────────────────

/**
 * Insert a message, hydrate it, broadcast `EV.ChatMessage` to the conversation
 * room in BOTH namespaces, mark it delivered when the counterpart is online
 * (+ `EV.ChatReceipt`) and refresh the agent inbox. Returns the hydrated
 * message with the caller's `tempId` echoed for optimistic UI reconciliation.
 */
export async function postMessage(deps: AppDeps, input: PostMessageInput): Promise<ChatMessage> {
  const conv = await deps.db.get<ConversationRefRow>(
    'SELECT id, website_id, visitor_id, status, assigned_user_id FROM conversations WHERE id = ?',
    [input.conversationId],
  );
  if (!conv) throw new Error('Conversation not found');

  const id = newId();
  const agentOnly = input.agentOnly ? 1 : 0;
  await deps.db.run(
    `INSERT INTO messages
       (id, conversation_id, sender_type, sender_user_id, body, kind, file_id, call_id, created_at, delivered_at, read_at, agent_only)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, ?)`,
    [
      id,
      input.conversationId,
      input.senderType,
      input.senderUserId ?? null,
      input.body,
      input.kind ?? 'TEXT',
      input.fileId ?? null,
      input.callId ?? null,
      input.createdAt ?? nowIso(),
      agentOnly,
    ],
  );

  const row = await deps.db.get<MessageRow>('SELECT * FROM messages WHERE id = ?', [id]);
  if (!row) throw new Error('Failed to load inserted message');

  const [message] = await hydrateMessages(deps, [row]);
  if (input.tempId) message.tempId = input.tempId;

  // Delivery: is the counterpart currently online?
  const counterpartOnline =
    input.senderType === 'VISITOR'
      ? conv.assigned_user_id !== null && deps.presence.isAgentOnline(conv.assigned_user_id)
      : deps.presence.isVisitorOnline(conv.visitor_id);

  if (counterpartOnline) {
    const deliveredAt = nowIso();
    await deps.db.run('UPDATE messages SET delivered_at = ? WHERE id = ?', [deliveredAt, id]);
    message.deliveredAt = deliveredAt;
  }

  const room = `conv:${conv.id}`;
  // Agent-only notes go to agents only — the visitor's widget never receives them.
  if (!input.agentOnly) {
    deps.io.of(WIDGET_NAMESPACE).to(room).emit(EV.ChatMessage, { message });
  }
  deps.io.of(AGENT_NAMESPACE).to(room).emit(EV.ChatMessage, { message });

  if (message.deliveredAt) {
    const receipt = {
      conversationId: conv.id,
      messageIds: [id],
      deliveredAt: message.deliveredAt,
    };
    deps.io.of(WIDGET_NAMESPACE).to(room).emit(EV.ChatReceipt, receipt);
    deps.io.of(AGENT_NAMESPACE).to(room).emit(EV.ChatReceipt, receipt);
  }

  await emitInboxUpdate(deps, conv.id);

  // Cross-language chat: translate in the background and patch the bubble in
  // (fire-and-forget so message delivery itself is never delayed).
  if (!input.agentOnly && (input.senderType === 'VISITOR' || input.senderType === 'AGENT')) {
    void applyTranslation(deps, message, {
      website_id: conv.website_id,
      visitor_id: conv.visitor_id,
    }).catch((err: unknown) => console.warn('[translate] apply failed:', (err as Error).message));
  }

  return message;
}
