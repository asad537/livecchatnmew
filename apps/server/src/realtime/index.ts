import type { Namespace, Socket } from 'socket.io';
import {
  AGENT_NAMESPACE,
  WIDGET_NAMESPACE,
  EV,
  type Role,
  type Team,
  type Visitor,
  type Website,
} from '@livechat/shared';
import type { AppDeps } from '../core/deps.js';
import { newId, nowIso } from '../core/db.js';
import {
  signVisitorToken,
  toUserPublic,
  verifyToken,
  type AgentTokenPayload,
  type UserRow,
  type VisitorTokenPayload,
} from '../core/auth.js';
import { hydrateMessages, postMessage, type MessageRow } from '../domain/messages.js';
import { maybeBotReply } from '../features/aibot/index.js';
import { postGreeting } from '../features/alex/index.js';
import { sendTranscriptEmail } from '../features/email/index.js';
import { captureVisitorInfo } from '../features/capture/index.js';
import { clientIp, localCountry, lookupCountry, updateVisitorGeo } from '../features/geo/index.js';
import { ipAllowed } from '../core/ip.js';
import {
  activateConversation,
  closeConversation,
  drainQueue,
  emitConversationAgent,
  emitConversationStatus,
  emitInboxUpdate,
  isAnyHumanAvailable,
  hasCapacity,
  loadSummary,
  recordAssignment,
  setVisitorRefresh,
  toBranding,
  toVisitor,
  transferConversation,
  userCanAccessWebsite,
  type ConversationRow,
  type VisitorRow,
  type WebsiteRow,
} from '../domain/conversations.js';
import {
  registerAgentCallHandlers,
  registerWidgetCallHandlers,
} from '../features/calls/index.js';

// ─── Socket data (pinned shapes, see SPEC "Socket rooms") ────

interface WidgetSocketData {
  visitorId: string;
  websiteId: string;
  conversationId: string | null;
  /** Cached country at handshake time — null means "resolve & recheck async". */
  geoCc: string | null;
}

interface AgentSocketData {
  userId: string;
  role: Role;
  name: string;
}

/** Extra per-socket context resolved during the handshake (not part of the pinned socket.data contract). */
const widgetCtx = new WeakMap<Socket, { visitorToken: string; page: string | null; websiteRow: WebsiteRow }>();
const agentCtx = new WeakMap<Socket, { user: UserRow }>();

const convRoom = (id: string): string => `conv:${id}`;
const userRoom = (id: string): string => `user:${id}`;
const websiteRoom = (id: string): string => `website:${id}`;

// A chat auto-ends only after this long with NO message from EITHER side.
const INACTIVITY_CLOSE_MS = 12 * 60 * 60 * 1000; // 12 hours
const INACTIVITY_SWEEP_MS = 15 * 60 * 1000; // re-check every 15 minutes
// Client's last message unanswered by any AGENT for this long → auto-close.
const UNANSWERED_CLOSE_MS = 60 * 60 * 1000; // 1 hour
const QUEUE_MESSAGE = "You're in the queue — an agent will be with you shortly.";
const VISITOR_LEFT_NOTICE = 'Visitor left the site';
const VISITOR_BACK_NOTICE = 'Visitor is back on the site';

// Scale/abuse guards
const MAX_MESSAGE_CHARS = 4000;
const MSG_RATE_MAX = 20; // messages per window per socket
const MSG_RATE_WINDOW_MS = 10_000;

/** Cheap sliding-window rate limiter, one per socket. */
function makeLimiter(max: number, windowMs: number): () => boolean {
  let count = 0;
  let resetAt = 0;
  return () => {
    const now = Date.now();
    if (now > resetAt) {
      count = 0;
      resetAt = now + windowMs;
    }
    return ++count <= max;
  };
}

// ─── Small utilities ─────────────────────────────────────────

function asString(value: unknown): string | null {
  return typeof value === 'string' && value.length > 0 ? value : null;
}

function asStringArray(value: unknown): string[] {
  return Array.isArray(value) ? value.filter((v): v is string => typeof v === 'string') : [];
}

/** Wrap a handler so a bad payload / runtime error never crashes the server. */
function safe(
  socket: Socket,
  handler: (...args: never[]) => void | Promise<void>,
): (...args: unknown[]) => void {
  return (...args: unknown[]) => {
    Promise.resolve()
      .then(() => (handler as (...a: unknown[]) => void | Promise<void>)(...args))
      .catch((err: unknown) => {
        const message = err instanceof Error ? err.message : 'Unexpected error';
        console.error('[realtime]', message);
        socket.emit(EV.AppError, { message });
      });
  };
}

function hostnameOf(origin: string | undefined): string | null {
  if (!origin) return null;
  try {
    return new URL(origin).hostname.toLowerCase();
  } catch {
    return null;
  }
}

// ─── Inactivity auto-close (12h) ─────────────────────────────
// A conversation ends on its own ONLY after 12 hours with no message from either
// side (agent/CSR or client), or an hour after an assigned agent left the
// client's last message unanswered. Either way it just becomes a normal CLOSED
// — there is no separate "missed" outcome. A periodic sweep is used instead of
// per-chat timers so it survives restarts.

async function sweepInactiveConversations(deps: AppDeps): Promise<void> {
  const closeAs = async (conversationId: string) => {
    await deps.db.run("UPDATE conversations SET status = 'CLOSED', closed_at = ? WHERE id = ?", [
      nowIso(),
      conversationId,
    ]);
    await emitConversationStatus(deps, conversationId);
    await emitInboxUpdate(deps, conversationId);
  };

  // Rule 1 — unanswered client: an ASSIGNED agent left the client's last
  // message without a reply for an hour → close the chat. (BOT/SYSTEM chatter
  // doesn't count as an answer.) Chats assigned to NOBODY are exempt — they
  // stay in the Offline Chats queue waiting to be picked up; only the 12h
  // total-silence rule can eventually end them.
  const unansweredCutoff = new Date(Date.now() - UNANSWERED_CLOSE_MS).toISOString();
  const unanswered = await deps.db.all<{ id: string }>(
    `SELECT c.id
       FROM conversations c
       JOIN (SELECT conversation_id, MAX(created_at) AS last_v FROM messages
              WHERE sender_type = 'VISITOR' GROUP BY conversation_id) v
         ON v.conversation_id = c.id
      WHERE c.status IN ('WAITING', 'OFFERED', 'ACTIVE')
        AND c.assigned_user_id IS NOT NULL
        AND v.last_v < ?
        AND NOT EXISTS (
          SELECT 1 FROM messages m
           WHERE m.conversation_id = c.id AND m.sender_type = 'AGENT' AND m.created_at >= v.last_v
        )
      LIMIT 500`,
    [unansweredCutoff],
  );
  for (const conv of unanswered) await closeAs(conv.id);

  // Rule 2 — total silence: nothing from either side for 12 hours → close.
  const cutoff = new Date(Date.now() - INACTIVITY_CLOSE_MS).toISOString();
  const stale = await deps.db.all<{ id: string; website_id: string }>(
    `SELECT c.id, c.website_id
       FROM conversations c
      WHERE c.status IN ('WAITING', 'OFFERED', 'ACTIVE')
        AND c.created_at < ?
        AND NOT EXISTS (
          SELECT 1 FROM messages m
           WHERE m.conversation_id = c.id AND m.created_at >= ?
        )
      LIMIT 500`,
    [cutoff, cutoff],
  );
  for (const conv of stale) {
    await closeAs(conv.id);
  }
}

// ─── Agent-outreach close timers (visitor left, never replied) ───────────────
// 15-minute grace after the visitor goes offline: the timer re-checks at fire
// time, so a visitor who came back (or finally replied) is left alone. No
// explicit cancel needed — the re-check covers every case.
const OUTREACH_CLOSE_MS = 15 * 60 * 1000;
const outreachTimers = new Map<string, NodeJS.Timeout>();

function scheduleOutreachClose(deps: AppDeps, conversationId: string, visitorId: string): void {
  if (outreachTimers.has(conversationId)) return; // already counting down
  const t = setTimeout(() => {
    outreachTimers.delete(conversationId);
    void (async () => {
      const conv = await deps.db.get<ConversationRow>(
        'SELECT * FROM conversations WHERE id = ?',
        [conversationId],
      );
      if (!conv || conv.status === 'CLOSED') return;
      if (deps.presence.isVisitorOnline(visitorId)) return; // they came back
      const replied = await deps.db.get<{ id: string }>(
        "SELECT id FROM messages WHERE conversation_id = ? AND sender_type = 'VISITOR' LIMIT 1",
        [conversationId],
      );
      if (replied) return; // client spoke — the 12h rule owns it now
      await closeConversation(deps, conversationId);
    })().catch((err: unknown) => console.error('[realtime] outreach close', err));
  }, OUTREACH_CLOSE_MS);
  t.unref?.();
  outreachTimers.set(conversationId, t);
}

// The automated "our support team is busy" notice (posted a few minutes after
// an unanswered client message) has been retired — visitors are never sent that
// message anymore, so there is no busy-notice timer.

// TL nudge throttle: one alert per conversation per 10 minutes.
const leadAlertAt = new Map<string, number>();

function notifyLeadOfOfflineAssignee(
  deps: AppDeps,
  conversationId: string,
  assigneeId: string,
): void {
  const last = leadAlertAt.get(conversationId) ?? 0;
  if (Date.now() - last < 10 * 60 * 1000) return;
  leadAlertAt.set(conversationId, Date.now());
  void (async () => {
    const assignee = await deps.db.get<{ name: string; team_lead_id: string | null }>(
      'SELECT name, team_lead_id FROM users WHERE id = ?',
      [assigneeId],
    );
    if (!assignee?.team_lead_id) return;
    if (!deps.presence.isAgentOnline(assignee.team_lead_id)) return;
    deps.io
      .of(AGENT_NAMESPACE)
      .to(userRoom(assignee.team_lead_id))
      .emit(EV.AgentAlert, {
        conversationId,
        message: `${assignee.name} is off-duty and their client just messaged — please take over.`,
      });
  })().catch((err: unknown) => console.error('[realtime] lead alert', err));
}

function startInactivitySweeper(deps: AppDeps): void {
  const run = () => {
    void sweepInactiveConversations(deps).catch((err) =>
      console.error('[realtime] inactivity sweep', err),
    );
    // Rebroadcast live visitor lists so a session that just crossed the
    // max-online cap (left-open tab / bot) drops off "Online now" even when the
    // site is otherwise quiet and nothing else would trigger a refresh.
    void (async () => {
      const sites = await deps.db.all<{ id: string }>('SELECT id FROM websites');
      for (const s of sites) await broadcastVisitors(deps, s.id);
    })().catch((err) => console.error('[realtime] visitor refresh sweep', err));
  };
  const t = setInterval(run, INACTIVITY_SWEEP_MS);
  t.unref?.();
  run(); // sweep once at startup too
}

// ─── Visitor list broadcast ──────────────────────────────────

async function buildVisitorList(deps: AppDeps, websiteId: string): Promise<Visitor[]> {
  const online = deps.presence.onlineVisitors(websiteId);
  if (online.length === 0) return [];
  const ids = online.map((o) => o.visitorId);
  const placeholders = ids.map(() => '?').join(',');
  const rows = await deps.db.all<VisitorRow>(
    `SELECT * FROM visitors WHERE id IN (${placeholders})`,
    ids,
  );
  const byId = new Map(rows.map((r) => [r.id, r]));
  // Pages viewed + chats per visitor (grouped, one query each).
  const pageCounts = await deps.db.all<{ visitor_id: string; n: number }>(
    `SELECT vp.visitor_id, COUNT(*) AS n FROM visitor_pages vp
       JOIN visitors v ON v.id = vp.visitor_id
      WHERE vp.visitor_id IN (${placeholders})
        AND vp.created_at >= COALESCE(v.session_started_at, v.created_at)
      GROUP BY vp.visitor_id`,
    ids,
  );
  const chatCounts = await deps.db.all<{ visitor_id: string; n: number }>(
    `SELECT visitor_id, COUNT(*) AS n FROM conversations
      WHERE visitor_id IN (${placeholders}) GROUP BY visitor_id`,
    ids,
  );
  // Current open conversation per visitor (+ its agent name) — for the live
  // "busy with another agent" hint, independent of history scoping.
  const openRows = await deps.db.all<{
    visitor_id: string;
    id: string;
    status: string;
    assigned_user_id: string | null;
    agent_name: string | null;
  }>(
    `SELECT c.visitor_id, c.id, c.status, c.assigned_user_id, u.name AS agent_name
       FROM conversations c
       LEFT JOIN users u ON u.id = c.assigned_user_id
      WHERE c.visitor_id IN (${placeholders})
        AND c.status IN ('WAITING', 'OFFERED', 'ACTIVE')`,
    ids,
  );
  const pagesBy = new Map(pageCounts.map((r) => [r.visitor_id, Number(r.n)]));
  const chatsBy = new Map(chatCounts.map((r) => [r.visitor_id, Number(r.n)]));
  const openBy = new Map(openRows.map((r) => [r.visitor_id, r]));
  return online.map((o) => {
    const row = byId.get(o.visitorId);
    const base: Visitor = row
      ? toVisitor(row, true, o.page)
      : {
          id: o.visitorId,
          websiteId,
          name: null,
          email: null,
          lastSeenAt: nowIso(),
          online: true,
          currentPage: o.page,
        };
    base.sessionPages = pagesBy.get(o.visitorId) ?? 0;
    base.chats = chatsBy.get(o.visitorId) ?? 0;
    const open = openBy.get(o.visitorId);
    base.activeConversation = open
      ? {
          id: open.id,
          status: open.status as NonNullable<Visitor['activeConversation']>['status'],
          assignedUserId: open.assigned_user_id,
          agentName: open.agent_name,
        }
      : null;
    return base;
  });
}

async function broadcastVisitors(deps: AppDeps, websiteId: string): Promise<void> {
  const visitors = await buildVisitorList(deps, websiteId);
  deps.io.of(AGENT_NAMESPACE).to(websiteRoom(websiteId)).emit(EV.VisitorsUpdate, {
    websiteId,
    visitors,
  });
}

// ─── Assignment history (for agent-open ack) ─────────────────

interface HistoryRow {
  id: string;
  conversation_id: string;
  from_user_id: string | null;
  to_user_id: string;
  reason: 'AUTO' | 'OFFER' | 'TRANSFER';
  created_at: string;
}

async function loadHistory(deps: AppDeps, conversationId: string) {
  const rows = await deps.db.all<HistoryRow>(
    'SELECT * FROM assignment_history WHERE conversation_id = ? ORDER BY created_at ASC',
    [conversationId],
  );
  const userIds = [
    ...new Set(
      rows.flatMap((r) => [r.from_user_id, r.to_user_id]).filter((v): v is string => !!v),
    ),
  ];
  const users = userIds.length
    ? await deps.db.all<UserRow>(
        `SELECT * FROM users WHERE id IN (${userIds.map(() => '?').join(',')})`,
        userIds,
      )
    : [];
  const byId = new Map(users.map((u) => [u.id, toUserPublic(u)]));
  return rows.map((r) => ({
    id: r.id,
    conversationId: r.conversation_id,
    fromUserId: r.from_user_id,
    toUserId: r.to_user_id,
    reason: r.reason,
    createdAt: r.created_at,
    fromUser: r.from_user_id ? (byId.get(r.from_user_id) ?? null) : null,
    toUser: byId.get(r.to_user_id) ?? null,
  }));
}

// ─── Access helpers ──────────────────────────────────────────

/** Is `userId` the Team Lead of the user the conversation is assigned to? */
async function isLeadOfAssignee(deps: AppDeps, conv: ConversationRow, userId: string): Promise<boolean> {
  if (!conv.assigned_user_id) return false;
  const csr = await deps.db.get<{ id: string }>(
    'SELECT id FROM users WHERE id = ? AND team_lead_id = ?',
    [conv.assigned_user_id, userId],
  );
  return !!csr;
}

/**
 * Assignee, ADMIN or MANAGER may manage (accept/close/transfer/send); a Team
 * Lead may manage their own CSRs' chats and the unassigned queue of accessible
 * sites.
 */
async function canManageConversation(
  deps: AppDeps,
  conv: ConversationRow,
  userId: string,
  role: Role,
): Promise<boolean> {
  if (role === 'ADMIN' || role === 'MANAGER') return true;
  if (conv.assigned_user_id === userId) return true;
  if (role === 'LEAD') {
    if (conv.assigned_user_id) return isLeadOfAssignee(deps, conv, userId);
    return userCanAccessWebsite(deps, userId, role, conv.website_id);
  }
  return false;
}

/** Assignee, ADMIN/MANAGER, or the assignee's own Team Lead may send agent messages. */
async function canSendAsAgent(
  deps: AppDeps,
  conv: ConversationRow,
  userId: string,
  role: Role,
): Promise<boolean> {
  if (role === 'ADMIN' || role === 'MANAGER' || conv.assigned_user_id === userId) return true;
  if (role === 'LEAD') return isLeadOfAssignee(deps, conv, userId);
  return false;
}

async function getConversation(
  deps: AppDeps,
  conversationId: string | null,
): Promise<ConversationRow | undefined> {
  if (!conversationId) return undefined;
  return deps.db.get<ConversationRow>('SELECT * FROM conversations WHERE id = ?', [conversationId]);
}

/**
 * Post "{name} has joined the chat" the first time an agent participates in a
 * conversation they weren't already part of — e.g. an admin / manager / team
 * lead jumping into a chat someone else started. Skips when this agent has
 * already spoken in the chat (they're a participant — including the person who
 * STARTED it) and when the exact notice already exists (so the accept/claim
 * flows, which announce explicitly, never double-post).
 */
async function announceAgentJoin(
  deps: AppDeps,
  conversationId: string,
  userId: string,
  name: string,
): Promise<void> {
  const spoke = await deps.db.get<{ id: string }>(
    "SELECT id FROM messages WHERE conversation_id = ? AND sender_type = 'AGENT' AND sender_user_id = ? LIMIT 1",
    [conversationId, userId],
  );
  if (spoke) return; // already in the conversation (e.g. they started it)
  const body = `${name} has joined the chat`;
  const already = await deps.db.get<{ id: string }>(
    "SELECT id FROM messages WHERE conversation_id = ? AND kind = 'SYSTEM' AND body = ? LIMIT 1",
    [conversationId, body],
  );
  if (already) return;
  await postMessage(deps, {
    conversationId,
    senderType: 'SYSTEM',
    kind: 'SYSTEM',
    body,
    // Agent-only: never emit this notice to the widget. Visitors don't need
    // to see "So-and-so has joined the chat" — it's internal bookkeeping,
    // and showing it as the FIRST message a visitor sees (before the agent's
    // real greeting) looks robotic.
    agentOnly: true,
    // Backdate a moment so the notice always sorts ABOVE the message that
    // triggered it, even when both land in the same millisecond.
    createdAt: new Date(Date.now() - 5).toISOString(),
  });
}

// ─── attachRealtime ──────────────────────────────────────────

export function attachRealtime(deps: AppDeps): void {
  attachWidgetNamespace(deps, deps.io.of(WIDGET_NAMESPACE));
  attachAgentNamespace(deps, deps.io.of(AGENT_NAMESPACE));
  startInactivitySweeper(deps); // auto-close chats after 12h of total silence

  // Assignment/status changes → refresh the live visitor list for that site
  // (coalesced per website so a burst produces one broadcast).
  const pendingVisitorRefresh = new Map<string, NodeJS.Timeout>();
  setVisitorRefresh((websiteId) => {
    if (pendingVisitorRefresh.has(websiteId)) return;
    const t = setTimeout(() => {
      pendingVisitorRefresh.delete(websiteId);
      void broadcastVisitors(deps, websiteId).catch((err) =>
        console.error('[realtime] visitor refresh', err),
      );
    }, 150);
    t.unref?.();
    pendingVisitorRefresh.set(websiteId, t);
  });

  // Fires once the visitor-offline grace period expires (not on every page
  // navigation) — only then do agents see them drop out of "Online now".
  deps.presence.setVisitorOfflineListener((websiteId, visitorId) => {
    void (async () => {
      await deps.db.run('UPDATE visitors SET last_seen_at = ? WHERE id = ?', [
        nowIso(),
        visitorId,
      ]);
      await broadcastVisitors(deps, websiteId);

      // Agent-only outreach (CSR messaged, visitor NEVER replied): when the
      // visitor leaves the site they get 15 minutes to come back; if they're
      // still gone (and still haven't replied) the chat closes on its own.
      // The instant the client has sent even one message this never fires —
      // the 12-hour inactivity rule owns the conversation instead.
      const openConvs = await deps.db.all<{ id: string }>(
        `SELECT c.id FROM conversations c
          WHERE c.visitor_id = ? AND c.status IN ('WAITING', 'OFFERED', 'ACTIVE')
            AND EXISTS (SELECT 1 FROM messages m
                         WHERE m.conversation_id = c.id AND m.sender_type = 'AGENT')
            AND NOT EXISTS (SELECT 1 FROM messages m
                             WHERE m.conversation_id = c.id AND m.sender_type = 'VISITOR')`,
        [visitorId],
      );
      for (const c of openConvs) scheduleOutreachClose(deps, c.id, visitorId);

      // "Visitor left the site" notice in every open chat that has messages —
      // agents see in the chat itself that nobody is reading right now.
      const noticeConvs = await deps.db.all<{ id: string }>(
        `SELECT c.id FROM conversations c
          WHERE c.visitor_id = ? AND c.status IN ('WAITING', 'OFFERED', 'ACTIVE')
            AND EXISTS (SELECT 1 FROM messages m WHERE m.conversation_id = c.id)`,
        [visitorId],
      );
      for (const c of noticeConvs) {
        const last = await deps.db.get<{ body: string | null; kind: string }>(
          'SELECT body, kind FROM messages WHERE conversation_id = ? ORDER BY created_at DESC, id DESC LIMIT 1',
          [c.id],
        );
        if (last?.kind === 'SYSTEM' && last.body === VISITOR_LEFT_NOTICE) continue;
        await postMessage(deps, {
          conversationId: c.id,
          senderType: 'SYSTEM',
          kind: 'SYSTEM',
          body: VISITOR_LEFT_NOTICE,
          agentOnly: true,
        });
      }
    })().catch((err: unknown) => console.error('[realtime] visitor offline', err));
  });
}

// ═════════════════════════════ /widget ═══════════════════════

function attachWidgetNamespace(deps: AppDeps, ns: Namespace): void {
  ns.use((socket, next) => {
    void (async () => {
      const auth = (socket.handshake.auth ?? {}) as Record<string, unknown>;
      const widgetKey = asString(auth.widgetKey);
      if (!widgetKey) return next(new Error('Missing widget key'));

      const website = await deps.db.get<WebsiteRow>(
        'SELECT * FROM websites WHERE widget_key = ?',
        [widgetKey],
      );
      if (!website) return next(new Error('Invalid widget key'));

      // Domain check: Origin hostname must appear in the website's domain list
      // (comma-separated; empty = allow all, for dev).
      const domains = (website.domains ?? '')
        .split(',')
        .map((d) => d.trim().toLowerCase())
        .filter(Boolean);
      if (domains.length > 0) {
        const host = hostnameOf(socket.handshake.headers.origin);
        if (!host || !domains.includes(host)) return next(new Error('Domain not allowed'));
      }

      // Admin blocklist — deny before we create/track a visitor. IP is known
      // synchronously; block here so a blocked address never opens a chat.
      const cip = clientIp(socket);
      if (deps.blocklist.isIpBlocked(cip)) return next(new Error('Access blocked'));

      // Resolve or create the visitor.
      let visitorRow: VisitorRow | undefined;
      const rawToken = asString(auth.visitorToken);
      if (rawToken) {
        const payload = verifyToken<VisitorTokenPayload>(deps.config, rawToken);
        if (payload && payload.typ === 'visitor' && payload.websiteId === website.id) {
          visitorRow = await deps.db.get<VisitorRow>('SELECT * FROM visitors WHERE id = ?', [
            payload.sub,
          ]);
        }
      }
      let visitorToken = rawToken ?? '';
      if (!visitorRow) {
        const id = newId();
        const now = nowIso();
        await deps.db.run(
          'INSERT INTO visitors (id, website_id, name, email, created_at, last_seen_at) VALUES (?, ?, NULL, NULL, ?, ?)',
          [id, website.id, now, now],
        );
        visitorRow = await deps.db.get<VisitorRow>('SELECT * FROM visitors WHERE id = ?', [id]);
        if (!visitorRow) return next(new Error('Failed to create visitor'));
        visitorToken = signVisitorToken(deps.config, id, website.id);
      }

      const data = socket.data as WidgetSocketData;
      data.visitorId = visitorRow.id;
      data.websiteId = website.id;
      data.conversationId = null;
      // Known synchronously: cached geo, else the instant offline lookup.
      data.geoCc = visitorRow.geo_cc ?? localCountry(clientIp(socket));
      widgetCtx.set(socket, {
        visitorToken,
        page: asString(auth.page),
        websiteRow: website,
      });

      // ── Visit/session tracking (Zendesk-style) ──
      const now = nowIso();
      const gapMs = Date.now() - Date.parse(visitorRow.last_seen_at || visitorRow.created_at);
      // "Visits" counts analytics-style (30-min idle = new visit)…
      const newVisit = !visitorRow.session_started_at || gapMs > 30 * 60 * 1000;
      // …but the live "On site" timer restarts whenever the visitor actually
      // went offline and came back (page navigation keeps them online via the
      // presence grace, so the timer keeps running across pages). The 30-min
      // idle gap is a belt: even if presence somehow says "online", a visitor
      // idle that long starts a fresh session — no absurd hours-long timers.
      const newSession =
        !visitorRow.session_started_at ||
        !deps.presence.isVisitorOnline(visitorRow.id) ||
        gapMs > 30 * 60 * 1000;
      await deps.db.run(
        `UPDATE visitors SET
           last_seen_at = ?,
           user_agent = ?,
           referrer = CASE WHEN ? != '' THEN ? ELSE referrer END,
           total_visits = total_visits + ?,
           session_started_at = CASE WHEN ? = 1 THEN ? ELSE COALESCE(session_started_at, ?) END
         WHERE id = ?`,
        [
          now,
          (asString(socket.handshake.headers['user-agent'] as string) ?? '').slice(0, 500) || null,
          asString(auth.referrer)?.slice(0, 500) ?? '',
          asString(auth.referrer)?.slice(0, 500) ?? '',
          newVisit ? 1 : 0,
          newSession ? 1 : 0,
          now,
          now,
          visitorRow.id,
        ],
      );
      // Page journey entry for this page load.
      const pageUrl = asString(auth.page)?.slice(0, 1000) ?? null;
      if (pageUrl) {
        await deps.db.run(
          'INSERT INTO visitor_pages (id, visitor_id, url, title, created_at) VALUES (?, ?, ?, ?, ?)',
          [newId(), visitorRow.id, pageUrl, asString(auth.pageTitle)?.slice(0, 500) ?? null, now],
        );
      }
      // Country blocklist — resolved synchronously and instantly: the cached
      // country if we have it, else the OFFLINE geoip DB (no network, ~1ms). So
      // a blocked-country visitor is rejected right here, before they connect —
      // no brief flash in the online list and no slow handshake. The rare IP the
      // offline DB doesn't know is caught by the async recheck / geo-resolve.
      if (deps.blocklist.hasCountryBlocks()) {
        const cc = visitorRow.geo_cc ?? localCountry(cip);
        if (cc && deps.blocklist.isCountryBlocked(cc)) return next(new Error('Access blocked'));
      }

      // Capture IP + city/country (best-effort, non-blocking).
      updateVisitorGeo(deps, visitorRow.id, clientIp(socket));
      next();
    })().catch((err: unknown) => {
      next(err instanceof Error ? err : new Error('Widget handshake failed'));
    });
  });

  ns.on('connection', (socket) => {
    const data = socket.data as WidgetSocketData;
    const ctx = widgetCtx.get(socket);
    if (!ctx) {
      socket.disconnect(true);
      return;
    }

    void onWidgetConnected(deps, socket, data, ctx).catch((err: unknown) => {
      console.error('[realtime] widget connect', err);
      socket.emit(EV.AppError, { message: 'Failed to initialise chat' });
    });

    // Country blocklist for a brand-new visitor whose country we couldn't check
    // synchronously at handshake time (no cached geo). Resolved off the critical
    // path so the socket connects instantly; if they turn out to be in a blocked
    // country we boot them right back off (the widget then hides itself).
    if (deps.blocklist.hasCountryBlocks() && !data.geoCc) {
      void (async () => {
        const cip = clientIp(socket);
        if (!cip) return;
        const cc = await lookupCountry(cip);
        if (deps.blocklist.isCountryBlocked(cc)) {
          socket.emit(EV.AppError, { message: 'Access blocked' });
          socket.disconnect(true);
        }
      })().catch(() => {});
    }

    // ── Visitor sends a message ──
    const allowVisitorMsg = makeLimiter(MSG_RATE_MAX, MSG_RATE_WINDOW_MS);
    socket.on(
      EV.WidgetMessage,
      safe(socket, async (payload: unknown) => {
        const p = (payload ?? {}) as Record<string, unknown>;
        const body = asString(p.body)?.trim().slice(0, MAX_MESSAGE_CHARS);
        const tempId = asString(p.tempId) ?? undefined;
        if (!body) return;
        if (!allowVisitorMsg()) {
          socket.emit(EV.AppError, { message: 'You are sending messages too quickly — please slow down.' });
          return;
        }

        let conv = await getConversation(deps, data.conversationId);
        if (conv && (conv.status === 'CLOSED')) conv = undefined;
        if (!conv) {
          // Another tab may already have an open conversation for this visitor.
          conv = await deps.db.get<ConversationRow>(
            "SELECT * FROM conversations WHERE visitor_id = ? AND status IN ('WAITING','OFFERED','ACTIVE') ORDER BY created_at DESC LIMIT 1",
            [data.visitorId],
          );
        }

        let isNew = false;
        if (!conv) {
          isNew = true;
          const id = newId();
          // Was a human (CSR/Team Lead) online the moment this chat arrived? This
          // splits the two queues: 1 → "Incoming" (live pickup, AI greets only),
          // 0 → "Offline Chats" (nobody online, AI handles it fully). Frozen at
          // arrival so a chat never hops queues when presence later changes.
          const humanOnline = await isAnyHumanAvailable(deps, data.websiteId);
          await deps.db.run(
            "INSERT INTO conversations (id, website_id, visitor_id, status, assigned_user_id, created_at, activated_at, closed_at, human_online_on_arrival) VALUES (?, ?, ?, 'WAITING', NULL, ?, NULL, NULL, ?)",
            [id, data.websiteId, data.visitorId, nowIso(), humanOnline ? 1 : 0],
          );
          conv = await deps.db.get<ConversationRow>('SELECT * FROM conversations WHERE id = ?', [
            id,
          ]);
          if (!conv) throw new Error('Failed to create conversation');
        }

        data.conversationId = conv.id;
        await socket.join(convRoom(conv.id));

        const wasOffered = conv.status === 'OFFERED';
        await postMessage(deps, {
          conversationId: conv.id,
          senderType: 'VISITOR',
          body,
          tempId,
        });

        // Assigned CSR is off-duty and their client is talking → nudge the
        // Team Lead (throttled per conversation) so somebody answers.
        if (
          !isNew &&
          conv.assigned_user_id &&
          !deps.presence.isAgentOnline(conv.assigned_user_id)
        ) {
          notifyLeadOfOfflineAssignee(deps, conv.id, conv.assigned_user_id);
        }

        // Auto-capture contact details the visitor typed (email / phone / name).
        if (await captureVisitorInfo(deps, data.visitorId, body)) {
          await emitInboxUpdate(deps, conv.id);
          void broadcastVisitors(deps, data.websiteId);
        }

        if (wasOffered) {
          // Customer replied to a CSR-initiated offer → ACTIVE, no accept step.
          await activateConversation(deps, conv.id);
          return;
        }

        if (isNew) {
          // No auto-assignment: a new chat stays WAITING and UNASSIGNED, waiting
          // in the queue for an agent to pick it up manually (type-to-join). It
          // just gets the queue notice so the visitor knows someone will come.
          await postMessage(deps, {
            conversationId: conv.id,
            senderType: 'SYSTEM',
            kind: 'SYSTEM',
            body: QUEUE_MESSAGE,
          });
          await emitInboxUpdate(deps, conv.id);
          await emitConversationStatus(deps, conv.id);
        }

        // AI greeter keeps queued visitors company until an agent joins
        // (it re-checks WAITING + unassigned internally, so always safe).
        maybeBotReply(deps, conv.id);
      }),
    );

    // ── Tab idle/active (presence only — the socket stays connected) ──
    socket.on(
      EV.WidgetActivity,
      safe(socket, async (payload: unknown) => {
        const p = (payload ?? {}) as Record<string, unknown>;
        const active = p.active === true;
        const { changed, idleMs } = deps.presence.setVisitorSocketIdle(
          data.websiteId,
          data.visitorId,
          socket.id,
          !active,
        );
        if (!changed) return;
        // Coming back from a long idle (30+ min) starts a fresh on-site session
        // so the timer doesn't read "20 hours" after an overnight background tab.
        if (active && idleMs > 30 * 60 * 1000) {
          await deps.db.run('UPDATE visitors SET session_started_at = ? WHERE id = ?', [
            nowIso(),
            data.visitorId,
          ]);
        }
        await broadcastVisitors(deps, data.websiteId);
      }),
    );

    // ── Visitor ends the chat ──
    socket.on(
      EV.WidgetEndChat,
      safe(socket, async () => {
        const conv = await getConversation(deps, data.conversationId);
        if (!conv || conv.status === 'CLOSED') return;
        if (conv.visitor_id !== data.visitorId) return;
        await closeConversation(deps, conv.id);
      }),
    );

    // ── Visitor asks for the transcript by email (widget ⋯ menu) ──
    let lastTranscriptAsk = 0;
    socket.on(
      EV.WidgetEmailTranscript,
      safe(socket, async (_payload: unknown, ack?: unknown) => {
        const reply = typeof ack === 'function' ? (ack as (r: unknown) => void) : () => {};
        if (!deps.config.smtpHost) {
          reply({ error: 'Email is not set up yet — please ask the agent for a copy.' });
          return;
        }
        const now = Date.now();
        if (now - lastTranscriptAsk < 60_000) {
          reply({ error: 'Transcript already sent — check your inbox (and spam).' });
          return;
        }
        const conv = await getConversation(deps, data.conversationId);
        if (!conv || conv.visitor_id !== data.visitorId) {
          reply({ error: 'Start a chat first, then request the transcript.' });
          return;
        }
        const vis = await deps.db.get<{ email: string | null }>(
          'SELECT email FROM visitors WHERE id = ?',
          [data.visitorId],
        );
        if (!vis?.email) {
          reply({ error: 'no-email' }); // widget opens the contact form
          return;
        }
        lastTranscriptAsk = now;
        void sendTranscriptEmail(deps, conv.id);
        reply({ ok: true, email: vis.email });
      }),
    );

    // ── CSAT rating (after close) ──
    socket.on(
      EV.WidgetRate,
      safe(socket, async (payload: unknown) => {
        const p = (payload ?? {}) as Record<string, unknown>;
        const rating = Math.round(Number(p.rating));
        const comment = asString(p.comment)?.trim().slice(0, 500) || null;
        if (!Number.isFinite(rating) || rating < 1 || rating > 5) return;

        // Rate the visitor's current chat (agent may request feedback
        // mid-conversation) or the most recently closed one — once.
        const conv = await deps.db.get<ConversationRow>(
          `SELECT * FROM conversations WHERE visitor_id = ? AND status IN ('ACTIVE', 'CLOSED')
            ORDER BY CASE WHEN status = 'ACTIVE' THEN 0 ELSE 1 END, created_at DESC LIMIT 1`,
          [data.visitorId],
        );
        if (!conv || conv.rating != null) return; // only rate once

        await deps.db.run('UPDATE conversations SET rating = ?, rating_comment = ? WHERE id = ?', [
          rating,
          comment,
          conv.id,
        ]);
        await postMessage(deps, {
          conversationId: conv.id,
          senderType: 'SYSTEM',
          kind: 'SYSTEM',
          body: `Customer rated this chat ${'★'.repeat(rating)}${'☆'.repeat(5 - rating)}${comment ? ` — “${comment}”` : ''}`,
        });
        await emitInboxUpdate(deps, conv.id);
      }),
    );

    // ── Typing indicator → agent side ──
    socket.on(
      EV.WidgetTyping,
      safe(socket, (payload: unknown) => {
        const p = (payload ?? {}) as Record<string, unknown>;
        if (!data.conversationId) return;
        deps.io
          .of(AGENT_NAMESPACE)
          .to(convRoom(data.conversationId))
          .emit(EV.ChatTyping, {
            conversationId: data.conversationId,
            from: 'VISITOR',
            typing: p.typing === true,
          });
      }),
    );

    // ── Read receipts (visitor read agent messages) ──
    socket.on(
      EV.WidgetRead,
      safe(socket, async (payload: unknown) => {
        const p = (payload ?? {}) as Record<string, unknown>;
        const messageIds = asStringArray(p.messageIds);
        if (!data.conversationId || messageIds.length === 0) return;
        const readAt = nowIso();
        await deps.db.run(
          `UPDATE messages SET read_at = ? WHERE conversation_id = ? AND sender_type != 'VISITOR' AND read_at IS NULL AND id IN (${messageIds.map(() => '?').join(',')})`,
          [readAt, data.conversationId, ...messageIds],
        );
        const receipt = { conversationId: data.conversationId, messageIds, readAt };
        deps.io.of(AGENT_NAMESPACE).to(convRoom(data.conversationId)).emit(EV.ChatReceipt, receipt);
        deps.io.of(WIDGET_NAMESPACE).to(convRoom(data.conversationId)).emit(EV.ChatReceipt, receipt);
      }),
    );

    // ── Visitor identifies themselves ──
    socket.on(
      EV.WidgetInfo,
      safe(socket, async (payload: unknown) => {
        const p = (payload ?? {}) as Record<string, unknown>;
        const name = asString(p.name)?.trim();
        const email = asString(p.email)?.trim();
        const phone = asString(p.phone)?.trim().slice(0, 32);
        if (!name && !email && !phone) return;
        if (name) {
          await deps.db.run('UPDATE visitors SET name = ? WHERE id = ?', [name, data.visitorId]);
        }
        if (email) {
          await deps.db.run('UPDATE visitors SET email = ? WHERE id = ?', [email, data.visitorId]);
        }
        if (phone) {
          await deps.db.run('UPDATE visitors SET phone = ? WHERE id = ?', [phone, data.visitorId]);
        }
        await broadcastVisitors(deps, data.websiteId);
        if (data.conversationId) await emitInboxUpdate(deps, data.conversationId);
      }),
    );

    // ── Calls (contract implemented by the features agent) ──
    registerWidgetCallHandlers(deps, socket);

    // ── Disconnect ──
    // A page reload/navigation closes this socket for a second — the presence
    // store keeps the visitor online through a grace window, and the real
    // offline handling lives in the setVisitorOfflineListener callback.
    // last_seen_at is stamped HERE too, so "Last seen" stays truthful even if
    // a server restart wipes the pending grace timer.
    socket.on('disconnect', () => {
      void deps.db
        .run('UPDATE visitors SET last_seen_at = ? WHERE id = ?', [nowIso(), data.visitorId])
        .catch(() => undefined);
      deps.presence.removeVisitor(data.websiteId, data.visitorId, socket.id);
    });
  });
}

async function proactiveGreet(
  deps: AppDeps,
  socket: Socket,
  data: WidgetSocketData,
  website: WebsiteRow,
): Promise<void> {
  if (data.conversationId || !socket.connected) return;
  const open = await deps.db.get<{ id: string }>(
    "SELECT id FROM conversations WHERE visitor_id = ? AND status IN ('WAITING','OFFERED','ACTIVE') LIMIT 1",
    [data.visitorId],
  );
  if (open) return;
  // At most one proactive opener per visitor per day.
  const recent = await deps.db.get<{ id: string }>(
    'SELECT id FROM conversations WHERE visitor_id = ? AND created_at > ? LIMIT 1',
    [data.visitorId, new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString()],
  );
  if (recent) return;
  const humanOnline = await isAnyHumanAvailable(deps, data.websiteId);
  const id = newId();
  await deps.db.run(
    "INSERT INTO conversations (id, website_id, visitor_id, status, assigned_user_id, created_at, activated_at, closed_at, human_online_on_arrival) VALUES (?, ?, ?, 'WAITING', NULL, ?, NULL, NULL, ?)",
    [id, data.websiteId, data.visitorId, nowIso(), humanOnline ? 1 : 0],
  );
  data.conversationId = id;
  await socket.join(convRoom(id));
  await postGreeting(deps, id, website.name);
  await emitInboxUpdate(deps, id);
  await emitConversationStatus(deps, id);
  void broadcastVisitors(deps, data.websiteId);
}

async function onWidgetConnected(
  deps: AppDeps,
  socket: Socket,
  data: WidgetSocketData,
  ctx: { visitorToken: string; page: string | null; websiteRow: WebsiteRow },
): Promise<void> {
  deps.presence.addVisitor(data.websiteId, data.visitorId, socket.id, ctx.page);
  await deps.db.run('UPDATE visitors SET last_seen_at = ? WHERE id = ?', [
    nowIso(),
    data.visitorId,
  ]);

  // Heartbeat: long visits keep last_seen_at fresh (~60s accuracy) even if
  // the process restarts mid-visit and never sees this socket disconnect.
  const heartbeat = setInterval(() => {
    void deps.db
      .run('UPDATE visitors SET last_seen_at = ? WHERE id = ?', [nowIso(), data.visitorId])
      .catch(() => undefined);
  }, 60_000);
  heartbeat.unref?.();
  socket.on('disconnect', () => clearInterval(heartbeat));

  // Resume the open conversation, if any.
  const conv = await deps.db.get<ConversationRow>(
    "SELECT * FROM conversations WHERE visitor_id = ? AND status IN ('WAITING','OFFERED','ACTIVE') ORDER BY created_at DESC LIMIT 1",
    [data.visitorId],
  );

  let conversation = null;
  let messages: unknown[] = [];
  let agent: { name: string; avatarColor: string; avatarUrl?: string | null } | null = null;

  if (conv) {
    data.conversationId = conv.id;
    await socket.join(convRoom(conv.id));
    // If the chat's last entry is the "left" notice, the visitor just returned.
    const lastMsg = await deps.db.get<{ body: string | null; kind: string }>(
      'SELECT body, kind FROM messages WHERE conversation_id = ? ORDER BY created_at DESC, id DESC LIMIT 1',
      [conv.id],
    );
    if (lastMsg?.kind === 'SYSTEM' && lastMsg.body === VISITOR_LEFT_NOTICE) {
      await postMessage(deps, {
        conversationId: conv.id,
        senderType: 'SYSTEM',
        kind: 'SYSTEM',
        body: VISITOR_BACK_NOTICE,
        agentOnly: true,
      });
    }
    conversation = (await loadSummary(deps, conv.id)) ?? null;
    // Bounded: only the latest 150 messages ride the widget handshake.
    // agent_only notes ("Visitor left the site" …) are never shown to the visitor.
    const rows = await deps.db.all<MessageRow>(
      'SELECT * FROM (SELECT * FROM messages WHERE conversation_id = ? AND agent_only = 0 ORDER BY created_at DESC, id DESC LIMIT 150) t ORDER BY created_at ASC, id ASC',
      [conv.id],
    );
    messages = await hydrateMessages(deps, rows);
    if (conv.assigned_user_id) {
      const assigned = await deps.db.get<UserRow>('SELECT * FROM users WHERE id = ?', [
        conv.assigned_user_id,
      ]);
      if (assigned) {
        agent = {
          name: assigned.name,
          avatarColor: assigned.avatar_color,
          avatarUrl: assigned.avatar_url ?? null,
        };
      }
    }
  }

  const visitorRow = await deps.db.get<VisitorRow>('SELECT * FROM visitors WHERE id = ?', [
    data.visitorId,
  ]);

  socket.emit(EV.WidgetReady, {
    visitorToken: ctx.visitorToken,
    visitor: visitorRow ? toVisitor(visitorRow, true, ctx.page) : null,
    website: toBranding(ctx.websiteRow),
    conversation,
    messages,
    agent,
  });

  await broadcastVisitors(deps, data.websiteId);

  // Alex proactive greeting: a few seconds after a visitor with no open chat
  // arrives, the AI opens the conversation itself (like the human agents did).
  if (!conv && deps.config.alex.baseUrl && deps.config.alex.proactive) {
    const timer = setTimeout(() => {
      void proactiveGreet(deps, socket, data, ctx.websiteRow).catch((err: unknown) =>
        console.warn('[alex] proactive greeting failed:', (err as Error).message),
      );
    }, deps.config.alex.proactiveDelayMs);
    timer.unref?.();
    socket.on('disconnect', () => clearTimeout(timer));
  }
}

// ═════════════════════════════ /agent ════════════════════════

function attachAgentNamespace(deps: AppDeps, ns: Namespace): void {
  ns.use((socket, next) => {
    void (async () => {
      const auth = (socket.handshake.auth ?? {}) as Record<string, unknown>;
      const token = asString(auth.token);
      if (!token) return next(new Error('Missing token'));
      const payload = verifyToken<AgentTokenPayload>(deps.config, token);
      if (!payload || payload.typ !== 'agent') return next(new Error('Unauthorized'));
      const user = await deps.db.get<UserRow>('SELECT * FROM users WHERE id = ?', [payload.sub]);
      if (!user) return next(new Error('Unauthorized'));
      // Per-user IP allow-list also gates the realtime socket.
      if (!ipAllowed(user.allowed_ips, clientIp(socket) ?? '')) {
        return next(new Error('Access is not allowed from your network.'));
      }

      const data = socket.data as AgentSocketData;
      data.userId = user.id;
      data.role = user.role;
      data.name = user.name;
      agentCtx.set(socket, { user });
      next();
    })().catch((err: unknown) => {
      next(err instanceof Error ? err : new Error('Agent handshake failed'));
    });
  });

  ns.on('connection', (socket) => {
    const data = socket.data as AgentSocketData;

    void onAgentConnected(deps, socket, data).catch((err: unknown) => {
      console.error('[realtime] agent connect', err);
      socket.emit(EV.AppError, { message: 'Failed to initialise session' });
    });

    // ── Open a conversation (ack: {conversation, messages, history}) ──
    socket.on(
      EV.AgentOpen,
      safe(socket, async (payload: unknown, ack?: unknown) => {
        const reply = typeof ack === 'function' ? (ack as (res: unknown) => void) : () => {};
        const p = (payload ?? {}) as Record<string, unknown>;
        const conv = await getConversation(deps, asString(p.conversationId));
        if (!conv) {
          reply({ error: 'Conversation not found' });
          return;
        }
        // MANAGER may open anything to watch; everyone else needs manage access.
        // Exception: an UNASSIGNED queued chat — chats are no longer
        // auto-assigned, so any agent on the website (CSR included) must be able
        // to open it to pick it up (Accept / type-to-join), same rule as those.
        const allowed =
          data.role === 'MANAGER' ||
          (await canManageConversation(deps, conv, data.userId, data.role)) ||
          (conv.status === 'WAITING' &&
            !conv.assigned_user_id &&
            (await userCanAccessWebsite(deps, data.userId, data.role, conv.website_id)));
        if (!allowed) {
          reply({ error: 'Forbidden' });
          socket.emit(EV.AppError, { message: 'You do not have access to this conversation' });
          return;
        }
        await socket.join(convRoom(conv.id));
        const [conversation, rows, history] = await Promise.all([
          loadSummary(deps, conv.id),
          // Bounded: agents get the latest 300 messages on open.
          deps.db.all<MessageRow>(
            'SELECT * FROM (SELECT * FROM messages WHERE conversation_id = ? ORDER BY created_at DESC, id DESC LIMIT 300) t ORDER BY created_at ASC, id ASC',
            [conv.id],
          ),
          loadHistory(deps, conv.id),
        ]);
        const messages = await hydrateMessages(deps, rows);
        reply({ conversation, messages, history });
      }),
    );

    // ── Agent sends a message ──
    const allowAgentMsg = makeLimiter(MSG_RATE_MAX * 2, MSG_RATE_WINDOW_MS);
    socket.on(
      EV.AgentMessage,
      safe(socket, async (payload: unknown) => {
        if (!allowAgentMsg()) return;
        const p = (payload ?? {}) as Record<string, unknown>;
        const body = asString(p.body)?.trim().slice(0, MAX_MESSAGE_CHARS);
        const tempId = asString(p.tempId) ?? undefined;
        const conv = await getConversation(deps, asString(p.conversationId));
        if (!conv || !body) return;
        if (conv.status === 'CLOSED') {
          socket.emit(EV.AppError, { message: 'Conversation is closed' });
          return;
        }
        if (conv.status === 'WAITING') {
          // Zendesk-style "start typing to join": messaging a queued chat claims it.
          const isMine = conv.assigned_user_id === data.userId;
          const isBoss = data.role === 'LEAD' || data.role === 'ADMIN';
          const allowed =
            isMine ||
            (isBoss
              ? await canManageConversation(deps, conv, data.userId, data.role)
              : !conv.assigned_user_id &&
                (await userCanAccessWebsite(deps, data.userId, data.role, conv.website_id)));
          if (!allowed) {
            socket.emit(EV.AppError, { message: 'This conversation is not assigned to you' });
            return;
          }
          if (!isMine) {
            await recordAssignment(
              deps,
              conv.id,
              conv.assigned_user_id,
              data.userId,
              conv.assigned_user_id ? 'TRANSFER' : 'AUTO',
            );
            await deps.db.run('UPDATE conversations SET assigned_user_id = ? WHERE id = ?', [
              data.userId,
              conv.id,
            ]);
          }
          await socket.join(convRoom(conv.id));
          await postMessage(deps, {
            conversationId: conv.id,
            senderType: 'SYSTEM',
            kind: 'SYSTEM',
            body: `${data.name} has joined the chat`,
            agentOnly: true, // internal notice — never surface to the visitor
          });
          await activateConversation(deps, conv.id);
        } else {
          if (!(await canSendAsAgent(deps, conv, data.userId, data.role))) {
            socket.emit(EV.AppError, { message: 'Only the assigned agent can send messages here' });
            return;
          }
          // A new agent (admin/manager/lead) jumping into an ongoing chat gets
          // announced the first time they speak here.
          await socket.join(convRoom(conv.id));
          await announceAgentJoin(deps, conv.id, data.userId, data.name);
        }
        await postMessage(deps, {
          conversationId: conv.id,
          senderType: 'AGENT',
          senderUserId: data.userId,
          body,
          tempId,
        });
      }),
    );

    // ── CSR-initiated chat → OFFERED ──
    socket.on(
      EV.AgentStartChat,
      safe(socket, async (payload: unknown, ack?: unknown) => {
        const reply =
          typeof ack === 'function'
            ? (ack as (r: { conversationId: string } | { error: string }) => void)
            : null;
        const p = (payload ?? {}) as Record<string, unknown>;
        const websiteId = asString(p.websiteId);
        const visitorId = asString(p.visitorId);
        // Body is optional — an agent can 'just claim' the visitor by pressing
        // Space in the visitor drawer, then type the first message once the
        // chat is open. When body is empty we skip the opener post entirely
        // (visitor's widget still gets the OFFERED status via emitConversationStatus).
        const body = asString(p.body)?.trim() ?? '';
        if (!websiteId || !visitorId) return;

        if (!(await userCanAccessWebsite(deps, data.userId, data.role, websiteId))) {
          socket.emit(EV.AppError, { message: 'You do not have access to this website' });
          return;
        }
        const visitor = await deps.db.get<VisitorRow>(
          'SELECT * FROM visitors WHERE id = ? AND website_id = ?',
          [visitorId, websiteId],
        );
        if (!visitor) {
          socket.emit(EV.AppError, { message: 'Visitor not found' });
          return;
        }
        const open = await deps.db.get<ConversationRow>(
          "SELECT * FROM conversations WHERE visitor_id = ? AND status IN ('WAITING','OFFERED','ACTIVE') LIMIT 1",
          [visitorId],
        );
        if (open) {
          // Already claimed by ANOTHER agent and this one can't manage it →
          // don't hand back an id they'd only fail to open (that races two
          // Space presses into a white screen). Tell them it's taken instead.
          if (
            open.assigned_user_id &&
            open.assigned_user_id !== data.userId &&
            !(await canManageConversation(deps, open, data.userId, data.role))
          ) {
            reply?.({ error: 'taken' });
            socket.emit(EV.AppError, {
              message: 'Another agent is already helping this visitor.',
            });
            return;
          }
          // Already an open conversation — post into it (when allowed) and
          // hand its id back so the client opens that chat instead of erroring.
          await socket.join(convRoom(open.id));
          if (await canSendAsAgent(deps, open, data.userId, data.role)) {
            // Always announce the join (agent-only), whether the opener has
            // text or not — the chat log otherwise looks empty on the agent
            // side after a Space-bar claim.
            await announceAgentJoin(deps, open.id, data.userId, data.name);
            if (body) {
              await postMessage(deps, {
                conversationId: open.id,
                senderType: 'AGENT',
                senderUserId: data.userId,
                body,
              });
            }
          }
          reply?.({ conversationId: open.id });
          return;
        }

        const conversationId = newId();
        // Atomic claim: only create the conversation if this visitor still has
        // no open one. If two agents press Space at the same instant, the guard
        // lets exactly one INSERT land — the other finds the winner below.
        await deps.db.run(
          `INSERT INTO conversations (id, website_id, visitor_id, status, assigned_user_id, created_at, activated_at, closed_at)
           SELECT ?, ?, ?, 'OFFERED', ?, ?, NULL, NULL
           WHERE NOT EXISTS (
             SELECT 1 FROM conversations
              WHERE visitor_id = ? AND status IN ('WAITING','OFFERED','ACTIVE')
           )`,
          [conversationId, websiteId, visitorId, data.userId, nowIso(), visitorId],
        );
        // Who actually holds the open chat now? Mine if I won the race, else the
        // agent who beat me to it.
        const winner = await deps.db.get<ConversationRow>(
          "SELECT * FROM conversations WHERE visitor_id = ? AND status IN ('WAITING','OFFERED','ACTIVE') ORDER BY created_at ASC LIMIT 1",
          [visitorId],
        );
        if (!winner || winner.id !== conversationId) {
          // Lost the race (my INSERT was skipped by the guard). Route to the
          // winner: open it if it's mine/manageable, otherwise report 'taken'.
          if (winner) {
            const mine = winner.assigned_user_id === data.userId;
            const canManage = await canManageConversation(deps, winner, data.userId, data.role);
            if (mine || canManage) {
              await socket.join(convRoom(winner.id));
              reply?.({ conversationId: winner.id });
              return;
            }
          }
          reply?.({ error: 'taken' });
          socket.emit(EV.AppError, {
            message: 'Another agent is already helping this visitor.',
          });
          return;
        }
        await recordAssignment(deps, conversationId, null, data.userId, 'OFFER');
        await socket.join(convRoom(conversationId));

        // Pull the visitor's live widget sockets into the conversation room so
        // they receive the opening message + status immediately.
        for (const ws of deps.io.of(WIDGET_NAMESPACE).sockets.values()) {
          const wd = ws.data as Partial<WidgetSocketData>;
          if (wd.visitorId === visitorId) {
            await ws.join(convRoom(conversationId));
            wd.conversationId = conversationId;
          }
        }

        if (body) {
          await postMessage(deps, {
            conversationId,
            senderType: 'AGENT',
            senderUserId: data.userId,
            body,
          });
        } else {
          // No opener typed — leave a 'joined the chat' notice so the agent
          // side isn't staring at an empty transcript. agentOnly keeps it off
          // the visitor's widget so they never see 'X has joined' as the
          // very first line before an actual message.
          await postMessage(deps, {
            conversationId,
            senderType: 'SYSTEM',
            kind: 'SYSTEM',
            body: `${data.name} has joined the chat`,
            agentOnly: true,
          });
        }
        await emitConversationStatus(deps, conversationId);
        await emitConversationAgent(deps, conversationId);

        // Agent-initiated outreach the visitor never answers is NOT counted as
        // missed — it just stays open until the 12h inactivity sweep closes it.
        reply?.({ conversationId });
      }),
    );

    // ── Accept (WAITING → ACTIVE) ──
    socket.on(
      EV.AgentAccept,
      safe(socket, async (payload: unknown) => {
        const p = (payload ?? {}) as Record<string, unknown>;
        const conv = await getConversation(deps, asString(p.conversationId));
        if (!conv) return;
        if (conv.status !== 'WAITING') {
          socket.emit(EV.AppError, { message: 'Conversation is not waiting to be accepted' });
          return;
        }
        if (conv.assigned_user_id !== data.userId) {
          if (!conv.assigned_user_id) {
            // Unassigned queued chat — any agent (CSR included) who can access
            // the website may claim it. Chats are no longer auto-assigned, so
            // this is the normal pickup path for the Incoming/Offline queues.
            if (!(await userCanAccessWebsite(deps, data.userId, data.role, conv.website_id))) {
              socket.emit(EV.AppError, { message: 'You do not have access to this conversation' });
              return;
            }
          } else {
            // Taking over a chat already assigned to someone else — only a
            // LEAD/ADMIN who manages it may do that.
            if (data.role !== 'LEAD' && data.role !== 'ADMIN') {
              socket.emit(EV.AppError, { message: 'This conversation is not assigned to you' });
              return;
            }
            if (!(await canManageConversation(deps, conv, data.userId, data.role))) {
              socket.emit(EV.AppError, { message: 'You do not have access to this conversation' });
              return;
            }
          }
          await recordAssignment(
            deps,
            conv.id,
            conv.assigned_user_id,
            data.userId,
            conv.assigned_user_id ? 'TRANSFER' : 'AUTO',
          );
          await deps.db.run('UPDATE conversations SET assigned_user_id = ? WHERE id = ?', [
            data.userId,
            conv.id,
          ]);
        }
        await socket.join(convRoom(conv.id));
        await postMessage(deps, {
          conversationId: conv.id,
          senderType: 'SYSTEM',
          kind: 'SYSTEM',
          body: `${data.name} has joined the chat`,
          agentOnly: true, // internal notice — never surface to the visitor
        });
        await activateConversation(deps, conv.id);
      }),
    );

    // ── Close ──
    socket.on(
      EV.AgentClose,
      safe(socket, async (payload: unknown) => {
        const p = (payload ?? {}) as Record<string, unknown>;
        const conv = await getConversation(deps, asString(p.conversationId));
        if (!conv) return;
        if (!(await canManageConversation(deps, conv, data.userId, data.role))) {
          socket.emit(EV.AppError, { message: 'You cannot close this conversation' });
          return;
        }
        await closeConversation(deps, conv.id);
      }),
    );

    // ── Transfer ──
    socket.on(
      EV.AgentTransfer,
      safe(socket, async (payload: unknown) => {
        const p = (payload ?? {}) as Record<string, unknown>;
        const toUserId = asString(p.toUserId);
        const conv = await getConversation(deps, asString(p.conversationId));
        if (!conv || !toUserId) return;
        if (!(await canManageConversation(deps, conv, data.userId, data.role))) {
          socket.emit(EV.AppError, { message: 'You cannot transfer this conversation' });
          return;
        }
        if (toUserId === conv.assigned_user_id) {
          socket.emit(EV.AppError, { message: 'Conversation is already assigned to that agent' });
          return;
        }
        const target = await deps.db.get<UserRow>('SELECT * FROM users WHERE id = ?', [toUserId]);
        if (!target) {
          socket.emit(EV.AppError, { message: 'Target agent not found' });
          return;
        }
        const isMember = await userCanAccessWebsite(
          deps,
          target.id,
          target.role,
          conv.website_id,
        );
        if (!isMember || !(await hasCapacity(deps, target))) {
          socket.emit(EV.AppError, { message: `${target.name} is not available right now` });
          return;
        }
        await transferConversation(deps, conv.id, conv.assigned_user_id, toUserId);
      }),
    );

    // ── Typing indicator → widget side ──
    socket.on(
      EV.AgentTyping,
      safe(socket, (payload: unknown) => {
        const p = (payload ?? {}) as Record<string, unknown>;
        const conversationId = asString(p.conversationId);
        if (!conversationId) return;
        deps.io
          .of(WIDGET_NAMESPACE)
          .to(convRoom(conversationId))
          .emit(EV.ChatTyping, {
            conversationId,
            from: 'AGENT',
            typing: p.typing === true,
          });
      }),
    );

    // ── Read receipts (agent read visitor messages) ──
    socket.on(
      EV.AgentRead,
      safe(socket, async (payload: unknown) => {
        const p = (payload ?? {}) as Record<string, unknown>;
        const messageIds = asStringArray(p.messageIds);
        const conv = await getConversation(deps, asString(p.conversationId));
        if (!conv || messageIds.length === 0) return;
        if (!(await canSendAsAgent(deps, conv, data.userId, data.role))) return;
        const readAt = nowIso();
        await deps.db.run(
          `UPDATE messages SET read_at = ? WHERE conversation_id = ? AND sender_type = 'VISITOR' AND read_at IS NULL AND id IN (${messageIds.map(() => '?').join(',')})`,
          [readAt, conv.id, ...messageIds],
        );
        const receipt = { conversationId: conv.id, messageIds, readAt };
        deps.io.of(WIDGET_NAMESPACE).to(convRoom(conv.id)).emit(EV.ChatReceipt, receipt);
        deps.io.of(AGENT_NAMESPACE).to(convRoom(conv.id)).emit(EV.ChatReceipt, receipt);
        await emitInboxUpdate(deps, conv.id);
      }),
    );

    // ── Request feedback: pops the rating stars on the visitor's widget ──
    socket.on(
      EV.AgentRequestFeedback,
      safe(socket, async (payload: unknown) => {
        const p = (payload ?? {}) as Record<string, unknown>;
        const conv = await getConversation(deps, asString(p.conversationId));
        if (!conv) return;
        if (conv.status !== 'ACTIVE') {
          socket.emit(EV.AppError, { message: 'Feedback can only be requested in an active chat' });
          return;
        }
        if (!(await canSendAsAgent(deps, conv, data.userId, data.role))) {
          socket.emit(EV.AppError, { message: 'Only the assigned agent can request feedback' });
          return;
        }
        if (conv.rating != null) {
          socket.emit(EV.AppError, { message: 'The customer already rated this chat' });
          return;
        }
        deps.io.of(WIDGET_NAMESPACE).to(convRoom(conv.id)).emit(EV.ChatFeedbackRequest, {
          conversationId: conv.id,
        });
        await postMessage(deps, {
          conversationId: conv.id,
          senderType: 'SYSTEM',
          kind: 'SYSTEM',
          body: `${data.name} requested feedback — the customer can now rate this chat ⭐`,
        });
      }),
    );

    // ── Watch a website (visitor list + inbox fan-out) ──
    socket.on(
      EV.AgentWatchWebsite,
      safe(socket, async (payload: unknown) => {
        const p = (payload ?? {}) as Record<string, unknown>;
        const websiteId = asString(p.websiteId);
        if (!websiteId) return;
        if (!(await userCanAccessWebsite(deps, data.userId, data.role, websiteId))) {
          socket.emit(EV.AppError, { message: 'You do not have access to this website' });
          return;
        }
        await socket.join(websiteRoom(websiteId));
        socket.emit(EV.VisitorsUpdate, {
          websiteId,
          visitors: await buildVisitorList(deps, websiteId),
        });
      }),
    );

    // ── Manual availability (Online / Away) ──
    socket.on(
      EV.AgentSetAway,
      safe(socket, async (payload: unknown) => {
        const p = (payload ?? {}) as Record<string, unknown>;
        const away = p.away === true;
        deps.presence.setAgentAway(data.userId, away);
        deps.io
          .of(AGENT_NAMESPACE)
          .emit(EV.PresenceUpdate, { userId: data.userId, online: true, away });
        // Coming back available may pick up queued chats.
        if (!away) {
          const sites = await deps.db.all<{ website_id: string }>(
            'SELECT DISTINCT website_id FROM conversations WHERE status = ?',
            ['WAITING'],
          );
          for (const s of sites) await drainQueue(deps, s.website_id);
        }
      }),
    );

    // ── Internal team chat (agent ↔ agent DMs) ──
    const allowDM = makeLimiter(MSG_RATE_MAX * 2, MSG_RATE_WINDOW_MS);
    socket.on(
      EV.AgentDMSend,
      safe(socket, async (payload: unknown, ack?: unknown) => {
        const reply = typeof ack === 'function' ? (ack as (res: unknown) => void) : () => {};
        if (!allowDM()) {
          reply({ error: 'Slow down — too many messages.' });
          return;
        }
        const p = (payload ?? {}) as Record<string, unknown>;
        const toUserId = asString(p.toUserId);
        const body = asString(p.body)?.trim().slice(0, 4000);
        const tempId = asString(p.tempId);
        if (!toUserId || !body) {
          reply({ error: 'toUserId and body are required' });
          return;
        }
        if (toUserId === data.userId) {
          reply({ error: 'You cannot DM yourself' });
          return;
        }
        // Sanity check: peer must be a real user (any signed-in agent can DM
        // anyone in the org — same trust model as canned response shortcuts).
        const peer = await deps.db.get<{ id: string }>('SELECT id FROM users WHERE id = ?', [
          toUserId,
        ]);
        if (!peer) {
          reply({ error: 'Recipient not found' });
          return;
        }
        const id = newId();
        const createdAt = nowIso();
        await deps.db.run(
          'INSERT INTO agent_messages (id, from_user_id, to_user_id, body, created_at) VALUES (?, ?, ?, ?, ?)',
          [id, data.userId, toUserId, body, createdAt],
        );
        const message = {
          id,
          fromUserId: data.userId,
          toUserId,
          body,
          createdAt,
          readAt: null,
          tempId,
        };
        // Echo to sender (all their tabs) AND deliver to recipient (all theirs).
        deps.io.of(AGENT_NAMESPACE).to(userRoom(data.userId)).emit(EV.AgentDM, { message });
        deps.io.of(AGENT_NAMESPACE).to(userRoom(toUserId)).emit(EV.AgentDM, { message });
        reply({ message });
      }),
    );

    socket.on(
      EV.AgentDMRead,
      safe(socket, async (payload: unknown) => {
        const p = (payload ?? {}) as Record<string, unknown>;
        const fromUserId = asString(p.fromUserId);
        if (!fromUserId) return;
        const readAt = nowIso();
        await deps.db.run(
          'UPDATE agent_messages SET read_at = ? WHERE to_user_id = ? AND from_user_id = ? AND read_at IS NULL',
          [readAt, data.userId, fromUserId],
        );
        // Tell the ORIGINAL sender their DMs were seen (so their side updates read state).
        deps.io.of(AGENT_NAMESPACE).to(userRoom(fromUserId)).emit(EV.AgentDMReadReceipt, {
          fromUserId,
          toUserId: data.userId,
          readAt,
        });
        // Also fan out to the reader's own tabs so unread badge clears everywhere.
        deps.io.of(AGENT_NAMESPACE).to(userRoom(data.userId)).emit(EV.AgentDMReadReceipt, {
          fromUserId,
          toUserId: data.userId,
          readAt,
        });
      }),
    );

    // ── Calls (contract implemented by the features agent) ──
    registerAgentCallHandlers(deps, socket);

    // ── Disconnect ──
    socket.on('disconnect', () => {
      const wentOffline = deps.presence.removeAgent(data.userId, socket.id);
      if (wentOffline) {
        deps.io.of(AGENT_NAMESPACE).emit(EV.PresenceUpdate, { userId: data.userId, online: false });
      }
    });
  });
}

async function onAgentConnected(
  deps: AppDeps,
  socket: Socket,
  data: AgentSocketData,
): Promise<void> {
  const ctx = agentCtx.get(socket);
  if (!ctx) {
    socket.disconnect(true);
    return;
  }

  const cameOnline = deps.presence.addAgent(data.userId, socket.id);
  await socket.join(userRoom(data.userId));
  if (cameOnline) {
    deps.io.of(AGENT_NAMESPACE).emit(EV.PresenceUpdate, { userId: data.userId, online: true });
  }

  // Scoped websites + teams (ADMIN/MANAGER = all; others via team membership).
  const isAdmin = data.role === 'ADMIN' || data.role === 'MANAGER';
  const websiteRows = isAdmin
    ? await deps.db.all<WebsiteRow>('SELECT * FROM websites ORDER BY name ASC')
    : await deps.db.all<WebsiteRow>(
        'SELECT w.* FROM websites w JOIN team_members tm ON tm.team_id = w.team_id WHERE tm.user_id = ? ORDER BY w.name ASC',
        [data.userId],
      );
  const websites: Website[] = websiteRows.map((w) => ({
    id: w.id,
    name: w.name,
    label: w.label ?? null,
    logoUrl: w.logo_url,
    primaryColor: w.primary_color,
    greeting: w.greeting,
    widgetKey: w.widget_key,
    domains: (w.domains ?? '')
      .split(',')
      .map((d) => d.trim())
      .filter(Boolean),
    teamId: w.team_id,
  }));

  interface TeamRow {
    id: string;
    name: string;
  }
  const teamRows = isAdmin
    ? await deps.db.all<TeamRow>('SELECT * FROM teams ORDER BY name ASC')
    : await deps.db.all<TeamRow>(
        'SELECT t.* FROM teams t JOIN team_members tm ON tm.team_id = t.id WHERE tm.user_id = ? ORDER BY t.name ASC',
        [data.userId],
      );

  const teams: Team[] = [];
  for (const t of teamRows) {
    const memberRows = await deps.db.all<UserRow & { is_lead: number }>(
      'SELECT u.*, tm.is_lead FROM team_members tm JOIN users u ON u.id = tm.user_id WHERE tm.team_id = ?',
      [t.id],
    );
    teams.push({
      id: t.id,
      name: t.name,
      members: memberRows.map((m) => ({
        ...toUserPublic(m),
        isLead: Number(m.is_lead) === 1,
        online: deps.presence.isAgentOnline(m.id),
        away: deps.presence.isAgentAway(m.id),
      })),
    });
  }

  // A Team Lead's CSR ids ride along so the dashboard can gate the composer.
  const csrIds =
    data.role === 'LEAD'
      ? (
          await deps.db.all<{ id: string }>('SELECT id FROM users WHERE team_lead_id = ?', [
            data.userId,
          ])
        ).map((r) => r.id)
      : [];

  // Include live online/away so the client's availability toggle is correct
  // right after a (re)connect, not reset to "Online".
  const me = {
    ...toUserPublic(ctx.user),
    online: deps.presence.isAgentOnline(ctx.user.id),
    away: deps.presence.isAgentAway(ctx.user.id),
  };
  socket.emit(EV.AgentReady, { me, websites, teams, csrIds });

  // A newly-online agent may free up the queue.
  await drainQueue(deps);
}
