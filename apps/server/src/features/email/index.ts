// ─────────────────────────────────────────────────────────────
// Email transcript — when a conversation closes (or the visitor
// asks from the widget's ⋯ menu), the visitor gets a branded copy
// of the chat by email (if they shared an address and SMTP is
// configured; otherwise this is a silent no-op).
//
// Deliberately self-contained (raw SQL, no domain/* imports) so it
// can be called from domain/conversations without import cycles.
// ─────────────────────────────────────────────────────────────
import type { AppDeps } from '../../core/deps.js';

interface ConvRow {
  id: string;
  website_id: string;
  visitor_id: string;
  assigned_user_id: string | null;
  created_at: string;
  closed_at: string | null;
  rating: number | null;
  rating_comment: string | null;
}

interface WebsiteRow {
  name: string;
  primary_color: string;
  team_id: string;
}

interface VisitorRow {
  name: string | null;
  email: string | null;
  phone: string | null;
  geo_city: string | null;
  geo_country: string | null;
}

interface MsgRow {
  sender_type: string;
  body: string | null;
  kind: string;
  created_at: string;
  sender_name: string | null;
  file_name: string | null;
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
let transporter: any | null = null;
let warnedOnce = false;

function esc(s: string): string {
  return s.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
}

function fmtClock(iso: string): string {
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return '';
  const p = (n: number) => String(n).padStart(2, '0');
  return `${p(d.getHours())}:${p(d.getMinutes())}:${p(d.getSeconds())}`;
}

function fmtStarted(iso: string): string {
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return '';
  const date = d.toLocaleDateString('en-GB', { day: 'numeric', month: 'short', year: 'numeric' });
  const time = d.toLocaleTimeString([], { hour: 'numeric', minute: '2-digit' });
  const offMin = -d.getTimezoneOffset();
  const sign = offMin >= 0 ? '+' : '-';
  const abs = Math.abs(offMin);
  const off = `GMT${sign}${Math.floor(abs / 60)}${abs % 60 ? `:${String(abs % 60).padStart(2, '0')}` : ''}`;
  return `${date}, ${time} (${off})`;
}

export interface TranscriptData {
  visitorLabel: string;
  websiteName: string;
  color: string;
  startedAt: string;
  lines: { time: string; who: string | null; body: string; system: boolean; visitor: boolean }[];
  details: { label: string; value: string | null }[];
}

/** Pure HTML renderer (exported for previews/tests). Email-client-safe: tables + inline styles. */

/** Digits-only anonymous visitor label (matches the dashboard's numbering). */
function visitorNumber(id: string): string {
  const hex = id.replace(/[^0-9a-f]/gi, '').slice(0, 8);
  if (!hex) return '';
  return String(parseInt(hex, 16) % 1_000_000).padStart(6, '0');
}

export function renderTranscriptHtml(d: TranscriptData): string {
  const rows = d.lines
    .map((l) => {
      if (l.system) {
        return `
          <tr>
            <td style="padding:5px 10px;vertical-align:top;white-space:nowrap;color:#9aa4b2;font-size:12px;font-family:monospace">(${l.time})</td>
            <td colspan="2" style="padding:5px 10px;vertical-align:top;color:#9aa4b2;font-size:12.5px;font-style:italic">*** ${esc(l.body)} ***</td>
          </tr>`;
      }
      return `
        <tr>
          <td style="padding:5px 10px;vertical-align:top;white-space:nowrap;color:#9aa4b2;font-size:12px;font-family:monospace">(${l.time})</td>
          <td style="padding:5px 10px;vertical-align:top;font-weight:700;white-space:nowrap;color:${l.visitor ? d.color : '#334155'};font-size:13.5px">${esc(l.who ?? '')}</td>
          <td style="padding:5px 10px;vertical-align:top;color:#0f172a;font-size:13.5px;line-height:1.45">${esc(l.body)}</td>
        </tr>`;
    })
    .join('');

  const detailRows = d.details
    .map(
      (r) => `
        <tr>
          <td style="padding:7px 12px;color:#9aa4b2;font-size:11px;font-weight:700;letter-spacing:.06em;text-transform:uppercase;white-space:nowrap;vertical-align:top;width:130px">${esc(r.label)}</td>
          <td style="padding:7px 12px;color:#0f172a;font-size:13.5px">${r.value ? esc(r.value) : '<span style="color:#cbd5e1">—</span>'}</td>
        </tr>`,
    )
    .join('');

  return `
  <div style="background:#f1f5f9;padding:28px 12px;font-family:-apple-system,Segoe UI,Roboto,Arial,sans-serif">
    <div style="max-width:680px;margin:0 auto">
      <div style="background:${d.color};border-radius:14px 14px 0 0;padding:20px 26px;color:#ffffff">
        <div style="font-size:19px;font-weight:800">${esc(d.websiteName)}</div>
        <div style="font-size:13px;opacity:.92;margin-top:2px">Chat transcript with ${esc(d.visitorLabel)}</div>
      </div>
      <div style="background:#ffffff;border:1px solid #e2e8f0;border-top:none;padding:22px 16px">
        <div style="font-weight:800;color:#0f172a;font-size:14.5px;padding:0 10px 10px">Chat started on ${esc(d.startedAt)}</div>
        <table style="border-collapse:collapse;width:100%">${rows}</table>
      </div>
      <div style="background:#ffffff;border:1px solid #e2e8f0;border-top:1px solid #eef2f7;padding:14px 16px">
        <table style="border-collapse:collapse;width:100%">${detailRows}</table>
      </div>
      <div style="background:#ffffff;border:1px solid #e2e8f0;border-top:none;border-radius:0 0 14px 14px;padding:16px 26px;color:#94a3b8;font-size:12.5px">
        Thanks for chatting with ${esc(d.websiteName)} — just reply to this email if you need anything else.
      </div>
    </div>
  </div>`;
}

/** Collect everything for a conversation's transcript email. */
async function buildTranscriptData(
  deps: AppDeps,
  conversationId: string,
): Promise<{ data: TranscriptData; email: string; websiteName: string } | null> {
  const conv = await deps.db.get<ConvRow>(
    'SELECT id, website_id, visitor_id, assigned_user_id, created_at, closed_at, rating, rating_comment FROM conversations WHERE id = ?',
    [conversationId],
  );
  if (!conv) return null;

  const [website, visitor, messages, agent, team, firstPage] = await Promise.all([
    deps.db.get<WebsiteRow>('SELECT name, primary_color, team_id FROM websites WHERE id = ?', [
      conv.website_id,
    ]),
    deps.db.get<VisitorRow>(
      'SELECT name, email, phone, geo_city, geo_country FROM visitors WHERE id = ?',
      [conv.visitor_id],
    ),
    deps.db.all<MsgRow>(
      `SELECT m.sender_type, m.body, m.kind, m.created_at,
              u.name AS sender_name, f.original_name AS file_name
         FROM messages m
         LEFT JOIN users u ON u.id = m.sender_user_id
         LEFT JOIN files f ON f.id = m.file_id
        WHERE m.conversation_id = ? AND m.agent_only = 0
        ORDER BY m.created_at ASC`,
      [conversationId],
    ),
    conv.assigned_user_id
      ? deps.db.get<{ name: string }>('SELECT name FROM users WHERE id = ?', [
          conv.assigned_user_id,
        ])
      : Promise.resolve(undefined),
    deps.db.get<{ name: string }>(
      'SELECT t.name FROM teams t JOIN websites w ON w.team_id = t.id WHERE w.id = ?',
      [conv.website_id],
    ),
    deps.db.get<{ url: string }>(
      'SELECT url FROM visitor_pages WHERE visitor_id = ? ORDER BY created_at ASC LIMIT 1',
      [conv.visitor_id],
    ),
  ]);
  if (!website || !visitor?.email) return null;

  const visitorLabel = visitor.name || `Visitor ${visitorNumber(conv.visitor_id)}`;

  const lines: TranscriptData['lines'] = [
    {
      time: fmtClock(conv.created_at),
      who: null,
      body: `${visitorLabel} joined the chat`,
      system: true,
      visitor: false,
    },
    ...messages.map((m) => {
      if (m.kind === 'SYSTEM') {
        return { time: fmtClock(m.created_at), who: null, body: m.body ?? '', system: true, visitor: false };
      }
      const who =
        m.sender_type === 'VISITOR'
          ? visitorLabel
          : m.sender_type === 'BOT'
            ? 'Alex'
            : m.sender_name || 'Agent';
      const body =
        m.kind === 'FILE'
          ? `📎 File: ${m.file_name ?? 'attachment'}`
          : m.kind === 'CALL'
            ? `📞 ${m.body ?? 'Call'}`
            : (m.body ?? '');
      return {
        time: fmtClock(m.created_at),
        who,
        body,
        system: false,
        visitor: m.sender_type === 'VISITOR',
      };
    }),
  ];
  if (conv.closed_at) {
    lines.push({
      time: fmtClock(conv.closed_at),
      who: null,
      body: 'Chat ended',
      system: true,
      visitor: false,
    });
  }

  const location = [visitor.geo_city, visitor.geo_country].filter(Boolean).join(', ') || null;
  const data: TranscriptData = {
    visitorLabel,
    websiteName: website.name,
    color: website.primary_color || '#5865f2',
    startedAt: fmtStarted(conv.created_at),
    lines,
    details: [
      { label: 'Name', value: visitor.name },
      { label: 'Email', value: visitor.email },
      { label: 'Phone', value: visitor.phone },
      { label: 'Location', value: location },
      { label: 'URL', value: firstPage?.url ?? null },
      { label: 'Department', value: team?.name ?? null },
      { label: 'Served by', value: agent?.name ?? null },
      {
        label: 'Rating',
        value:
          conv.rating != null
            ? `${'★'.repeat(Number(conv.rating))}${'☆'.repeat(5 - Number(conv.rating))}`
            : null,
      },
      { label: 'Comment', value: conv.rating_comment },
    ],
  };
  return { data, email: visitor.email, websiteName: website.name };
}

/** Fire-and-forget: email the transcript to the visitor (close or on demand). */
export async function sendTranscriptEmail(deps: AppDeps, conversationId: string): Promise<void> {
  const { config } = deps;
  if (!config.smtpHost) {
    if (!warnedOnce) {
      warnedOnce = true;
      console.log('[email] SMTP not configured — transcript emails disabled (set SMTP_HOST to enable)');
    }
    return;
  }

  try {
    const built = await buildTranscriptData(deps, conversationId);
    if (!built) return;

    if (!transporter) {
      const { default: nodemailer } = await import('nodemailer');
      transporter = nodemailer.createTransport({
        host: config.smtpHost,
        port: config.smtpPort,
        secure: config.smtpPort === 465,
        auth: config.smtpUser ? { user: config.smtpUser, pass: config.smtpPass ?? '' } : undefined,
      });
    }

    await transporter.sendMail({
      from: config.smtpFrom || config.smtpUser || `no-reply@${config.smtpHost}`,
      to: built.email,
      subject: `Chat transcript — ${built.websiteName}`,
      html: renderTranscriptHtml(built.data),
    });
    console.log(`[email] transcript sent to ${built.email} (conversation ${conversationId})`);
  } catch (err) {
    console.warn('[email] transcript failed:', (err as Error).message);
  }
}
