// ─── Message list building blocks ────────────────────────────
import type { ChatMessage } from '@livechat/shared';
import type { JSX } from 'preact';
import { useState } from 'preact/hooks';
import { dayKey, dayLabel, fmtBytes, fmtTime, initials } from './util';

/** Agent bubble text: shows the auto-translation, with a tap to see the original. */
function BubbleText({ m }: { m: LocalMessage }): JSX.Element {
  const [showOriginal, setShowOriginal] = useState(false);
  // Visitors read the agent's words in their own language; their own messages
  // (and anything without a translation) render as-is.
  const translated = m.senderType === 'AGENT' ? m.translatedBody : null;
  if (!translated) return <>{linkify(m.body)}</>;
  return (
    <>
      {linkify(showOriginal ? m.body : translated)}
      <button
        type="button"
        class="lc-translate-toggle"
        onClick={() => setShowOriginal((v) => !v)}
      >
        {showOriginal ? 'Show translation' : 'Show original'}
      </button>
    </>
  );
}

/** Messages carried in local state — pending flag for optimistic sends. */
export type LocalMessage = ChatMessage & { pending?: boolean };

// ─── Inline icons (all pure SVG, currentColor) ───────────────

export const IconChat = ({ size = 26 }: { size?: number }): JSX.Element => (
  <svg width={size} height={size} viewBox="0 0 24 24" fill="currentColor" class="lc-launcher-icon">
    <path d="M12 3C6.8 3 2.6 6.6 2.6 11c0 2.1 1 4 2.5 5.4-.2 1.2-.8 2.5-1.7 3.4-.2.2-.1.6.2.6 1.9.1 3.7-.6 5-1.5 1.1.3 2.2.5 3.4.5 5.2 0 9.4-3.6 9.4-8.2S17.2 3 12 3Z" />
  </svg>
);

export const IconClose = (): JSX.Element => (
  <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.4" stroke-linecap="round">
    <path d="M6 9l6 6 6-6" />
  </svg>
);

export const IconSend = (): JSX.Element => (
  <svg width="17" height="17" viewBox="0 0 24 24" fill="currentColor">
    <path d="M3.4 20.6 21.8 12 3.4 3.4 3.3 10l13 2-13 2z" />
  </svg>
);

export const IconClip = (): JSX.Element => (
  <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round">
    <path d="M21 12.5 12.3 21a5.6 5.6 0 0 1-8-8l8.8-8.6a3.8 3.8 0 0 1 5.4 5.3l-8.8 8.6a1.9 1.9 0 0 1-2.7-2.7l8-7.9" />
  </svg>
);

export const IconFile = (): JSX.Element => (
  <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round">
    <path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z" />
    <path d="M14 2v6h6" />
  </svg>
);

export const IconPhone = ({ size = 18 }: { size?: number }): JSX.Element => (
  <svg width={size} height={size} viewBox="0 0 24 24" fill="currentColor">
    <path d="M6.6 10.8c1.4 2.8 3.8 5.1 6.6 6.6l2.2-2.2c.3-.3.7-.4 1-.2 1.1.4 2.3.6 3.6.6.6 0 1 .4 1 1V20c0 .6-.4 1-1 1C10.8 21 3 13.2 3 4c0-.6.4-1 1-1h3.5c.6 0 1 .4 1 1 0 1.2.2 2.4.6 3.6.1.3 0 .7-.2 1z" />
  </svg>
);

export const IconVideo = ({ size = 18 }: { size?: number }): JSX.Element => (
  <svg width={size} height={size} viewBox="0 0 24 24" fill="currentColor">
    <path d="M17 10.5V7a1 1 0 0 0-1-1H4a1 1 0 0 0-1 1v10a1 1 0 0 0 1 1h12a1 1 0 0 0 1-1v-3.5l4 4v-11z" />
  </svg>
);

export const IconMic = ({ off = false }: { off?: boolean }): JSX.Element => (
  <svg width="19" height="19" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.9" stroke-linecap="round" stroke-linejoin="round">
    <path d="M12 2a3 3 0 0 0-3 3v6a3 3 0 0 0 6 0V5a3 3 0 0 0-3-3z" />
    <path d="M19 10v1a7 7 0 0 1-14 0v-1M12 18v4" />
    {off && <path d="M3 3l18 18" stroke-width="2.2" />}
  </svg>
);

export const IconCam = ({ off = false }: { off?: boolean }): JSX.Element => (
  <svg width="19" height="19" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.9" stroke-linecap="round" stroke-linejoin="round">
    <path d="M17 10.5V7a1 1 0 0 0-1-1H4a1 1 0 0 0-1 1v10a1 1 0 0 0 1 1h12a1 1 0 0 0 1-1v-3.5l4 4v-11l-4 4z" />
    {off && <path d="M2 2l20 20" stroke-width="2.2" />}
  </svg>
);

export const IconHangup = (): JSX.Element => (
  <svg width="20" height="20" viewBox="0 0 24 24" fill="currentColor">
    <path d="M12 9c-3.5 0-6.7 1-9.4 2.9-.4.3-.6.7-.6 1.2v3c0 .6.4 1 1 1h3.4c.6 0 1-.4 1-1v-2c1.4-.5 2.9-.8 4.6-.8s3.2.3 4.6.8v2c0 .6.4 1 1 1H21c.6 0 1-.4 1-1v-3c0-.5-.2-.9-.6-1.2C18.7 10 15.5 9 12 9z" />
  </svg>
);

// ─── Clickable links in message text ─────────────────────────

const URL_RE = /https?:\/\/[^\s<>"')\]]+[^\s<>"')\].,!?;:]/g;

/** Render plain text with URLs as clickable links (safe — no innerHTML). */
export function linkify(text: string): (string | JSX.Element)[] {
  const parts: (string | JSX.Element)[] = [];
  let last = 0;
  for (const m of text.matchAll(URL_RE)) {
    const idx = m.index ?? 0;
    if (idx > last) parts.push(text.slice(last, idx));
    parts.push(
      <a key={idx} class="lc-link" href={m[0]} target="_blank" rel="noopener noreferrer">
        {m[0]}
      </a>,
    );
    last = idx + m[0].length;
  }
  if (last < text.length) parts.push(text.slice(last));
  return parts;
}

// ─── Ticks (sent / delivered / read) ─────────────────────────

export function Ticks({ m }: { m: LocalMessage }): JSX.Element {
  const read = !!m.readAt;
  const delivered = !!m.deliveredAt;
  return (
    <span class={read ? 'lc-ticks lc-ticks-read' : 'lc-ticks'} title={read ? 'Read' : delivered ? 'Delivered' : 'Sent'}>
      <svg width="16" height="11" viewBox="0 0 18 12" fill="none" stroke="currentColor" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round">
        <path d="M1 6.5 4.5 10 11 2.5" />
        {(read || delivered) && <path d="M7.5 8.5 9 10 15.5 2.5" />}
      </svg>
    </span>
  );
}

// ─── Cards ───────────────────────────────────────────────────

export function FileCard({ m, server, token }: { m: LocalMessage; server: string; token: string }): JSX.Element {
  const file = m.file;
  if (!file) return <div class="lc-file">{m.body || 'File'}</div>;
  const href = file.downloadUrl ? `${server}${file.downloadUrl}?token=${encodeURIComponent(token)}` : null;
  const isImage = /^image\//i.test(file.mime || '');
  const isVideo = /^video\//i.test(file.mime || '');

  if (href && file.scanStatus === 'CLEAN' && isImage) {
    return (
      <a class="lc-image" href={href} target="_blank" rel="noopener noreferrer" title={file.originalName}>
        <img
          src={href}
          alt={file.originalName}
          loading="lazy"
          onError={(e) => {
            // A reload mid-download (server restart, flaky network) must not leave a broken picture: retry a few times.
            const img = e.currentTarget as HTMLImageElement & { _tries?: number };
            img._tries = (img._tries ?? 0) + 1;
            if (img._tries <= 4) setTimeout(() => { img.src = `${href}&r=${Date.now()}`; }, 1500 * img._tries);
          }}
        />
      </a>
    );
  }

  if (href && file.scanStatus === 'CLEAN' && isVideo) {
    return <video class="lc-video" src={href} controls preload="metadata" />;
  }
  return (
    <div class="lc-file">
      <div class="lc-file-icon">
        <IconFile />
      </div>
      <div class="lc-file-info">
        <div class="lc-file-name">{file.originalName}</div>
        <div class="lc-file-sub">
          {fmtBytes(file.size)}
          {file.scanStatus === 'PENDING' ? ' · Scanning…' : ''}
        </div>
      </div>
      {file.scanStatus === 'BLOCKED' ? (
        <span class="lc-file-blocked">Blocked</span>
      ) : href && file.scanStatus === 'CLEAN' ? (
        <a class="lc-file-dl" href={href} target="_blank" rel="noopener noreferrer">
          Download
        </a>
      ) : null}
    </div>
  );
}

export function CallCard({ m }: { m: LocalMessage }): JSX.Element {
  const kind = m.call?.kind ?? (m.body.toLowerCase().includes('video') ? 'VIDEO' : 'AUDIO');
  const status = m.call?.status;
  const statusLabel =
    status === 'ENDED' ? 'Call ended' : status === 'DECLINED' ? 'Declined' : status === 'ACTIVE' ? 'In progress' : 'Ringing…';
  return (
    <div class="lc-callmsg">
      <div class="lc-file-icon">{kind === 'VIDEO' ? <IconVideo /> : <IconPhone />}</div>
      <div>
        <div class="lc-callmsg-body">{m.body || (kind === 'VIDEO' ? 'Video call' : 'Audio call')}</div>
        <div class="lc-callmsg-sub">{statusLabel}</div>
      </div>
    </div>
  );
}

// ─── One message row ─────────────────────────────────────────

export interface AgentChip {
  name: string;
  avatarColor: string;
  avatarUrl?: string | null;
}

export interface MessageRowProps {
  m: LocalMessage;
  server: string;
  token: string;
  fallbackAgent: AgentChip | null;
}

/** Avatar dot: profile photo when available, colored initials otherwise. */
export function AvatarDot({ who, server }: { who: AgentChip | null | undefined; server: string }): JSX.Element {
  if (who?.avatarUrl) {
    const src = /^https?:/i.test(who.avatarUrl) ? who.avatarUrl : `${server}${who.avatarUrl}`;
    return <img class="lc-avatar-dot lc-avatar-img" src={src} alt="" />;
  }
  return (
    <div class="lc-avatar-dot" style={`background:${who?.avatarColor ?? '#64748b'}`}>
      {initials(who?.name)}
    </div>
  );
}

export function MessageRow({ m, server, token, fallbackAgent }: MessageRowProps): JSX.Element {
  if (m.senderType === 'SYSTEM' || m.kind === 'SYSTEM') {
    return <div class="lc-system">{m.body}</div>;
  }

  const content =
    m.kind === 'FILE' ? (
      <FileCard m={m} server={server} token={token} />
    ) : m.kind === 'CALL' ? (
      <CallCard m={m} />
    ) : (
      <div class={`lc-bubble ${m.senderType === 'VISITOR' ? 'lc-bubble-v' : 'lc-bubble-a'} ${m.pending ? 'lc-pending' : ''}`}>
        <BubbleText m={m} />
      </div>
    );

  if (m.senderType === 'VISITOR') {
    return (
      <div class="lc-row lc-row-v">
        {content}
        <div class="lc-meta">
          <span>{fmtTime(m.createdAt)}</span>
          <Ticks m={m} />
        </div>
      </div>
    );
  }

  const sender = m.sender ?? fallbackAgent;
  return (
    <div class="lc-row lc-row-a">
      <div class="lc-arow">
        <AvatarDot who={sender} server={server} />
        {content}
      </div>
      <div class="lc-meta" style="padding-left:31px">
        <span>{sender?.name ? `${sender.name} · ` : ''}{fmtTime(m.createdAt)}</span>
      </div>
    </div>
  );
}

/** Interleave date separators between message rows. */
export function renderMessages(
  messages: LocalMessage[],
  props: Omit<MessageRowProps, 'm'>,
): JSX.Element[] {
  const out: JSX.Element[] = [];
  let lastDay = '';
  for (const m of messages) {
    const dk = dayKey(m.createdAt);
    if (dk !== lastDay) {
      lastDay = dk;
      out.push(
        <div class="lc-day" key={`day-${dk}`}>
          {dayLabel(m.createdAt)}
        </div>,
      );
    }
    out.push(<MessageRow key={m.tempId ?? m.id} m={m} {...props} />);
  }
  return out;
}

export function TypingRow({ agent, server }: { agent: AgentChip | null; server: string }): JSX.Element {
  return (
    <div class="lc-row lc-row-a">
      <div class="lc-arow">
        <AvatarDot who={agent} server={server} />
        <div class="lc-bubble lc-bubble-a">
          <span class="lc-dots">
            <span />
            <span />
            <span />
          </span>
        </div>
      </div>
    </div>
  );
}

// ─── CSAT rating card (shown after the conversation closes) ──
export function RatingCard({
  onSubmit,
}: {
  onSubmit: (rating: number, comment: string) => void;
}): JSX.Element {
  const [hover, setHover] = useState(0);
  const [stars, setStars] = useState(0);
  const [comment, setComment] = useState('');

  return (
    <div class="lc-ratebox">
      <div class="lc-rate-title">How was your experience?</div>
      <div class="lc-stars">
        {[1, 2, 3, 4, 5].map((n) => (
          <button
            key={n}
            type="button"
            class={`lc-star ${(hover || stars) >= n ? 'lc-star-on' : ''}`}
            onMouseEnter={() => setHover(n)}
            onMouseLeave={() => setHover(0)}
            onClick={() => setStars(n)}
            aria-label={`${n} star${n > 1 ? 's' : ''}`}
          >
            ★
          </button>
        ))}
      </div>
      {stars > 0 && (
        <>
          <textarea
            class="lc-rate-comment"
            placeholder="Any feedback? (optional)"
            rows={2}
            value={comment}
            onInput={(e) => setComment((e.target as HTMLTextAreaElement).value)}
          />
          <button type="button" class="lc-btn lc-rate-submit" onClick={() => onSubmit(stars, comment.trim())}>
            Submit rating
          </button>
        </>
      )}
    </div>
  );
}
