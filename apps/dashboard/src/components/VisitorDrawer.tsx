import React, { useEffect, useMemo, useRef, useState } from 'react';
import type { ChatMessage, Visitor } from '@livechat/shared';
import { EV } from '@livechat/shared';
import { getSocket } from '../socket';
import { useApp } from '../state';
import { api, type VisitorChat, type VisitorProfile } from '../api';
import ChatPane from './ChatPane';
import {
  avatarGradient,
  classNames,
  durationSince,
  flagEmoji,
  formatDay,
  formatTime,
  formatWhen,
  initials,
  pageLabel,
  referrerLabel,
  uaParse,
  visitorNumber,
} from '../util';
import { IconClock, IconDownload, IconEye, IconGlobe, IconMessage, IconSend, IconX } from '../icons';

interface Props {
  visitorId: string;
  accentColor: string;
  /** Live row from the visitors stream (fresher online/currentPage than the API). */
  live?: Visitor | null;
  onClose(): void;
  onStartChat(visitor: Visitor): void;
  onOpenConversation(conversationId: string): void;
}

type Tab = 'chat' | 'chats';

export default function VisitorDrawer({
  visitorId,
  accentColor,
  live,
  onClose,
  onStartChat,
  onOpenConversation,
}: Props) {
  const { me, csrIds, openChatTab, openDockedChat } = useApp();
  const [profile, setProfile] = useState<VisitorProfile | null>(null);
  const [chats, setChats] = useState<VisitorChat[] | null>(null);
  const [lastServed, setLastServed] = useState<{
    name: string;
    avatarColor: string | null;
    servedAt: string;
  } | null>(null);
  const [tab, setTab] = useState<Tab>('chat');
  const [error, setError] = useState<string | null>(null);
  const [, forceTick] = useState(0);

  // Live chat tab — a conversation started right here in the drawer.
  const [startedId, setStartedId] = useState<string | null>(null);
  const [starterDraft, setStarterDraft] = useState('');

  // CRM edit state
  const [form, setForm] = useState({ name: '', email: '', phone: '', notes: '' });
  const [dirty, setDirty] = useState(false);
  const [saving, setSaving] = useState(false);
  const [savedFlash, setSavedFlash] = useState(false);

  // Inline transcript viewer
  const [openChatId, setOpenChatId] = useState<string | null>(null);
  const [transcript, setTranscript] = useState<Record<string, ChatMessage[]>>({});

  const panelRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    let cancelled = false;
    setProfile(null);
    setChats(null);
    setError(null);
    setOpenChatId(null);
    setStartedId(null);
    setStarterDraft('');
    setTab('chat');
    void api
      .visitorProfile(visitorId)
      .then((p) => {
        if (cancelled) return;
        setProfile(p);
        setForm({
          name: p.visitor.name ?? '',
          email: p.visitor.email ?? '',
          phone: p.visitor.phone ?? '',
          notes: p.visitor.notes ?? '',
        });
        setDirty(false);
      })
      .catch((e: Error) => !cancelled && setError(e.message));
    void api
      .visitorConversations(visitorId)
      .then((list) => !cancelled && setChats(list))
      .catch(() => !cancelled && setChats([]));
    // Scope-free 'last agent who served this visitor' — visible even when
    // the past-chats list is empty due to role scoping.
    setLastServed(null);
    void api
      .visitorLastServed(visitorId)
      .then((agent) => !cancelled && setLastServed(agent))
      .catch(() => !cancelled && setLastServed(null));
    return () => {
      cancelled = true;
    };
  }, [visitorId]);

  // Tick the "time on site" counter while the visitor is online.
  const online = live?.online ?? profile?.visitor.online ?? false;
  useEffect(() => {
    if (!online) return;
    const t = setInterval(() => forceTick((n) => n + 1), 1000);
    return () => clearInterval(t);
  }, [online]);

  useEffect(() => {
    const onKey = (e: KeyboardEvent) => e.key === 'Escape' && onClose();
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, [onClose]);

  // When this visitor's live open conversation changes (started, transferred,
  // closed), refresh the Past-chats list so it — and its count — stay honest
  // without a manual reload.
  const liveConvKey = `${live?.activeConversation?.id ?? ''}:${live?.activeConversation?.status ?? ''}:${live?.activeConversation?.assignedUserId ?? ''}`;
  const firstConvKey = useRef(true);
  useEffect(() => {
    if (firstConvKey.current) {
      firstConvKey.current = false;
      return; // initial load already fetched chats
    }
    void api
      .visitorConversations(visitorId)
      .then(setChats)
      .catch(() => undefined);
  }, [liveConvKey, visitorId]);

  const v: Visitor | null = useMemo(() => {
    if (!profile) return live ?? null;
    return { ...profile.visitor, ...(live ?? {}), notes: profile.visitor.notes };
  }, [profile, live]);

  const name = v?.name || (v ? `Visitor ${visitorNumber(v.id)}` : 'Visitor');
  const ua = uaParse(v?.userAgent);

  const save = async () => {
    if (!dirty || saving) return;
    setSaving(true);
    try {
      const updated = await api.updateVisitor(visitorId, {
        name: form.name,
        email: form.email,
        phone: form.phone,
        notes: form.notes,
      });
      setProfile((p) => (p ? { ...p, visitor: { ...p.visitor, ...updated } } : p));
      setDirty(false);
      setSavedFlash(true);
      setTimeout(() => setSavedFlash(false), 1600);
    } catch (e) {
      setError((e as Error).message);
    } finally {
      setSaving(false);
    }
  };

  const toggleTranscript = async (chatId: string) => {
    if (openChatId === chatId) {
      setOpenChatId(null);
      return;
    }
    setOpenChatId(chatId);
    if (!transcript[chatId]) {
      try {
        const messages = await api.messages(chatId);
        setTranscript((t) => ({ ...t, [chatId]: messages }));
      } catch {
        setTranscript((t) => ({ ...t, [chatId]: [] }));
      }
    }
  };

  const downloadTranscript = (chat: VisitorChat) => {
    const messages = transcript[chat.id] ?? [];
    const lines = messages
      .filter((m) => m.kind === 'TEXT' || m.kind === 'SYSTEM')
      .map((m) => {
        const who =
          m.senderType === 'VISITOR'
            ? name
            : m.senderType === 'BOT'
              ? 'Alex'
              : m.senderType === 'SYSTEM'
                ? '—'
                : m.sender?.name || 'Agent';
        return `[${new Date(m.createdAt).toLocaleString()}] ${who}: ${m.body}`;
      });
    const blob = new Blob(
      [`Chat transcript — ${formatDay(chat.createdAt)}\n\n${lines.join('\n')}\n`],
      { type: 'text/plain;charset=utf-8' },
    );
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `chat-${chat.id.slice(0, 8)}.txt`;
    a.click();
    URL.revokeObjectURL(url);
  };

  // Current open conversation for this visitor. Prefer the LIVE signal from
  // the visitors stream (activeConversation) — it always knows the current
  // agent even when history scoping hides another CSR's transcript, and it
  // updates in real time on transfer / start / accept without a reload.
  const liveOpen = live?.activeConversation ?? null;
  const scopedOpen = chats?.find((c) => c.status !== 'CLOSED') ?? null;
  const openConv: {
    id: string;
    status: string;
    assignedUserId: string | null;
    agentName: string | null;
  } | null = liveOpen ?? scopedOpen ?? null;
  // Can the current agent actually open this conversation? (Assigned to me,
  // unassigned/queued, my CSR's chat if I'm a Team Lead, or I'm an
  // admin/manager.) Otherwise it's someone else's chat.
  const canOpenConv =
    !openConv ||
    openConv.assignedUserId == null ||
    openConv.assignedUserId === me?.id ||
    (me?.role === 'LEAD' && !!openConv.assignedUserId && csrIds.includes(openConv.assignedUserId)) ||
    me?.role === 'ADMIN' ||
    me?.role === 'MANAGER';
  const busyAgent = openConv && !canOpenConv ? openConv.agentName : null;
  const chatConvId = (canOpenConv ? openConv?.id : undefined) ?? startedId;
  // "Past chats" lists finished conversations only — the ongoing one lives in
  // the Chat tab, so it must not double as history.
  const pastChats = chats?.filter((c) => c.status === 'CLOSED') ?? null;

  const startChatInline = () => {
    if (!v) return;
    const body = starterDraft.trim();
    // Body is optional — Space bar can 'just claim' the visitor without any
    // opener; the agent then types the first message once the chat is open.
    getSocket()?.emit(
      EV.AgentStartChat,
      { websiteId: v.websiteId, visitorId: v.id, body },
      (ack: { conversationId?: string } | undefined) => {
        if (!ack?.conversationId) return;
        setStartedId(ack.conversationId);
        // Also dock it at the bottom so the chat stays reachable from any page,
        // even after this drawer is closed.
        openChatTab(ack.conversationId);
        // Refresh the past-chats list so counts stay honest.
        void api.visitorConversations(visitorId).then(setChats).catch(() => undefined);
      },
    );
    setStarterDraft('');
  };

  // Keyboard shortcut: Space claims the visitor and opens the chat instantly.
  // Only fires while the drawer is open, the visitor has no existing open
  // conversation for us, and focus isn't inside a text field (so typing the
  // opener in the composer is unaffected).
  useEffect(() => {
    if (!v || openConv || startedId) return;
    const onKey = (e: KeyboardEvent) => {
      if (e.key !== ' ') return;
      const target = e.target as HTMLElement | null;
      if (target) {
        const tag = target.tagName;
        if (tag === 'INPUT' || tag === 'TEXTAREA' || target.isContentEditable) return;
      }
      e.preventDefault();
      startChatInline();
    };
    document.addEventListener('keydown', onKey);
    return () => document.removeEventListener('keydown', onKey);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [v?.id, openConv?.id, startedId]);

  const stat = (label: string, value: React.ReactNode) => (
    <div className="vd-stat">
      <span className="vd-stat-value">{value}</span>
      <span className="vd-stat-label">{label}</span>
    </div>
  );

  const infoRow = (label: string, value: React.ReactNode) =>
    value ? (
      <div className="vd-info-row">
        <span className="vd-info-label">{label}</span>
        <span className="vd-info-value">{value}</span>
      </div>
    ) : null;

  // Profile info — shown beside the chat (wide) and under the Profile tab.
  const profileContent = (
    <>
        {/* CRM fields */}
        <section className="vd-section">
          <h4 className="vd-section-title">Contact details</h4>
          <div className="vd-form-grid">
            {(
              [
                ['name', 'Name', 'text'],
                ['email', 'Email', 'email'],
                ['phone', 'Phone', 'tel'],
              ] as const
            ).map(([key, label, type]) => (
              <label className="field" key={key}>
                <span>{label}</span>
                <input
                  type={type}
                  value={form[key]}
                  placeholder={`Add ${label.toLowerCase()}…`}
                  onChange={(e) => {
                    setForm((f) => ({ ...f, [key]: e.target.value }));
                    setDirty(true);
                  }}
                />
              </label>
            ))}
          </div>
          <label className="field">
            <span>Notes (internal)</span>
            <textarea
              rows={3}
              value={form.notes}
              placeholder="Add notes about this visitor — only agents can see these."
              onChange={(e) => {
                setForm((f) => ({ ...f, notes: e.target.value }));
                setDirty(true);
              }}
            />
          </label>
          <div className="vd-form-actions">
            {savedFlash && <span className="vd-saved">✓ Saved</span>}
            <button className="btn btn-primary btn-sm" onClick={save} disabled={!dirty || saving}>
              {saving ? 'Saving…' : 'Save details'}
            </button>
          </div>
        </section>

        {/* Currently / previously served — surfaces the agent handling
            this visitor right now (ongoing OFFERED/ACTIVE chat) OR the last
            agent from any prior conversation. Uses ALL chats (not just
            closed) so a live handoff shows up even before the chat closes.
            Falls back to the live visitorsByWebsite stream when the visitor
            has no persisted conversation yet but is currently being served. */}
        {(() => {
          // Show the agent who has done the MOST work with this visitor
          // (ranked by agent message count on the server). Falls back to
          // any live 'currently serving' agent if the visitor has never
          // received an agent message yet.
          type Served = { name: string; color: string | null; when: string | null; live: boolean };
          const liveAgent = openConv?.agentName ?? null;
          const served: Served | null = lastServed
            ? {
                name: lastServed.name,
                color: lastServed.avatarColor,
                when: lastServed.servedAt,
                live: liveAgent === lastServed.name,
              }
            : liveAgent
              ? { name: liveAgent, color: null, when: null, live: true }
              : null;
          if (!served) return null;
          return (
            <section className="vd-section vd-served-by">
              <div className="vd-served-label">
                {served.live ? 'Currently serving' : 'Most served by'}
              </div>
              <div className="vd-served-agent">
                <span
                  className="avatar avatar-xs"
                  style={{ background: served.color ?? 'var(--accent)' }}
                >
                  {initials(served.name)}
                </span>
                <div className="vd-served-meta">
                  <div className="vd-served-name">{served.name}</div>
                  {served.when && (
                    <div className="vd-served-when">
                      Last spoke {formatDay(served.when)} · {formatTime(served.when)}
                    </div>
                  )}
                </div>
              </div>
            </section>
          );
        })()}

        {/* Session path */}
        <section className="vd-section">
          <h4 className="vd-section-title">
            <IconEye size={14} /> Visitor path
          </h4>
          {profile && profile.sessionPath.length > 0 ? (
            <ol className="vd-path">
              {[...profile.sessionPath].reverse().map((p, i) => (
                <li
                  key={`${p.at}_${i}`}
                  className={classNames('vd-path-item', i === 0 && online && 'current')}
                >
                  <span className="vd-path-time">{formatWhen(p.at)}</span>
                  <div className="vd-path-page">
                    <span className="vd-path-title">{p.title || pageLabel(p.url)}</span>
                    {p.url && (
                      <a
                        className="vd-path-url"
                        href={p.url}
                        target="_blank"
                        rel="noopener noreferrer"
                        title={p.url}
                      >
                        {pageLabel(p.url)}
                      </a>
                    )}
                  </div>
                </li>
              ))}
            </ol>
          ) : (
            <p className="vd-muted">{profile ? 'No page views recorded yet.' : 'Loading…'}</p>
          )}
        </section>

        {/* Technology / source */}
        <section className="vd-section">
          <h4 className="vd-section-title">
            <IconGlobe size={14} /> Technology &amp; source
          </h4>
          <div className="vd-chips">
            {v?.userAgent && <span className="vd-chip">{ua.browser}</span>}
            {v?.userAgent && <span className="vd-chip">{ua.os}</span>}
            {v?.userAgent && <span className="vd-chip">{ua.device}</span>}
          </div>
          {infoRow('IP address', v?.ip)}
          {infoRow(
            'Location',
            v?.city || v?.country
              ? `${flagEmoji(v?.countryCode)} ${[v?.city, v?.country].filter(Boolean).join(', ')}`
              : null,
          )}
          {infoRow('Came from', v?.referrer ? referrerLabel(v.referrer) : 'Direct')}
          {infoRow('User agent', v?.userAgent && <code className="vd-ua">{v.userAgent}</code>)}
        </section>
    </>
  );

  return (
    <div className="vd-backdrop" onClick={onClose}>
      <div className="vd-panel" ref={panelRef} onClick={(e) => e.stopPropagation()}>
        {/* ── Header ── */}
        <div className="vd-head" style={{ background: `linear-gradient(135deg, ${accentColor}22, transparent)` }}>
          <div className="vd-head-main">
            <span className="avatar avatar-lg" style={{ background: avatarGradient(visitorId) }}>
              {initials(name)}
            </span>
            <div className="vd-head-meta">
              <div className="vd-head-name">
                <span className="vd-flag" title={v?.country ?? undefined}>
                  {flagEmoji(v?.countryCode)}
                </span>
                {name}
                <span className={classNames('dot', online ? 'dot-online' : 'dot-offline')} />
              </div>
              <div className="vd-head-sub">
                {online
                  ? `Online · on site ${durationSince(v?.sessionStartedAt)}`
                  : `Last seen ${formatWhen(v?.lastSeenAt) || 'a while ago'}`}
                {(v?.city || v?.country) && (
                  <> · 📍 {[v?.city, v?.country].filter(Boolean).join(', ')}</>
                )}
              </div>
            </div>
          </div>
          <div className="vd-head-actions">
            {(() => {
              // An open conversation? "Message" jumps straight into it (docked window).
              const open = chats?.find((c) => c.status !== 'CLOSED');
              if (open) {
                return (
                  <button
                    className="btn btn-primary btn-sm"
                    onClick={() => onOpenConversation(open.id)}
                  >
                    <IconMessage size={14} /> Message
                  </button>
                );
              }
              return v ? (
                <button className="btn btn-primary btn-sm" onClick={() => onStartChat(v)}>
                  <IconMessage size={14} /> Start chat
                </button>
              ) : null;
            })()}
            {chatConvId && (
              <button
                className="icon-btn"
                title="Minimize to docked chat"
                aria-label="Minimize"
                onClick={() => {
                  openDockedChat(chatConvId);
                  onClose();
                }}
              >
                <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.4" strokeLinecap="round">
                  <line x1="5" y1="12" x2="19" y2="12" />
                </svg>
              </button>
            )}
            <button className="icon-btn" onClick={onClose} aria-label="Close">
              <IconX size={16} />
            </button>
          </div>
        </div>

        {/* ── Stats ── */}
        <div className="vd-stats">
          {stat('Visits', v?.totalVisits ?? 0)}
          {stat('Chats', profile?.stats.chats ?? v?.chats ?? 0)}
          {stat('Pages today', v?.sessionPages ?? 0)}
          {stat(
            'Avg rating',
            profile?.stats.avgRating != null ? `${profile.stats.avgRating} ★` : '—',
          )}
          {stat('First seen', profile ? formatDay(profile.stats.firstSeenAt) : '—')}
        </div>

        {/* ── Tabs ── */}
        <div className="vd-tabs">
          <button
            className={classNames('vd-tab', tab === 'chat' && 'active')}
            onClick={() => setTab('chat')}
          >
            Chat
            {chatConvId && <span className="vd-tab-live" />}
          </button>
          <button
            className={classNames('vd-tab', tab === 'chats' && 'active')}
            onClick={() => setTab('chats')}
          >
            Past chats{pastChats ? ` (${pastChats.length})` : ''}
          </button>
        </div>

        {error && <div className="vd-error">{error}</div>}

        {tab === 'chat' && (
          <div className="vd-chat-split">
          <div className="vd-body-chat">
            {chatConvId ? (
              // Full live chat right here — reply, type-to-join, calls, everything.
              <ChatPane conversationId={chatConvId} showSidebar={false} />
            ) : busyAgent ? (
              // Visitor is already in a live chat with another agent.
              <div className="vd-chat-starter">
                <div className="vd-chat-starter-hint">
                  <span className="vd-chat-starter-emoji">💬</span>
                  <p>
                    {name} is already chatting with <strong>{busyAgent}</strong>.
                    <br />
                    A visitor can only be in one live chat at a time.
                  </p>
                </div>
              </div>
            ) : chats === null ? (
              <p className="vd-muted vd-chat-loading">Loading…</p>
            ) : (
              // No open conversation yet — typing here starts one instantly.
              <div className="vd-chat-starter">
                <div className="vd-chat-starter-hint">
                  <span className="vd-chat-starter-emoji">💬</span>
                  <p>
                    No open conversation with {name}.
                    <br />
                    <strong>Type below to start the chat right here.</strong>
                  </p>
                </div>
                <div className="composer">
                  <textarea
                    className="composer-input"
                    rows={1}
                    autoFocus
                    placeholder="Type a message — or just press Space to start the chat…"
                    value={starterDraft}
                    onChange={(e) => setStarterDraft(e.target.value)}
                    onKeyDown={(e) => {
                      if (e.key === 'Enter' && !e.shiftKey) {
                        e.preventDefault();
                        startChatInline();
                        return;
                      }
                      // Space in an EMPTY composer claims the visitor (no opener
                      // needed). If any text is already typed, Space acts as a
                      // normal word separator so the opener can still contain spaces.
                      if (e.key === ' ' && !starterDraft) {
                        e.preventDefault();
                        startChatInline();
                      }
                    }}
                  />
                  <button
                    className="btn btn-primary btn-send"
                    onClick={startChatInline}
                    title="Send (or press Space to start empty)"
                  >
                    <IconSend size={16} />
                  </button>
                </div>
              </div>
            )}
          </div>
          {/* Profile info lives beside the chat on wide drawers */}
          <aside className="vd-side">{profileContent}</aside>
          </div>
        )}



        {tab === 'chats' && (
          <div className="vd-body">
            {!chats && <p className="vd-muted">Loading…</p>}
            {pastChats && pastChats.length === 0 && <p className="vd-muted">No past conversations yet.</p>}
            {pastChats?.map((c) => {
              // canView === false ⇒ another agent's chat — show the card for
              // context/handoff but don't let the caller expand the transcript.
              const restricted = c.canView === false;
              return (
              <div
                key={c.id}
                className={classNames(
                  'vd-chat card',
                  openChatId === c.id && 'open',
                  restricted && 'restricted',
                )}
              >
                <button
                  className="vd-chat-head"
                  onClick={() => !restricted && void toggleTranscript(c.id)}
                  disabled={restricted}
                  title={restricted ? "Transcript is restricted to this chat's agent" : undefined}
                >
                  <div className="vd-chat-main">
                    <span className={classNames('status-badge', `status-${c.status.toLowerCase()}`)}>
                      {c.status}
                    </span>
                    <span className="vd-chat-when">
                      <IconClock size={12} /> {formatDay(c.createdAt)} {formatTime(c.createdAt)}
                    </span>
                    {c.agentName && <span className="vd-chat-agent">with {c.agentName}</span>}
                    {c.rating != null && <span className="vd-chat-rating">{'★'.repeat(c.rating)}</span>}
                    {restricted && <span className="vd-chat-locked">🔒 Transcript restricted</span>}
                  </div>
                  {c.preview && !restricted && <div className="vd-chat-preview">{c.preview}</div>}
                  <div className="vd-chat-count">{c.messageCount} messages</div>
                </button>
                {openChatId === c.id && (
                  <div className="vd-transcript">
                    {!transcript[c.id] && <p className="vd-muted">Loading transcript…</p>}
                    {transcript[c.id]?.filter((m) => m.kind === 'TEXT' || m.kind === 'SYSTEM')
                      .map((m) => (
                        <div
                          key={m.id}
                          className={classNames(
                            'vd-msg',
                            m.senderType === 'VISITOR' && 'from-visitor',
                            m.senderType === 'SYSTEM' && 'from-system',
                          )}
                        >
                          <span className="vd-msg-who">
                            {m.senderType === 'VISITOR'
                              ? name
                              : m.senderType === 'BOT'
                                ? 'Alex'
                                : m.senderType === 'SYSTEM'
                                  ? ''
                                  : m.sender?.name || 'Agent'}
                          </span>
                          <span className="vd-msg-body">{m.body}</span>
                          <span className="vd-msg-time">{formatTime(m.createdAt)}</span>
                        </div>
                      ))}
                    <div className="vd-transcript-actions">
                      <button className="btn btn-ghost btn-sm" onClick={() => downloadTranscript(c)}>
                        <IconDownload size={14} /> Download .txt
                      </button>
                      <button className="btn btn-ghost btn-sm" onClick={() => onOpenConversation(c.id)}>
                        <IconMessage size={14} /> Open in Inbox
                      </button>
                    </div>
                  </div>
                )}
              </div>
              );
            })}
          </div>
        )}
      </div>
    </div>
  );
}
