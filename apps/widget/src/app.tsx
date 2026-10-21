// ─── Widget application root ─────────────────────────────────
import { io, type Socket } from 'socket.io-client';
import type { JSX } from 'preact';
import { useCallback, useEffect, useMemo, useRef, useState } from 'preact/hooks';
import {
  API,
  EV,
  MAX_FILE_BYTES,
  WIDGET_NAMESPACE,
  type CallMeta,
  type ChatMessage,
  type ConversationStatus,
  type ConversationSummary,
  type Visitor,
  type WebsiteBranding,
} from '@livechat/shared';
import { CallSession } from './rtc';
import { CallOverlay, IncomingCallCard, type Invite } from './call-ui';
import {
  IconChat,
  IconClip,
  IconClose,
  IconSend,
  RatingCard,
  renderMessages,
  TypingRow,
  type LocalMessage,
} from './messages';
import { initials, lsGet, lsSet, newTempId, onColor, rgba, shade, ssGet, ssSet } from './util';

interface ReadyPayload {
  visitorToken: string;
  visitor: Visitor;
  website: WebsiteBranding;
  conversation?: ConversationSummary | null;
  messages?: ChatMessage[];
  agent?: { name: string; avatarColor: string; avatarUrl?: string | null } | null;
}

interface ConvState {
  id: string;
  status: ConversationStatus;
}

interface ActiveCall {
  session: CallSession;
  meta: CallMeta;
}

const DEFAULT_PRIMARY = '#5865f2';

// Soft two-note "message received" ping (WebAudio, no asset). Best-effort:
// browsers only allow it after the visitor has interacted with the page.
let audioCtx: AudioContext | null = null;
function playPing(): void {
  try {
    const Ctor =
      window.AudioContext ??
      (window as unknown as { webkitAudioContext?: typeof AudioContext }).webkitAudioContext;
    if (!Ctor) return;
    if (!audioCtx) audioCtx = new Ctor();
    if (audioCtx.state === 'suspended') void audioCtx.resume();
    const ac = audioCtx;
    const note = (freq: number, start: number, dur: number, peak: number) => {
      const osc = ac.createOscillator();
      const gain = ac.createGain();
      osc.type = 'sine';
      osc.frequency.value = freq;
      gain.gain.setValueAtTime(0, start);
      gain.gain.linearRampToValueAtTime(peak, start + 0.015);
      gain.gain.exponentialRampToValueAtTime(0.0001, start + dur);
      osc.connect(gain).connect(ac.destination);
      osc.start(start);
      osc.stop(start + dur + 0.02);
    };
    const t = ac.currentTime;
    note(660, t, 0.18, 0.09);
    note(880, t + 0.08, 0.22, 0.07);
  } catch {
    /* audio not available */
  }
}

export function App({ server, widgetKey }: { server: string; widgetKey: string }): JSX.Element | null {
  const notifyFlutter = (event: string) => {
    const bridge = (window as Window & { FlutterChatBridge?: { postMessage?: (value: string) => void } }).FlutterChatBridge;
    bridge?.postMessage?.(event);
  };
  const tokenKey = `livechat:token:${widgetKey}`;
  const infoDismissKey = `livechat:info-dismissed:${widgetKey}`;
  const colorKey = `livechat:color:${widgetKey}`;
  // Recording-consent gate (FSCA-style): the visitor must Accept before they can
  // reply. Remembered per browser so we don't re-ask on every visit/refresh.
  const consentKey = `livechat:consent:${widgetKey}`;

  const [open, setOpen] = useState(false);
  const [connected, setConnected] = useState(false);
  // "Reconnecting…" only after a real outage — short blips (tab wake,
  // server restart, network switch) reconnect silently.
  const [showOffline, setShowOffline] = useState(false);
  useEffect(() => {
    if (connected) {
      setShowOffline(false);
      return;
    }
    const t = window.setTimeout(() => setShowOffline(true), 3000);
    return () => window.clearTimeout(t);
  }, [connected]);
  const [website, setWebsite] = useState<WebsiteBranding | null>(null);
  // Brand colour resolved instantly from cache (and refreshed by a fast HTTP
  // boot fetch below), so the launcher never flashes the default blue while the
  // socket handshake — which can take a few seconds — is still in flight.
  const [brandColor, setBrandColor] = useState(() => lsGet(colorKey) || DEFAULT_PRIMARY);
  // Visitor is on the admin blocklist (IP / country) → hide the widget entirely
  // instead of letting it spin forever on "Reconnecting…".
  const [blocked, setBlocked] = useState(false);
  const [visitor, setVisitor] = useState<Visitor | null>(null);
  const [visitorToken, setVisitorToken] = useState('');
  const [conversation, setConversation] = useState<ConvState | null>(null);
  const [messages, setMessages] = useState<LocalMessage[]>([]);
  const [agent, setAgent] = useState<{
    name: string;
    avatarColor: string;
    avatarUrl?: string | null;
  } | null>(null);
  const [agentTyping, setAgentTyping] = useState(false);
  const [unread, setUnread] = useState(0);
  const [draft, setDraft] = useState('');
  const [uploading, setUploading] = useState(false);
  const [toast, setToast] = useState<string | null>(null);
  const [infoDismissed, setInfoDismissed] = useState(() => lsGet(infoDismissKey) === '1');
  const [invite, setInvite] = useState<Invite | null>(null);
  const [rated, setRated] = useState(false);
  const [feedbackAsk, setFeedbackAsk] = useState(false); // agent requested a rating mid-chat
  const [confirmEnd, setConfirmEnd] = useState(false);
  const [call, setCall] = useState<ActiveCall | null>(null);
  const [, setCallTick] = useState(0);
  // ⋯ menu (Zendesk-style): sound toggle, email transcript, edit contact, end chat
  const [menuOpen, setMenuOpen] = useState(false);
  const soundKey = `livechat:sound:${widgetKey}`;
  const [soundOn, setSoundOn] = useState(() => lsGet(soundKey) !== '0');
  const soundOnRef = useRef(soundOn);
  const toggleSound = () => {
    setSoundOn((s) => {
      soundOnRef.current = !s;
      lsSet(soundKey, !s ? '1' : '0');
      return !s;
    });
  };
  const [forceInfoForm, setForceInfoForm] = useState(false);
  // The visitor has accepted the recording-consent notice (this browser).
  const [consented, setConsented] = useState(() => lsGet(consentKey) === '1');
  // The visitor declined recording — they can still chat, we just won't record.
  const [declined, setDeclined] = useState(false);

  // Fetch branding over a fast HTTP call at boot so the launcher shows the real
  // brand colour right away — within ~100ms on a first visit, and instantly
  // (from cache) on every visit after — instead of waiting on the socket.
  useEffect(() => {
    let cancelled = false;
    fetch(`${server}/api/widget/boot?key=${encodeURIComponent(widgetKey)}`)
      .then((r) => (r.ok ? r.json() : null))
      .then((data: { website?: WebsiteBranding; blocked?: boolean } | null) => {
        if (cancelled || !data) return;
        // Blocked visitor → hide right away, don't even open a socket.
        if (data.blocked) {
          setBlocked(true);
          return;
        }
        const c = data.website?.primaryColor;
        if (c) {
          setBrandColor(c);
          lsSet(colorKey, c);
        }
      })
      .catch(() => {
        /* offline — the socket handshake will still deliver branding */
      });
    return () => {
      cancelled = true;
    };
  }, [server, widgetKey, colorKey]);

  // Keep the cached colour fresh once the authoritative branding arrives.
  useEffect(() => {
    if (website?.primaryColor) lsSet(colorKey, website.primaryColor);
  }, [website, colorKey]);

  const socketRef = useRef<Socket | null>(null);
  const openRef = useRef(open);
  // Visitor explicitly closed the widget this session → agent messages badge
  // the launcher instead of popping the panel open again.
  const dismissKey = `livechat:minimized:${widgetKey}`;
  const dismissedRef = useRef(ssGet(dismissKey) === '1');
  const markDismissed = (v: boolean) => {
    dismissedRef.current = v;
    ssSet(dismissKey, v ? '1' : '0');
  };
  const callRef = useRef<ActiveCall | null>(null);
  const inviteRef = useRef<Invite | null>(null);
  const bodyRef = useRef<HTMLDivElement>(null);
  const taRef = useRef<HTMLTextAreaElement>(null);
  const convRef = useRef<ConvState | null>(null);
  const reportedReads = useRef(new Set<string>());
  const typingTimer = useRef<number | undefined>(undefined);
  const agentTypingTimer = useRef<number | undefined>(undefined);
  const toastTimer = useRef<number | undefined>(undefined);

  openRef.current = open;
  callRef.current = call;
  inviteRef.current = invite;
  convRef.current = conversation;

  const bumpCall = useCallback(() => setCallTick((t) => t + 1), []);

  const showToast = useCallback((text: string) => {
    setToast(text);
    if (toastTimer.current) window.clearTimeout(toastTimer.current);
    toastTimer.current = window.setTimeout(() => setToast(null), 3500);
  }, []);

  const endCall = useCallback((emitLeave: boolean) => {
    const active = callRef.current;
    if (!active) return;
    active.session.leave(emitLeave);
    setCall(null);
  }, []);

  // ── Socket lifecycle ───────────────────────────────────────
  useEffect(() => {
    const socket = io(`${server}${WIDGET_NAMESPACE}`, {
      auth: (cb) =>
        cb({
          widgetKey,
          visitorToken: lsGet(tokenKey) || undefined,
          page: window.location.href,
          pageTitle: document.title || undefined,
          referrer: document.referrer || undefined,
        }),
      // Try WebSocket first (one round-trip) instead of polling → upgrade, so the
      // chat connects and the greeting appears faster; polling stays as fallback.
      transports: ['websocket', 'polling'],
      reconnectionDelayMax: 8000,
    });
    socketRef.current = socket;

    socket.on('connect', () => setConnected(true));
    socket.on('disconnect', () => setConnected(false));

    // Blocklisted visitor (IP / country): the server rejects the handshake with
    // "Access blocked". Stop retrying and hide the widget rather than looping on
    // "Reconnecting…". (The boot fetch usually catches this first and faster.)
    socket.on('connect_error', (err: Error) => {
      if (/access blocked|blocked/i.test(err?.message ?? '')) {
        socket.io.opts.reconnection = false;
        socket.disconnect();
        setBlocked(true);
      }
    });

    socket.on(EV.WidgetReady, (payload: ReadyPayload) => {
      lsSet(tokenKey, payload.visitorToken);
      setVisitorToken(payload.visitorToken);
      setWebsite(payload.website);
      setVisitor(payload.visitor);
      setAgent(payload.agent ?? null);
      setConversation(
        payload.conversation ? { id: payload.conversation.id, status: payload.conversation.status } : null,
      );
      setMessages((payload.messages ?? []) as LocalMessage[]);
      setConnected(true);

      // First-time visitor on this browser → pop the chat open once with the
      // greeting, so they see "How can we help?" without clicking the bubble.
      // Skips repeat visits and anyone who minimized it earlier this session.
      const seenKey = `livechat:greeted:${widgetKey}`;
      const noHistory = (payload.messages ?? []).length === 0;
      if (noHistory && !lsGet(seenKey) && !dismissedRef.current) {
        lsSet(seenKey, '1');
        window.setTimeout(() => setOpen(true), 700);
      }
    });

    socket.on(EV.ChatMessage, ({ message }: { message: ChatMessage }) => {
      setMessages((prev) => {
        if (message.tempId) {
          const i = prev.findIndex((m) => m.pending && m.tempId === message.tempId);
          if (i >= 0) {
            const next = prev.slice();
            next[i] = message;
            return next;
          }
        }
        if (prev.some((m) => m.id === message.id)) {
          return prev.map((m) => (m.id === message.id ? { ...m, ...message } : m));
        }
        return [...prev, message];
      });
      setConversation((prev) => prev ?? { id: message.conversationId, status: 'WAITING' });
      if ((message.senderType === 'AGENT' || message.senderType === 'BOT') && soundOnRef.current) {
        playPing();
      }
      if (message.senderType === 'AGENT' || message.senderType === 'BOT') {
        notifyFlutter('CHAT_MESSAGE_RECEIVED');
        setUnread((u) => u + 1);
      }
      if ((message.senderType === 'AGENT' || message.senderType === 'BOT') && !openRef.current) {
        // Auto-open on an agent message — unless the visitor closed the
        // widget themselves, then just keep counting on the launcher badge.
        if (dismissedRef.current) setUnread((u) => u + 1);
        else setOpen(true);
      }
    });

    socket.on(
      EV.ChatReceipt,
      ({ messageIds, deliveredAt, readAt }: { conversationId: string; messageIds: string[]; deliveredAt?: string; readAt?: string }) => {
        if (!Array.isArray(messageIds) || messageIds.length === 0) return;
        const ids = new Set(messageIds);
        setMessages((prev) =>
          prev.map((m) =>
            ids.has(m.id)
              ? { ...m, deliveredAt: deliveredAt ?? m.deliveredAt, readAt: readAt ?? m.readAt }
              : m,
          ),
        );
      },
    );

    // Auto-translate: the agent's reply arrives in English, then a moment later
    // its translation into the visitor's language patches the bubble in place.
    socket.on(
      EV.ChatTranslation,
      ({ messageId, translatedBody, origLang }: { conversationId: string; messageId: string; translatedBody: string; origLang: string }) => {
        setMessages((prev) =>
          prev.map((m) => (m.id === messageId ? { ...m, translatedBody, origLang } : m)),
        );
      },
    );

    socket.on(EV.ChatTyping, ({ from, typing }: { conversationId: string; from: 'VISITOR' | 'AGENT'; typing: boolean }) => {
      if (from !== 'AGENT') return;
      setAgentTyping(typing);
      if (agentTypingTimer.current) window.clearTimeout(agentTypingTimer.current);
      if (typing) agentTypingTimer.current = window.setTimeout(() => setAgentTyping(false), 5000);
    });

    socket.on(EV.ChatStatus, ({ conversation: conv }: { conversation: ConversationSummary }) => {
      setConversation((prev) => {
        if (prev && prev.id !== conv.id && prev.status !== 'CLOSED') return prev;
        return { id: conv.id, status: conv.status };
      });
      if (conv.status === 'CLOSED') setAgentTyping(false);
    });

    socket.on(EV.ChatAgent, ({ agent: a }: { conversationId: string; agent: { name: string; avatarColor: string; avatarUrl?: string | null } | null }) => {
      setAgent(a ?? null);
    });

    socket.on(EV.CallInvite, ({ call: meta, from }: { call: CallMeta; from?: { name: string } }) => {
      if (callRef.current) return; // already in a call
      setInvite({ call: meta, from: from ?? null });
      notifyFlutter('CALL_RINGING');
    });

    socket.on(EV.CallStatus, ({ call: meta }: { call: CallMeta }) => {
      const pendingInvite = inviteRef.current;
      if (pendingInvite && pendingInvite.call.id === meta.id && meta.status !== 'INVITED') {
        setInvite(null);
      }
      const active = callRef.current;
      if (active && active.meta.id === meta.id) {
        if (meta.status === 'ENDED' || meta.status === 'DECLINED') {
          notifyFlutter('CALL_ENDED');
          active.session.leave(false);
          setCall(null);
        } else {
          setCall({ session: active.session, meta });
        }
      }
      // Keep CALL message cards in the transcript in sync (Ringing… → Call ended).
      setMessages((prev) =>
        prev.map((m) => (m.callId === meta.id ? { ...m, call: meta } : m)),
      );
    });

    socket.on(EV.AppError, ({ message }: { message: string }) => {
      // Deferred country-block check booted us → hide the widget entirely.
      if (/access blocked|blocked/i.test(message ?? '')) {
        socket.io.opts.reconnection = false;
        socket.disconnect();
        setBlocked(true);
        return;
      }
      if (message) showToast(message);
    });

    // Agent asked for a rating — show the stars right in the chat.
    socket.on(EV.ChatFeedbackRequest, () => {
      setFeedbackAsk(true);
      if (!dismissedRef.current) setOpen(true);
    });

    // Busy fallback: the team hasn't replied, the server asks us to collect
    // contact details — open the form only if email or phone is still missing.
    socket.on(EV.ChatRequestInfo, () => {
      setVisitor((v) => {
        if (!v || !v.email || !v.phone) {
          setInfoDismissed(false);
          setForceInfoForm(true);
          if (!dismissedRef.current) setOpen(true);
        }
        return v;
      });
    });

    // Closing the window/browser doesn't always flush the websocket close
    // frame — without this the server only notices at ping-timeout (30s+).
    // pagehide fires on close AND navigation: the beacon tells the server to
    // drop our sockets now; a navigating tab simply reconnects within the
    // presence grace, so nothing flickers.
    const onPageHide = () => {
      const token = lsGet(tokenKey);
      if (token && navigator.sendBeacon) {
        try {
          navigator.sendBeacon(`${server}/api/widget/bye`, JSON.stringify({ token }));
        } catch {
          /* best effort */
        }
      }
      socket.disconnect();
    };
    window.addEventListener('pagehide', onPageHide);

    // ── Idle detection (Zendesk-style) ──
    // Backgrounded tab (30 min) or no mouse/key/scroll/touch for 30 min →
    // report idle: the socket stays connected (messages still arrive) but the
    // visitor drops out of the agents' "Online now" list. Any activity or
    // refocusing the tab flips them back to online instantly — no reload.
    const IDLE_AFTER_MS = 30 * 60 * 1000;
    const HIDDEN_AFTER_MS = 30 * 60 * 1000;
    let lastActivity = Date.now();
    let reportedActive = true;
    let hiddenTimer: number | null = null;
    const report = (active: boolean) => {
      if (reportedActive === active) return;
      reportedActive = active;
      socket.emit(EV.WidgetActivity, { active });
    };
    const onActivity = () => {
      lastActivity = Date.now();
      if (!document.hidden) report(true);
    };
    const onVisibility = () => {
      if (hiddenTimer != null) {
        clearTimeout(hiddenTimer);
        hiddenTimer = null;
      }
      if (document.hidden) {
        hiddenTimer = window.setTimeout(() => report(false), HIDDEN_AFTER_MS);
      } else {
        lastActivity = Date.now();
        report(true);
      }
    };
    const idleCheck = window.setInterval(() => {
      if (!document.hidden && Date.now() - lastActivity > IDLE_AFTER_MS) report(false);
    }, 60_000);
    const activityEvents = ['pointerdown', 'pointermove', 'keydown', 'scroll', 'touchstart'] as const;
    for (const ev of activityEvents) window.addEventListener(ev, onActivity, { passive: true });
    document.addEventListener('visibilitychange', onVisibility);
    // A fresh socket is "active" server-side. Re-sync our real state WITHOUT
    // resetting the idle clock — otherwise a left-open tab that quietly
    // reconnects every so often would keep looking active forever. If we're
    // actually idle/hidden, tell the server again so we stay off "Online now".
    socket.on('connect', () => {
      reportedActive = true;
      if (document.hidden || Date.now() - lastActivity > IDLE_AFTER_MS) report(false);
    });

    return () => {
      window.removeEventListener('pagehide', onPageHide);
      for (const ev of activityEvents) window.removeEventListener(ev, onActivity);
      document.removeEventListener('visibilitychange', onVisibility);
      window.clearInterval(idleCheck);
      if (hiddenTimer != null) clearTimeout(hiddenTimer);
      callRef.current?.session.leave(false);
      socket.removeAllListeners();
      socket.disconnect();
      socketRef.current = null;
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [server, widgetKey]);

  // ── Read receipts + unread reset when panel is open ────────
  useEffect(() => {
    if (!open) return;
    setUnread(0);
    const ids = messages
      .filter((m) => m.senderType === 'AGENT' && !m.pending && !m.readAt && !reportedReads.current.has(m.id))
      .map((m) => m.id);
    if (ids.length > 0) {
      for (const id of ids) reportedReads.current.add(id);
      socketRef.current?.emit(EV.WidgetRead, { messageIds: ids });
    }
  }, [open, messages]);

  // ── Auto-scroll message list ───────────────────────────────
  useEffect(() => {
    const el = bodyRef.current;
    if (el) el.scrollTop = el.scrollHeight;
  }, [messages, agentTyping, open, conversation?.status]);

  // ── Composer ───────────────────────────────────────────────
  const emitTyping = useCallback((typing: boolean) => {
    socketRef.current?.emit(EV.WidgetTyping, { typing });
  }, []);

  const onDraftInput = (e: Event) => {
    const el = e.currentTarget as HTMLTextAreaElement;
    setDraft(el.value);
    el.style.height = 'auto';
    el.style.height = `${Math.min(el.scrollHeight, 110)}px`;
    emitTyping(true);
    if (typingTimer.current) window.clearTimeout(typingTimer.current);
    typingTimer.current = window.setTimeout(() => emitTyping(false), 1400);
  };

  const send = useCallback(() => {
    const body = draft.trim();
    const socket = socketRef.current;
    if (!body || !socket) return;
    // Sending a message counts as continuing → implicit consent.
    if (!consented) {
      lsSet(consentKey, '1');
      setConsented(true);
    }
    const tempId = newTempId();
    socket.emit(EV.WidgetMessage, { body, tempId });
    setMessages((prev) => [
      ...prev,
      {
        id: tempId,
        tempId,
        conversationId: convRef.current?.id ?? '',
        senderType: 'VISITOR',
        senderUserId: null,
        body,
        kind: 'TEXT',
        fileId: null,
        callId: null,
        createdAt: new Date().toISOString(),
        deliveredAt: null,
        readAt: null,
        pending: true,
      },
    ]);
    setDraft('');
    if (typingTimer.current) window.clearTimeout(typingTimer.current);
    emitTyping(false);
    const ta = taRef.current;
    if (ta) {
      ta.value = '';
      ta.style.height = 'auto';
      ta.focus();
    }
  }, [draft, emitTyping, consented, consentKey]);

  const onKeyDown = (e: KeyboardEvent) => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      send();
    }
  };

  // ── File upload (picker or drag & drop) ────────────────────
  const uploadFile = async (file: File) => {
    const conv = convRef.current;
    if (!file || !conv) return;
    if (file.size > MAX_FILE_BYTES) {
      showToast(`File is too large (max ${Math.round(MAX_FILE_BYTES / (1024 * 1024))} MB)`);
      return;
    }
    const token = lsGet(tokenKey) ?? visitorToken;
    setUploading(true);
    try {
      const fd = new FormData();
      fd.append('file', file);
      fd.append('conversationId', conv.id);
      fd.append('token', token);
      const url = `${server}${API.uploads}?conversationId=${encodeURIComponent(conv.id)}&token=${encodeURIComponent(token)}`;
      const res = await fetch(url, { method: 'POST', body: fd });
      if (!res.ok) {
        let msg = 'Upload failed';
        try {
          const json = (await res.json()) as { error?: string };
          if (json && typeof json.error === 'string') msg = json.error;
        } catch {
          /* non-JSON error body */
        }
        showToast(msg);
      }
    } catch {
      showToast('Upload failed — check your connection');
    } finally {
      setUploading(false);
    }
  };

  // Upload several files in order (picker multi-select or a multi-file drop).
  const uploadFiles = async (files: FileList | File[]) => {
    for (const f of Array.from(files)) await uploadFile(f);
  };

  const onPickFile = (e: Event) => {
    const input = e.currentTarget as HTMLInputElement;
    const files = input.files;
    input.value = '';
    if (files && files.length > 0) void uploadFiles(files);
  };

  const [dragOver, setDragOver] = useState(false);

  // ── Info mini-form ─────────────────────────────────────────
  const submitInfo = (name: string, email: string, phone: string) => {
    const payload: { name?: string; email?: string; phone?: string } = {};
    if (name.trim()) payload.name = name.trim();
    if (email.trim()) payload.email = email.trim();
    if (phone.trim()) payload.phone = phone.trim();
    if (payload.name || payload.email || payload.phone) {
      socketRef.current?.emit(EV.WidgetInfo, payload);
      setVisitor((v) =>
        v
          ? {
              ...v,
              name: payload.name ?? v.name,
              email: payload.email ?? v.email,
              phone: payload.phone ?? v.phone,
            }
          : v,
      );
    }
    lsSet(infoDismissKey, '1');
    setInfoDismissed(true);
    setForceInfoForm(false);
  };

  // ── ⋯ menu actions ─────────────────────────────────────────
  const emailTranscript = () => {
    setMenuOpen(false);
    const socket = socketRef.current;
    if (!socket) return;
    socket.emit(
      EV.WidgetEmailTranscript,
      {},
      (ack: { ok?: boolean; email?: string; error?: string } | undefined) => {
        if (ack?.ok) {
          showToast(`Transcript sent to ${ack.email ?? 'your email'} 💌`);
        } else if (ack?.error === 'no-email') {
          // Ask for an email first, then they can request again.
          setInfoDismissed(false);
          setForceInfoForm(true);
          showToast('Add your email so we can send the transcript.');
        } else {
          showToast(ack?.error ?? 'Could not send the transcript right now.');
        }
      },
    );
  };

  const editContact = () => {
    setMenuOpen(false);
    setInfoDismissed(false);
    setForceInfoForm(true);
  };

  const dismissInfo = () => {
    lsSet(infoDismissKey, '1');
    setInfoDismissed(true);
    setForceInfoForm(false);
  };

  // ── Start new conversation after close ─────────────────────
  const startNewConversation = () => {
    setConversation(null);
    setMessages([]);
    setAgent(null);
    setAgentTyping(false);
    setRated(false);
    setFeedbackAsk(false);
    reportedReads.current.clear();
    const socket = socketRef.current;
    if (socket) {
      socket.disconnect();
      socket.connect();
    }
    taRef.current?.focus();
  };

  // ── Recording consent ──────────────────────────────────────
  // Accept → remember it and let the visitor type. Decline → dismiss the notice
  // and show a short line saying we won't record; the visitor can still chat.
  const acceptConsent = () => {
    lsSet(consentKey, '1');
    setConsented(true);
    setDeclined(false);
    window.setTimeout(() => taRef.current?.focus(), 0);
  };
  const declineConsent = () => {
    setDeclined(true);
    window.setTimeout(() => taRef.current?.focus(), 0);
  };

  // ── CSAT rating after close ────────────────────────────────
  const submitRating = (rating: number, comment: string) => {
    socketRef.current?.emit(EV.WidgetRate, { rating, comment: comment || undefined });
    setRated(true);
  };

  // ── Calls ──────────────────────────────────────────────────
  const acceptCall = async (inv: Invite) => {
    const socket = socketRef.current;
    if (!socket) return;
    const session = new CallSession(socket, inv.call.id, inv.call.kind, bumpCall);
    try {
      await session.init();
    } catch {
      socket.emit(EV.WidgetCallDecline, { callId: inv.call.id });
      setInvite(null);
      showToast(
        inv.call.kind === 'VIDEO'
          ? 'Camera/microphone access is required for video calls'
          : 'Microphone access is required for audio calls',
      );
      return;
    }
    socket.emit(EV.WidgetCallAccept, { callId: inv.call.id });
    session.join();
    notifyFlutter('CALL_STARTED');
    setCall({ session, meta: { ...inv.call, status: 'ACTIVE' } });
    setInvite(null);
  };

  const declineCall = (inv: Invite) => {
    socketRef.current?.emit(EV.WidgetCallDecline, { callId: inv.call.id });
    notifyFlutter('CALL_ENDED');
    setInvite(null);
  };

  // ── Derived UI state ───────────────────────────────────────
  const primary = website?.primaryColor || brandColor;
  const header = website?.headerColor || primary;
  const cssVars = useMemo(
    () =>
      `--lc-primary:${primary};` +
      `--lc-primary-dark:${shade(primary, -0.22)};` +
      `--lc-on-primary:${onColor(primary)};` +
      `--lc-header:${header};` +
      `--lc-header-dark:${shade(header, -0.18)};` +
      `--lc-on-header:${onColor(header)};` +
      `--lc-soft:${rgba(primary, 0.1)};` +
      `--lc-glow:${rgba(header, 0.38)}`,
    [primary, header],
  );

  // The launcher bubble renders instantly on page load (no waiting for the
  // socket handshake) so the customer sees it the moment they arrive. Only the
  // open panel's content needs the branding/history from WidgetReady.
  const status = conversation?.status ?? null;
  const ended = status === 'CLOSED';
  // The contact form no longer opens on its own when the visitor arrives — it
  // only appears when they choose "Edit contact details" from the menu (which
  // sets forceInfoForm), so the chat isn't blocked by a form up front.
  const showInfoForm = forceInfoForm;
  const selfLabel = visitor?.name || 'You';

  // Blocked visitor → render nothing at all (no bubble, no "Reconnecting…").
  if (blocked) return null;

  return (
    <div class="lc-root" style={cssVars}>
      {open && !website && (
        <div class="lc-panel lc-panel-loading">
          <div class="lc-header">
            <div class="lc-head-info">
              <div class="lc-title">Chat</div>
              <div class="lc-subtitle">Connecting…</div>
            </div>
          </div>
          <div class="lc-body lc-body-loading">
            <span class="lc-spinner" />
          </div>
        </div>
      )}
      {open && website && (
        <div
          class="lc-panel"
          onDragOver={(e) => {
            if (!conversation || ended) return;
            e.preventDefault();
            if (!dragOver) setDragOver(true);
          }}
          onDragLeave={(e) => {
            if (e.currentTarget === e.target) setDragOver(false);
          }}
          onDrop={(e) => {
            e.preventDefault();
            setDragOver(false);
            const files = (e as unknown as DragEvent).dataTransfer?.files;
            if (files && files.length > 0 && conversation && !ended) void uploadFiles(files);
          }}
        >
          {dragOver && (
            <div class="lc-drop">
              <IconClip />
              <span>Drop to send</span>
            </div>
          )}
          {/* Header */}
          <div class="lc-header">
            <div class="lc-logo">
              {website.logoUrl ? <img src={website.logoUrl} alt="" /> : <IconChat size={20} />}
            </div>
            <div class="lc-head-info">
              <div class="lc-title">{website.name}</div>
              {agent ? (
                <div class="lc-agent-chip">
                  {agent.avatarUrl ? (
                    <img
                      class="lc-avatar-dot lc-avatar-img"
                      src={/^https?:/i.test(agent.avatarUrl) ? agent.avatarUrl : `${server}${agent.avatarUrl}`}
                      alt=""
                    />
                  ) : (
                    <span class="lc-avatar-dot" style={`background:${agent.avatarColor}`}>
                      {initials(agent.name)}
                    </span>
                  )}
                  <span class="lc-agent-name">{agent.name}</span>
                  <span class="lc-online-dot" />
                </div>
              ) : (
                <div class="lc-subtitle">We're here to help</div>
              )}
            </div>
            <div class="lc-menu-wrap">
              <button
                type="button"
                class="lc-iconbtn"
                aria-label="More options"
                onClick={() => setMenuOpen((m) => !m)}
              >
                <svg width="18" height="18" viewBox="0 0 24 24" fill="currentColor">
                  <circle cx="5" cy="12" r="1.9" />
                  <circle cx="12" cy="12" r="1.9" />
                  <circle cx="19" cy="12" r="1.9" />
                </svg>
              </button>
              {menuOpen && (
                <>
                  <div class="lc-menu-scrim" onClick={() => setMenuOpen(false)} />
                  <div class="lc-menu" role="menu">
                    <button type="button" class="lc-menu-item" onClick={toggleSound}>
                      <span>Sound</span>
                      {soundOn ? (
                        <svg width="17" height="17" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                          <polygon points="11 5 6 9 2 9 2 15 6 15 11 19 11 5" />
                          <path d="M15.5 8.5a5 5 0 0 1 0 7" />
                          <path d="M18.5 5.5a9 9 0 0 1 0 13" />
                        </svg>
                      ) : (
                        <svg width="17" height="17" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                          <polygon points="11 5 6 9 2 9 2 15 6 15 11 19 11 5" />
                          <line x1="22" y1="9" x2="16" y2="15" />
                          <line x1="16" y1="9" x2="22" y2="15" />
                        </svg>
                      )}
                    </button>
                    <button type="button" class="lc-menu-item" onClick={emailTranscript}>
                      Email transcript
                    </button>
                    <button type="button" class="lc-menu-item" onClick={editContact}>
                      Submit contact details
                    </button>
                    <button
                      type="button"
                      class="lc-menu-item lc-menu-danger"
                      disabled={!conversation || ended}
                      onClick={() => {
                        setMenuOpen(false);
                        setConfirmEnd(true);
                      }}
                    >
                      End chat
                    </button>
                  </div>
                </>
              )}
            </div>
            <button
              type="button"
              class="lc-iconbtn"
              aria-label="Minimize"
              onClick={() => {
                markDismissed(true);
                setOpen(false);
              }}
            >
              <IconClose />
            </button>
          </div>

          {/* End-chat confirmation (inline, no browser dialog) */}
          {confirmEnd && (
            <div class="lc-confirm">
              <span>End this chat?</span>
              <button
                type="button"
                class="lc-confirm-yes"
                onClick={() => {
                  socketRef.current?.emit(EV.WidgetEndChat, {});
                  setConfirmEnd(false);
                }}
              >
                End chat
              </button>
              <button type="button" class="lc-confirm-no" onClick={() => setConfirmEnd(false)}>
                Cancel
              </button>
            </div>
          )}

          {/* Status strips */}
          {showOffline ? (
            <div class="lc-strip lc-strip-warn">
              <span class="lc-spin" /> Reconnecting…
            </div>
          ) : status === 'WAITING' ? (
            <div class="lc-strip lc-strip-queue">
              <span class="lc-radar"><span /><span /><span /></span>
              <span>
                You are in queue
                <span class="lc-dots"><i>.</i><i>.</i><i>.</i></span>
              </span>
            </div>
          ) : null}

          {/* Messages */}
          <div class="lc-body" ref={bodyRef}>
            {messages.length === 0 && website.greeting && <div class="lc-greet">{website.greeting}</div>}
            {renderMessages(messages, { server, token: visitorToken, fallbackAgent: agent })}
            {agentTyping && !ended && <TypingRow agent={agent} server={server} />}
            {showInfoForm && (
              <InfoForm
                onSubmit={submitInfo}
                onDismiss={dismissInfo}
                initialName={visitor?.name ?? ''}
                initialEmail={visitor?.email ?? ''}
                initialPhone={visitor?.phone ?? ''}
                editing={forceInfoForm}
              />
            )}
          </div>

          {/* Composer / closed bar */}
          {ended ? (
            <div class="lc-closedbar">
              {status === 'CLOSED' && !rated ? (
                <RatingCard onSubmit={submitRating} />
              ) : (
                <span>
                  {rated ? 'Thank you for your feedback! 💚' : 'This conversation has ended.'}
                </span>
              )}
              <button type="button" class="lc-btn" onClick={startNewConversation}>
                Start new conversation
              </button>
            </div>
          ) : (
            <>
            {feedbackAsk && !rated && (
              <div class="lc-closedbar lc-feedback-ask">
                <RatingCard onSubmit={submitRating} />
              </div>
            )}
            {feedbackAsk && rated && (
              <div class="lc-closedbar lc-feedback-ask">
                <span>Thank you for your feedback! 💚</span>
              </div>
            )}
            {!consented && declined && (
              <div class="lc-consent lc-consent-declined">
                <div class="lc-consent-text">
                  Thanks — we won’t record this chat. You can still send us a message below.
                </div>
              </div>
            )}
            {!consented && !declined && (
              <div class="lc-consent">
                <div class="lc-consent-text">
                  By continuing, you consent to this chat being recorded and handled in
                  accordance with {website.name}'s privacy policy.
                </div>
                <div class="lc-consent-actions">
                  <button type="button" class="lc-consent-accept" onClick={acceptConsent}>
                    Accept
                  </button>
                  <button type="button" class="lc-consent-decline" onClick={declineConsent}>
                    Decline
                  </button>
                </div>
              </div>
            )}
            <div class="lc-composer">
              <button
                type="button"
                class="lc-attach"
                title={conversation ? 'Attach a file' : 'Send a message first to attach files'}
                disabled={!conversation || uploading || !connected}
                onClick={() => {
                  const picker = document.createElement('input');
                  picker.type = 'file';
                  picker.multiple = true;
                  picker.onchange = (ev) => void onPickFile(ev);
                  picker.click();
                }}
              >
                {uploading ? <span class="lc-spin" /> : <IconClip />}
              </button>
              <textarea
                ref={taRef}
                class="lc-ta"
                rows={1}
                placeholder={'Type your message…'}
                value={draft}
                onInput={onDraftInput}
                onKeyDown={onKeyDown}
                disabled={!connected}
              />
              <button
                type="button"
                class="lc-send"
                title="Send"
                disabled={!draft.trim() || !connected}
                onClick={send}
              >
                <IconSend />
              </button>
            </div>
            </>
          )}

          {toast && <div class="lc-toast">{toast}</div>}
        </div>
      )}

      {/* Incoming call card — floats above everything, shown even when panel is closed */}
      {invite && !call && (
        <IncomingCallCard invite={invite} onAccept={() => void acceptCall(invite)} onDecline={() => declineCall(invite)} />
      )}

      {/* In-call overlay — independent of the panel so the call survives panel close */}
      {call && (
        <CallOverlay
          session={call.session}
          meta={call.meta}
          selfLabel={selfLabel}
          brandColor={primary}
          onHangup={() => endCall(true)}
        />
      )}

      {/* Launcher — only once the connection is live (branding loaded), so it
          never flashes a wrong colour or a dead "Reconnecting…" bubble. */}
      {website && (
        <button
          type="button"
          class={`lc-launcher ${unread > 0 ? 'lc-pulse' : ''}`}
          title={open ? 'Close chat' : `Chat with ${website?.name ?? 'us'}`}
          onClick={() => {
            const next = !openRef.current;
            markDismissed(!next);
            setOpen(next);
          }}
        >
          {open ? <IconClose /> : <IconChat />}
          {!open && unread > 0 && <span class="lc-badge">{unread > 9 ? '9+' : unread}</span>}
        </button>
      )}
    </div>
  );
}

// ─── Pre-chat name/email mini-form ───────────────────────────

function InfoForm({
  onSubmit,
  onDismiss,
  initialName = '',
  initialEmail = '',
  initialPhone = '',
  editing = false,
}: {
  onSubmit: (name: string, email: string, phone: string) => void;
  onDismiss: () => void;
  initialName?: string;
  initialEmail?: string;
  initialPhone?: string;
  editing?: boolean;
}): JSX.Element {
  const [name, setName] = useState(initialName);
  const [email, setEmail] = useState(initialEmail);
  const [phone, setPhone] = useState(initialPhone);
  const submit = (e: Event) => {
    e.preventDefault();
    onSubmit(name, email, phone);
  };
  return (
    <form class="lc-form" onSubmit={submit}>
      <div class="lc-form-title">{editing ? 'Your contact details' : 'Introduce yourself'}</div>
      <div class="lc-form-sub">
        {editing ? 'Update your details below.' : 'So we can follow up if you step away.'}
      </div>
      <input
        class="lc-input"
        type="text"
        placeholder="Your name"
        value={name}
        onInput={(e) => setName((e.currentTarget as HTMLInputElement).value)}
      />
      <input
        class="lc-input"
        type="email"
        placeholder="Email address"
        value={email}
        onInput={(e) => setEmail((e.currentTarget as HTMLInputElement).value)}
      />
      <input
        class="lc-input"
        type="tel"
        placeholder="Phone number"
        value={phone}
        onInput={(e) => setPhone((e.currentTarget as HTMLInputElement).value)}
      />
      <div class="lc-form-row">
        <button type="submit" class="lc-btn" disabled={!name.trim() && !email.trim() && !phone.trim()}>
          Save
        </button>
        <button type="button" class="lc-btn lc-btn-ghost" onClick={onDismiss}>
          {editing ? 'Cancel' : 'No thanks'}
        </button>
      </div>
    </form>
  );
}
