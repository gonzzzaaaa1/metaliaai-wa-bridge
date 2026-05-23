/**
 * Metalia AI — WhatsApp Bridge (Baileys)
 * Maintains a persistent WhatsApp Web session.
 * Exposes REST endpoints for the admin panel and Vercel functions.
 *
 * Deploy on Railway (free tier).
 *
 * Required env vars:
 *   WEBHOOK_URL   — Vercel webhook: https://metaliaai.vercel.app/api/bot/webhook
 *   API_SECRET    — any random string to protect /send and /logout
 *   PORT          — set automatically by Railway
 *
 * Optional (for session persistence across redeploys):
 *   VERCEL_AUTH_URL — set to: https://metaliaai.vercel.app/api/bridge-auth
 *   (uses the same WA_BRIDGE_SECRET for auth — no DATABASE_URL needed)
 */

const {
  default: makeWASocket,
  useMultiFileAuthState,
  DisconnectReason,
  fetchLatestBaileysVersion,
  isJidBroadcast,
  makeCacheableSignalKeyStore,
  downloadMediaMessage,
} = require("@whiskeysockets/baileys");
const { Boom } = require("@hapi/boom");
const express  = require("express");
const QRCode   = require("qrcode");
const pino     = require("pino");
const fs       = require("fs");
const path     = require("path");

const app = express();
app.use(express.json({ limit: "12mb" }));

// Max media size to forward (Vercel body limit ~4.5MB → keep binary under ~3MB)
const MAX_MEDIA_BYTES = 3 * 1024 * 1024;
app.use((req, res, next) => {
  res.header("Access-Control-Allow-Origin", "*");
  res.header("Access-Control-Allow-Headers", "Content-Type, x-api-secret");
  next();
});

const PORT         = process.env.PORT        || 3001;
const WEBHOOK_URL  = process.env.WEBHOOK_URL || "";
const API_SECRET   = process.env.API_SECRET  || "changeme";
// Auth persistence via Vercel API (no DATABASE_URL needed)
// Set VERCEL_AUTH_URL = https://metaliaai.vercel.app/api/bridge-auth
const VERCEL_AUTH_URL = (process.env.VERCEL_AUTH_URL || "").replace(/\/$/, "");

if (VERCEL_AUTH_URL) {
  console.log("[bridge] ✅ Auth persistence enabled via Vercel API");
} else {
  console.warn("[bridge] ⚠️  VERCEL_AUTH_URL not set — session lost on redeploy");
  console.warn("[bridge]    Set VERCEL_AUTH_URL=https://metaliaai.vercel.app/api/bridge-auth");
}

// ── Auth state persistence via Vercel ────────────────────────────────────────
const AUTH_DIR = "./auth_info";

function serializeAuthDir() {
  const files = {};
  if (!fs.existsSync(AUTH_DIR)) return files;
  function walk(dir, prefix) {
    for (const name of fs.readdirSync(dir)) {
      const full = path.join(dir, name);
      const rel  = prefix ? `${prefix}/${name}` : name;
      if (fs.statSync(full).isDirectory()) {
        walk(full, rel);
      } else {
        files[rel] = fs.readFileSync(full).toString("base64");
      }
    }
  }
  walk(AUTH_DIR, "");
  return files;
}

function restoreAuthDir(files) {
  if (!files || typeof files !== "object") return;
  if (!fs.existsSync(AUTH_DIR)) fs.mkdirSync(AUTH_DIR, { recursive: true });
  for (const [rel, b64] of Object.entries(files)) {
    const full = path.join(AUTH_DIR, rel);
    fs.mkdirSync(path.dirname(full), { recursive: true });
    fs.writeFileSync(full, Buffer.from(b64, "base64"));
  }
}

async function saveAuthToVercel() {
  if (!VERCEL_AUTH_URL) return;
  try {
    const files = serializeAuthDir();
    if (Object.keys(files).length === 0) return;
    const res = await fetch(VERCEL_AUTH_URL, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "x-bridge-secret": API_SECRET,
      },
      body: JSON.stringify({ files }),
    });
    const data = await res.json().catch(() => ({}));
    if (!res.ok) {
      console.error("[bridge] Auth save error:", res.status, data);
    } else {
      console.log(`[bridge] 💾 Auth saved (${data.saved} files)`);
    }
  } catch (err) {
    console.error("[bridge] Auth save error:", err.message);
  }
}

async function loadAuthFromVercel() {
  if (!VERCEL_AUTH_URL) return;
  try {
    const res = await fetch(`${VERCEL_AUTH_URL}?secret=${encodeURIComponent(API_SECRET)}`, {
      headers: { "x-bridge-secret": API_SECRET },
    });
    const data = await res.json().catch(() => ({}));
    if (!res.ok) {
      console.error("[bridge] Auth load error:", res.status);
      return;
    }
    if (data.found && data.files) {
      restoreAuthDir(data.files);
      console.log(`[bridge] 📂 Auth restored (${Object.keys(data.files).length} files)`);
    } else {
      console.log("[bridge] No stored auth — will show QR");
    }
  } catch (err) {
    console.error("[bridge] Auth load error:", err.message);
  }
}

async function clearAuthFromVercel() {
  if (!VERCEL_AUTH_URL) return;
  try {
    await fetch(VERCEL_AUTH_URL, {
      method: "DELETE",
      headers: { "x-bridge-secret": API_SECRET },
    });
    console.log("[bridge] 🗑️  Auth cleared");
  } catch (err) {
    console.error("[bridge] Auth clear error:", err.message);
  }
}

// ── State ─────────────────────────────────────────────────────────────────────
let currentQR       = null;
let connectionState = "disconnected";
let sock            = null;
let reconnectTimer  = null;
let connectedAt     = 0;

const processedIds   = new Set();
const MAX_MSG_AGE_MS  = 60 * 1000;

// Debug counters — exposed via /debug
const stats = {
  upsertCalls: 0,
  totalMsgs: 0,
  skippedFromMe: 0,
  skippedJid: 0,
  skippedOld: 0,
  skippedEmpty: 0,
  forwarded: 0,
  lastMsgJid: null,
  lastMsgFromMe: null,
  connectedAs: null,   // phone number / JID the bridge is logged in as
  disconnects: 0,
  lastDisconnectCode: null,
};

const logger = pino({ level: "silent" });

// ── Notify Vercel webhook ─────────────────────────────────────────────────────
async function notifyWebhook(payload) {
  if (!WEBHOOK_URL) {
    console.warn("[bridge] ⚠️  WEBHOOK_URL not set — message NOT forwarded");
    return;
  }
  try {
    const res  = await fetch(WEBHOOK_URL, {
      method:  "POST",
      headers: { "Content-Type": "application/json" },
      body:    JSON.stringify(payload),
    });
    const text = await res.text().catch(() => "");
    if (!res.ok) {
      console.error(`[bridge] Webhook error ${res.status}: ${text.slice(0, 200)}`);
    } else {
      console.log(`[bridge] ✅ Webhook OK ${res.status} — ${text.slice(0, 120)}`);
    }
  } catch (err) {
    console.error("[bridge] Webhook notify error:", err.message);
  }
}

// ── Connect ───────────────────────────────────────────────────────────────────
async function connectToWhatsApp() {
  if (reconnectTimer) { clearTimeout(reconnectTimer); reconnectTimer = null; }
  connectionState = "connecting";

  await loadAuthFromVercel();

  const { state, saveCreds } = await useMultiFileAuthState(AUTH_DIR);
  const { version }          = await fetchLatestBaileysVersion();

  sock = makeWASocket({
    version,
    logger,
    printQRInTerminal: true,
    auth: {
      creds: state.creds,
      keys:  makeCacheableSignalKeyStore(state.keys, logger),
    },
    generateHighQualityLinkPreview: false,
  });

  // ── Connection updates ──────────────────────────────────────────────────────
  sock.ev.on("connection.update", async (update) => {
    const { connection, lastDisconnect, qr } = update;

    if (qr) {
      currentQR       = qr;
      connectionState = "qr";
      console.log("[bridge] 📱 QR ready — scan in admin panel");
    }

    if (connection === "close") {
      currentQR       = null;
      connectionState = "disconnected";
      const code      = lastDisconnect?.error instanceof Boom
        ? lastDisconnect.error.output?.statusCode : 0;
      const loggedOut = code === DisconnectReason.loggedOut;
      stats.disconnects++;
      stats.lastDisconnectCode = code;
      console.log(`[bridge] 🔴 Disconnected (code ${code}, loggedOut: ${loggedOut})`);

      notifyWebhook({ type: "DisconnectedCallback", connected: false });

      if (!loggedOut) {
        console.log("[bridge] Reconnecting in 5s...");
        reconnectTimer = setTimeout(connectToWhatsApp, 5000);
      } else {
        console.log("[bridge] Logged out — will show QR on next connect");
        await clearAuthFromVercel();
        if (fs.existsSync(AUTH_DIR)) fs.rmSync(AUTH_DIR, { recursive: true, force: true });
        reconnectTimer = setTimeout(connectToWhatsApp, 2000);
      }
    }

    if (connection === "open") {
      currentQR       = null;
      connectionState = "connected";
      connectedAt     = Date.now();
      processedIds.clear();
      const myJid = sock.user?.id || "unknown";
      console.log(`[bridge] 🟢 Connected as: ${myJid}`);
      stats.connectedAs = myJid;
      notifyWebhook({ type: "ConnectedCallback", connected: true });
      await saveAuthToVercel();
    }
  });

  // ── Save credentials ────────────────────────────────────────────────────────
  sock.ev.on("creds.update", async () => {
    await saveCreds();
    await saveAuthToVercel();
  });

  // ── Incoming messages ───────────────────────────────────────────────────────
  sock.ev.on("messages.upsert", async ({ messages, type }) => {
    stats.upsertCalls++;
    console.log(`[bridge] 📥 messages.upsert type=${type} count=${messages.length}`);

    // Accept both "notify" (real-time) and "append" (history sync).
    // We discriminate using the timestamp filter below — only truly recent
    // messages (< MAX_MSG_AGE_MS) are forwarded regardless of type.
    if (type !== "notify" && type !== "append") return;

    for (const msg of messages) {
      stats.totalMsgs++;
      stats.lastMsgJid = msg.key?.remoteJid || null;
      stats.lastMsgFromMe = msg.key?.fromMe ?? null;
      console.log(`[bridge] 📨 msg jid=${msg.key?.remoteJid} fromMe=${msg.key?.fromMe} type=${type}`);

      try {
        if (msg.key.fromMe) { stats.skippedFromMe++; continue; }
        if (!msg.key.remoteJid) { stats.skippedJid++; continue; }
        if (isJidBroadcast(msg.key.remoteJid)) { stats.skippedJid++; continue; }
        if (msg.key.remoteJid === "status@broadcast") { stats.skippedJid++; continue; }
        if (msg.key.remoteJid.endsWith("@g.us")) { stats.skippedJid++; continue; }
        // Accept @s.whatsapp.net AND @lid (newer WhatsApp JID format for some accounts)
        const jid = msg.key.remoteJid;
        if (!jid.endsWith("@s.whatsapp.net") && !jid.endsWith("@lid")) {
          stats.skippedJid++;
          continue;
        }

        const msgId = msg.key.id;
        if (msgId) {
          if (processedIds.has(msgId)) continue;
          processedIds.add(msgId);
          if (processedIds.size > 1000) {
            processedIds.delete(processedIds.values().next().value);
          }
        }

        // ── Timestamp-based age filter (replaces startup grace + type filter) ──
        // Only forward messages sent within the last MAX_MSG_AGE_MS seconds.
        // This handles both: offline-queue flood AND startup history sync.
        const tsSec = typeof msg.messageTimestamp === "number"
          ? msg.messageTimestamp
          : Number(msg.messageTimestamp?.toNumber?.() ?? msg.messageTimestamp ?? 0);
        if (tsSec > 0 && Date.now() - tsSec * 1000 > MAX_MSG_AGE_MS) {
          stats.skippedOld++;
          console.log(`[bridge] 🕒 Old msg skipped (${Math.round((Date.now() - tsSec * 1000) / 1000)}s, type=${type})`);
          continue;
        }

        // Extract phone (display/storage): strip JID suffix
        const phone      = msg.key.remoteJid.replace(/@[^@]+$/, "");
        // Keep full JID for sending back (critical for @lid accounts)
        const fullJid    = msg.key.remoteJid;
        const m          = msg.message || {};
        const text       =
          m.conversation ||
          m.extendedTextMessage?.text ||
          m.imageMessage?.caption ||
          m.videoMessage?.caption ||
          m.documentMessage?.caption ||
          "";
        const senderName = msg.pushName || "Cliente";

        let media;
        const mediaNode =
          (m.imageMessage    && { node: m.imageMessage,    kind: "image" }) ||
          (m.documentMessage && { node: m.documentMessage, kind: "document" }) ||
          (m.audioMessage    && { node: m.audioMessage,    kind: "audio" }) ||
          (m.videoMessage    && { node: m.videoMessage,    kind: "video" }) ||
          (m.stickerMessage  && { node: m.stickerMessage,  kind: "sticker" }) ||
          null;

        if (mediaNode) {
          const mimetype      = mediaNode.node.mimetype || "application/octet-stream";
          const fileLen       = Number(mediaNode.node.fileLength || 0);
          const geminiReadable = /^(image\/|application\/pdf|audio\/)/i.test(mimetype);
          if (geminiReadable && (fileLen === 0 || fileLen <= MAX_MEDIA_BYTES)) {
            try {
              const buffer = await downloadMediaMessage(
                msg, "buffer", {},
                { logger, reuploadRequest: sock.updateMediaMessage }
              );
              if (buffer && buffer.length <= MAX_MEDIA_BYTES) {
                media = {
                  mimetype,
                  data:     buffer.toString("base64"),
                  filename: mediaNode.node.fileName || undefined,
                  caption:  mediaNode.node.caption  || undefined,
                  kind:     mediaNode.kind,
                };
              } else {
                media = { mimetype, data: "", kind: mediaNode.kind, tooLarge: true };
              }
            } catch (e) {
              console.error("[bridge] media download failed:", e.message);
              media = { mimetype, data: "", kind: mediaNode.kind };
            }
          } else {
            media = { mimetype, data: "", kind: mediaNode.kind, tooLarge: fileLen > MAX_MEDIA_BYTES };
          }
        }

        if (!text && !media) { stats.skippedEmpty++; continue; }

        stats.forwarded++;
        console.log(`[bridge] 📩 ${senderName} (${phone}): "${text || "[" + (media?.kind || "media") + "]"}"`);

        await notifyWebhook({
          type:        "ReceivedCallback",
          phone,           // number only (for display/DB storage)
          jid:       fullJid,  // full JID incl. @lid/@s.whatsapp.net (for sending back)
          fromMe:      false,
          chatName:    senderName,
          senderName,
          senderPhone: phone,
          text:        text ? { message: text } : undefined,
          media:       media && media.data ? media : undefined,
          mediaKind:   media ? media.kind : undefined,
          momment:     Date.now(),
        });
      } catch (err) {
        console.error("[bridge] Message processing error:", err);
      }
    }
  });
}

// ── Middleware: API key protection ────────────────────────────────────────────
function requireSecret(req, res, next) {
  const secret = req.headers["x-api-secret"] || req.query.secret;
  if (secret !== API_SECRET) return res.status(401).json({ error: "Unauthorized" });
  next();
}

// ── Routes ────────────────────────────────────────────────────────────────────

app.get("/health", (req, res) => {
  res.json({
    ok: true,
    status: connectionState,
    webhookConfigured: !!WEBHOOK_URL,
    authPersist: !!VERCEL_AUTH_URL,
  });
});

app.get("/status", (req, res) => {
  res.json({
    status: connectionState,
    connected: connectionState === "connected",
    hasQR: !!currentQR,
  });
});

app.get("/debug", (req, res) => {
  res.json({
    connectionState,
    webhookConfigured: !!WEBHOOK_URL,
    webhookUrl: WEBHOOK_URL
      ? WEBHOOK_URL.replace(/https?:\/\/[^/]+/, "https://<host>")
      : "(not set — WEBHOOK_URL env var missing!)",
    authPersist: !!VERCEL_AUTH_URL,
    processedIdsCount: processedIds.size,
    connectedAt: connectedAt ? new Date(connectedAt).toISOString() : null,
    uptimeSec:   connectedAt ? Math.round((Date.now() - connectedAt) / 1000) : null,
    stats,
  });
});

app.post("/test-webhook", async (req, res) => {
  const phone = req.body?.phone || "5493517000000";
  const text  = req.body?.text  || "Mensaje de prueba desde el bridge";
  console.log(`[bridge] 🧪 Firing test webhook for ${phone}: "${text}"`);
  await notifyWebhook({
    type:        "ReceivedCallback",
    phone,
    fromMe:      false,
    chatName:    "Test Bridge",
    senderName:  "Test Bridge",
    senderPhone: phone,
    text:        { message: text },
    momment:     Date.now(),
  });
  res.json({ ok: true, phone, text, webhookUrl: WEBHOOK_URL || "(not set)" });
});

app.get("/qr", async (req, res) => {
  if (!currentQR) {
    return res.status(404).json({
      error:  "No QR available",
      status: connectionState,
      hint:   connectionState === "connected" ? "Already connected" : "Starting up, try again in a few seconds",
    });
  }
  try {
    const qrDataUrl = await QRCode.toDataURL(currentQR, { width: 300, margin: 2 });
    res.json({ qrcode: qrDataUrl, status: connectionState });
  } catch (err) {
    res.status(500).json({ error: "QR generation failed", detail: String(err) });
  }
});

app.post("/send", requireSecret, async (req, res) => {
  const { phone, message } = req.body;
  if (!phone || !message) return res.status(400).json({ error: "phone and message required" });
  if (connectionState !== "connected" || !sock) {
    return res.status(503).json({ error: "WhatsApp not connected", status: connectionState });
  }
  try {
    const jid  = phone.includes("@") ? phone : `${phone}@s.whatsapp.net`;
    const sent = await sock.sendMessage(jid, { text: message });
    res.json({ ok: true, messageId: sent?.key?.id });
  } catch (err) {
    console.error("[bridge] Send error:", err);
    res.status(500).json({ error: String(err) });
  }
});

app.post("/logout", requireSecret, async (req, res) => {
  try { if (sock) await sock.logout(); } catch { /* ignore */ }
  await clearAuthFromVercel();
  if (fs.existsSync(AUTH_DIR)) fs.rmSync(AUTH_DIR, { recursive: true, force: true });
  res.json({ ok: true, message: "Logged out — scan QR to reconnect" });
  setTimeout(connectToWhatsApp, 2000);
});

// Force fresh QR without needing API_SECRET.
// Clears all saved auth and restarts the process — Railway auto-restarts it.
// Safe: attacker can only force re-scan, not gain access to the account.
app.post("/force-qr", async (req, res) => {
  console.log("[bridge] 🔄 Force-QR — clearing auth and restarting process");
  await clearAuthFromVercel();
  if (fs.existsSync(AUTH_DIR)) fs.rmSync(AUTH_DIR, { recursive: true, force: true });
  res.json({ ok: true, message: "Auth cleared — bridge restarting, scan QR in admin panel in 10s" });
  // Give Railway time to send the response before exiting
  setTimeout(() => { console.log("[bridge] Exiting for clean restart"); process.exit(0); }, 500);
});

// ── Start ─────────────────────────────────────────────────────────────────────
connectToWhatsApp().catch(console.error);

app.listen(PORT, () => {
  console.log(`[bridge] 🚀 Running on port ${PORT}`);
  console.log(`[bridge] Webhook URL: ${WEBHOOK_URL || "(not set)"}`);
  console.log(`[bridge] Auth persist: ${VERCEL_AUTH_URL ? "enabled" : "disabled"}`);
});
