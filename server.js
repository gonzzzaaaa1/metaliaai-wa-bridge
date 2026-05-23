/**
 * Metalia AI — WhatsApp Bridge (Baileys)
 * Maintains a persistent WhatsApp Web session.
 * Exposes REST endpoints for the admin panel and Vercel functions.
 *
 * Deploy on Railway (free tier).
 *
 * Required env vars:
 *   WEBHOOK_URL   — your Vercel webhook: https://metaliaai.vercel.app/api/bot/webhook
 *   API_SECRET    — any random string to protect /send endpoint
 *   PORT          — set automatically by Railway
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
const express = require("express");
const QRCode = require("qrcode");
const pino = require("pino");

const app = express();
app.use(express.json({ limit: "12mb" }));

// Max media size to forward to the webhook (Vercel body limit ~4.5MB → keep binary under ~3MB)
const MAX_MEDIA_BYTES = 3 * 1024 * 1024;
app.use((req, res, next) => {
  res.header("Access-Control-Allow-Origin", "*");
  res.header("Access-Control-Allow-Headers", "Content-Type, x-api-secret");
  next();
});

const PORT        = process.env.PORT || 3001;
const WEBHOOK_URL = process.env.WEBHOOK_URL || "";
const API_SECRET  = process.env.API_SECRET || "changeme";

// ── State ─────────────────────────────────────────────────────────────────────
let currentQR        = null;  // raw QR string from Baileys
let connectionState  = "disconnected"; // "disconnected" | "connecting" | "qr" | "connected"
let sock             = null;
let reconnectTimer   = null;
let connectedAt      = 0;     // ms timestamp when connection opened

// Anti-spam guards
const processedIds   = new Set();       // dedup by message id
const MAX_MSG_AGE_MS = 60 * 1000;       // ignore messages older than 60s (offline queue)
const STARTUP_GRACE_MS = 8 * 1000;      // ignore everything for 8s after connect (history sync flood)

const logger = pino({ level: "silent" });

// ── Notify Vercel webhook ─────────────────────────────────────────────────────
async function notifyWebhook(payload) {
  if (!WEBHOOK_URL) return;
  try {
    await fetch(WEBHOOK_URL, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
  } catch (err) {
    console.error("[bridge] Webhook notify error:", err.message);
  }
}

// ── Connect ───────────────────────────────────────────────────────────────────
async function connectToWhatsApp() {
  if (reconnectTimer) { clearTimeout(reconnectTimer); reconnectTimer = null; }
  connectionState = "connecting";

  const { state, saveCreds } = await useMultiFileAuthState("./auth_info");
  const { version } = await fetchLatestBaileysVersion();

  sock = makeWASocket({
    version,
    logger,
    printQRInTerminal: true,
    auth: {
      creds: state.creds,
      keys: makeCacheableSignalKeyStore(state.keys, logger),
    },
    generateHighQualityLinkPreview: false,
  });

  // ── Connection updates ────────────────────────────────────────────────────
  sock.ev.on("connection.update", async (update) => {
    const { connection, lastDisconnect, qr } = update;

    if (qr) {
      currentQR = qr;
      connectionState = "qr";
      console.log("[bridge] 📱 QR ready — scan in admin panel");
    }

    if (connection === "close") {
      currentQR = null;
      connectionState = "disconnected";
      const statusCode = lastDisconnect?.error instanceof Boom
        ? lastDisconnect.error.output?.statusCode
        : 0;
      const loggedOut = statusCode === DisconnectReason.loggedOut;
      console.log(`[bridge] 🔴 Disconnected (code ${statusCode}, loggedOut: ${loggedOut})`);

      notifyWebhook({ type: "DisconnectedCallback", connected: false });

      if (!loggedOut) {
        console.log("[bridge] Reconnecting in 5s...");
        reconnectTimer = setTimeout(connectToWhatsApp, 5000);
      } else {
        console.log("[bridge] Logged out — delete ./auth_info and restart to re-link");
      }
    }

    if (connection === "open") {
      currentQR = null;
      connectionState = "connected";
      connectedAt = Date.now();
      processedIds.clear();
      console.log("[bridge] 🟢 WhatsApp connected! (8s startup grace active)");
      notifyWebhook({ type: "ConnectedCallback", connected: true });
    }
  });

  // ── Save credentials ──────────────────────────────────────────────────────
  sock.ev.on("creds.update", saveCreds);

  // ── Incoming messages ─────────────────────────────────────────────────────
  sock.ev.on("messages.upsert", async ({ messages, type }) => {
    if (type !== "notify") return;

    // ── Anti-spam: ignore the history/offline flood right after connecting ────
    if (Date.now() - connectedAt < STARTUP_GRACE_MS) {
      console.log(`[bridge] ⏳ Startup grace — ignoring ${messages.length} synced message(s)`);
      return;
    }

    for (const msg of messages) {
      try {
        if (msg.key.fromMe) continue;
        if (!msg.key.remoteJid) continue;
        if (isJidBroadcast(msg.key.remoteJid)) continue;
        if (msg.key.remoteJid === "status@broadcast") continue;
        if (msg.key.remoteJid.endsWith("@g.us")) continue;       // skip groups
        if (!msg.key.remoteJid.endsWith("@s.whatsapp.net")) continue; // only 1:1 chats

        // Dedup: skip if we already handled this message id
        const msgId = msg.key.id;
        if (msgId) {
          if (processedIds.has(msgId)) continue;
          processedIds.add(msgId);
          if (processedIds.size > 1000) {
            // keep the set bounded
            const first = processedIds.values().next().value;
            processedIds.delete(first);
          }
        }

        // Skip old messages (offline queue delivered on connect)
        const tsSec = typeof msg.messageTimestamp === "number"
          ? msg.messageTimestamp
          : Number(msg.messageTimestamp?.toNumber?.() ?? msg.messageTimestamp ?? 0);
        if (tsSec > 0 && Date.now() - tsSec * 1000 > MAX_MSG_AGE_MS) {
          console.log(`[bridge] 🕒 Ignoring old message (${Math.round((Date.now() - tsSec * 1000) / 1000)}s old)`);
          continue;
        }

        const phone = msg.key.remoteJid.replace("@s.whatsapp.net", "");
        const m = msg.message || {};
        const text  =
          m.conversation ||
          m.extendedTextMessage?.text ||
          m.imageMessage?.caption ||
          m.videoMessage?.caption ||
          m.documentMessage?.caption ||
          "";

        const senderName = msg.pushName || "Cliente";

        // ── Detect & download media ───────────────────────────────────────────
        let media; // { mimetype, data(base64), filename, caption, kind }
        const mediaNode =
          (m.imageMessage    && { node: m.imageMessage,    kind: "image" }) ||
          (m.documentMessage && { node: m.documentMessage, kind: "document" }) ||
          (m.audioMessage    && { node: m.audioMessage,    kind: "audio" }) ||
          (m.videoMessage    && { node: m.videoMessage,    kind: "video" }) ||
          (m.stickerMessage  && { node: m.stickerMessage,  kind: "sticker" }) ||
          null;

        if (mediaNode) {
          const mimetype = mediaNode.node.mimetype || "application/octet-stream";
          const fileLen = Number(mediaNode.node.fileLength || 0);
          // Only download types Gemini can read, and within size limit
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
                  data: buffer.toString("base64"),
                  filename: mediaNode.node.fileName || undefined,
                  caption: mediaNode.node.caption || undefined,
                  kind: mediaNode.kind,
                };
              } else {
                media = { mimetype, data: "", kind: mediaNode.kind, tooLarge: true };
              }
            } catch (e) {
              console.error("[bridge] media download failed:", e.message);
              media = { mimetype, data: "", kind: mediaNode.kind };
            }
          } else {
            // Not downloadable (too big or unsupported) — just flag the kind
            media = { mimetype, data: "", kind: mediaNode.kind, tooLarge: fileLen > MAX_MEDIA_BYTES };
          }
        }

        if (!text && !media) continue; // nothing to forward

        console.log(`[bridge] 📩 ${senderName} (${phone}): "${text || "[" + (media?.kind || "media") + "]"}"`);

        // Forward to Vercel
        await notifyWebhook({
          type:        "ReceivedCallback",
          phone,
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
  if (secret !== API_SECRET) {
    return res.status(401).json({ error: "Unauthorized" });
  }
  next();
}

// ── Routes ────────────────────────────────────────────────────────────────────

// Health (no auth needed)
app.get("/health", (req, res) => {
  res.json({ ok: true, status: connectionState });
});

// Status (no auth needed — panel needs to poll this)
app.get("/status", (req, res) => {
  res.json({
    status: connectionState,
    connected: connectionState === "connected",
    hasQR: !!currentQR,
  });
});

// QR code image as base64 data URL
app.get("/qr", async (req, res) => {
  if (!currentQR) {
    return res.status(404).json({
      error: "No QR available",
      status: connectionState,
      hint: connectionState === "connected"
        ? "Already connected"
        : "Starting up, try again in a few seconds",
    });
  }
  try {
    const qrDataUrl = await QRCode.toDataURL(currentQR, { width: 300, margin: 2 });
    res.json({ qrcode: qrDataUrl, status: connectionState });
  } catch (err) {
    res.status(500).json({ error: "QR generation failed", detail: String(err) });
  }
});

// Send message (protected)
app.post("/send", requireSecret, async (req, res) => {
  const { phone, message } = req.body;
  if (!phone || !message) {
    return res.status(400).json({ error: "phone and message required" });
  }
  if (connectionState !== "connected" || !sock) {
    return res.status(503).json({ error: "WhatsApp not connected", status: connectionState });
  }
  try {
    const jid = phone.includes("@") ? phone : `${phone}@s.whatsapp.net`;
    const sent = await sock.sendMessage(jid, { text: message });
    res.json({ ok: true, messageId: sent?.key?.id });
  } catch (err) {
    console.error("[bridge] Send error:", err);
    res.status(500).json({ error: String(err) });
  }
});

// Logout (reset session — protected)
app.post("/logout", requireSecret, async (req, res) => {
  try {
    if (sock) await sock.logout();
  } catch { /* ignore */ }
  const fs = require("fs");
  const path = require("path");
  const authDir = "./auth_info";
  if (fs.existsSync(authDir)) {
    fs.rmSync(authDir, { recursive: true, force: true });
  }
  res.json({ ok: true, message: "Logged out — reconnecting..." });
  setTimeout(connectToWhatsApp, 2000);
});

// ── Start ─────────────────────────────────────────────────────────────────────
connectToWhatsApp().catch(console.error);

app.listen(PORT, () => {
  console.log(`[bridge] 🚀 Running on port ${PORT}`);
  console.log(`[bridge] Webhook URL: ${WEBHOOK_URL || "(not set)"}`);
});
