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
 *   DATABASE_URL  — Neon Postgres URL (same as Vercel) for auth persistence
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
const QRCode  = require("qrcode");
const pino    = require("pino");
const fs      = require("fs");
const path    = require("path");
const { Pool } = require("pg");

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
const DATABASE_URL = process.env.DATABASE_URL || "";

// ── Postgres client (for auth persistence) ───────────────────────────────────
let db = null;
if (DATABASE_URL) {
  db = new Pool({
    connectionString: DATABASE_URL,
    ssl: { rejectUnauthorized: false },
    max: 3,
  });
  console.log("[bridge] ✅ Postgres connected — auth state will persist across deploys");
} else {
  console.warn("[bridge] ⚠️  DATABASE_URL not set — auth state is ephemeral (lost on redeploy)");
}

// ── Auth state persistence ─────────────────────────────────────────────────────
const AUTH_DIR = "./auth_info";

// Serialize auth_info directory → { "filename": "base64content", ... }
function serializeAuthDir() {
  const files = {};
  if (!fs.existsSync(AUTH_DIR)) return files;
  function walk(dir, prefix) {
    for (const name of fs.readdirSync(dir)) {
      const fullPath = path.join(dir, name);
      const relPath  = prefix ? `${prefix}/${name}` : name;
      if (fs.statSync(fullPath).isDirectory()) {
        walk(fullPath, relPath);
      } else {
        files[relPath] = fs.readFileSync(fullPath).toString("base64");
      }
    }
  }
  walk(AUTH_DIR, "");
  return files;
}

// Restore auth_info directory from serialized object
function restoreAuthDir(files) {
  if (!files || typeof files !== "object") return;
  if (!fs.existsSync(AUTH_DIR)) fs.mkdirSync(AUTH_DIR, { recursive: true });
  for (const [rel, b64] of Object.entries(files)) {
    const fullPath = path.join(AUTH_DIR, rel);
    fs.mkdirSync(path.dirname(fullPath), { recursive: true });
    fs.writeFileSync(fullPath, Buffer.from(b64, "base64"));
  }
}

async function saveAuthToDB() {
  if (!db) return;
  try {
    const files = serializeAuthDir();
    if (Object.keys(files).length === 0) return;
    await db.query(`
      CREATE TABLE IF NOT EXISTS wa_auth_state (
        id TEXT PRIMARY KEY,
        files JSONB NOT NULL,
        updated_at TIMESTAMPTZ DEFAULT NOW()
      )
    `);
    await db.query(`
      INSERT INTO wa_auth_state (id, files, updated_at)
      VALUES ('metalia', $1, NOW())
      ON CONFLICT (id) DO UPDATE SET files = $1, updated_at = NOW()
    `, [JSON.stringify(files)]);
    console.log(`[bridge] 💾 Auth saved to Postgres (${Object.keys(files).length} files)`);
  } catch (err) {
    console.error("[bridge] Auth save error:", err.message);
  }
}

async function loadAuthFromDB() {
  if (!db) return;
  try {
    await db.query(`
      CREATE TABLE IF NOT EXISTS wa_auth_state (
        id TEXT PRIMARY KEY,
        files JSONB NOT NULL,
        updated_at TIMESTAMPTZ DEFAULT NOW()
      )
    `);
    const result = await db.query("SELECT files FROM wa_auth_state WHERE id = 'metalia'");
    if (result.rows.length > 0) {
      const files = result.rows[0].files;
      restoreAuthDir(files);
      console.log(`[bridge] 📂 Auth restored from Postgres (${Object.keys(files).length} files)`);
    } else {
      console.log("[bridge] No stored auth found — will show QR");
    }
  } catch (err) {
    console.error("[bridge] Auth load error:", err.message);
  }
}

async function clearAuthFromDB() {
  if (!db) return;
  try {
    await db.query("DELETE FROM wa_auth_state WHERE id = 'metalia'");
    console.log("[bridge] 🗑️  Auth cleared from Postgres");
  } catch (err) {
    console.error("[bridge] Auth clear error:", err.message);
  }
}

// ── State ─────────────────────────────────────────────────────────────────────
let currentQR        = null;
let connectionState  = "disconnected";
let sock             = null;
let reconnectTimer   = null;
let connectedAt      = 0;

// Anti-spam guards
const processedIds   = new Set();
const MAX_MSG_AGE_MS = 60 * 1000;     // ignore messages older than 60s (offline queue)
const STARTUP_GRACE_MS = 8 * 1000;   // ignore everything for 8s after connect (history sync)

const logger = pino({ level: "silent" });

// ── Notify Vercel webhook ─────────────────────────────────────────────────────
async function notifyWebhook(payload) {
  if (!WEBHOOK_URL) {
    console.warn("[bridge] ⚠️  WEBHOOK_URL not set — message NOT forwarded");
    return;
  }
  try {
    const res = await fetch(WEBHOOK_URL, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
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

  // Restore auth state from Postgres before loading from disk
  await loadAuthFromDB();

  const { state, saveCreds } = await useMultiFileAuthState(AUTH_DIR);
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
        console.log("[bridge] Logged out — will show QR on next connect");
        await clearAuthFromDB();
        // Clear local files too
        if (fs.existsSync(AUTH_DIR)) fs.rmSync(AUTH_DIR, { recursive: true, force: true });
        reconnectTimer = setTimeout(connectToWhatsApp, 2000);
      }
    }

    if (connection === "open") {
      currentQR = null;
      connectionState = "connected";
      connectedAt = Date.now();
      processedIds.clear();
      console.log("[bridge] 🟢 WhatsApp connected! (8s startup grace active)");
      notifyWebhook({ type: "ConnectedCallback", connected: true });
      // Save auth to Postgres so it survives redeploys
      await saveAuthToDB();
    }
  });

  // ── Save credentials ──────────────────────────────────────────────────────
  sock.ev.on("creds.update", async () => {
    await saveCreds();
    // Also persist to Postgres
    await saveAuthToDB();
  });

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
        const text =
          m.conversation ||
          m.extendedTextMessage?.text ||
          m.imageMessage?.caption ||
          m.videoMessage?.caption ||
          m.documentMessage?.caption ||
          "";

        const senderName = msg.pushName || "Cliente";

        // ── Detect & download media ───────────────────────────────────────────
        let media;
        const mediaNode =
          (m.imageMessage    && { node: m.imageMessage,    kind: "image" }) ||
          (m.documentMessage && { node: m.documentMessage, kind: "document" }) ||
          (m.audioMessage    && { node: m.audioMessage,    kind: "audio" }) ||
          (m.videoMessage    && { node: m.videoMessage,    kind: "video" }) ||
          (m.stickerMessage  && { node: m.stickerMessage,  kind: "sticker" }) ||
          null;

        if (mediaNode) {
          const mimetype = mediaNode.node.mimetype || "application/octet-stream";
          const fileLen  = Number(mediaNode.node.fileLength || 0);
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

        if (!text && !media) continue;

        console.log(`[bridge] 📩 ${senderName} (${phone}): "${text || "[" + (media?.kind || "media") + "]"}"`);

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
  res.json({
    ok: true,
    status: connectionState,
    webhookConfigured: !!WEBHOOK_URL,
    dbConfigured: !!DATABASE_URL,
  });
});

// Status (no auth needed — panel needs to poll this)
app.get("/status", (req, res) => {
  res.json({
    status: connectionState,
    connected: connectionState === "connected",
    hasQR: !!currentQR,
  });
});

// Debug — show config (no auth needed, masked values)
app.get("/debug", (req, res) => {
  res.json({
    connectionState,
    webhookConfigured: !!WEBHOOK_URL,
    webhookUrl: WEBHOOK_URL
      ? WEBHOOK_URL.replace(/https?:\/\/[^/]+/, "https://<host>")
      : "(not set — WEBHOOK_URL env var missing!)",
    dbConfigured: !!DATABASE_URL,
    processedIdsCount: processedIds.size,
    connectedAt: connectedAt ? new Date(connectedAt).toISOString() : null,
    uptimeSec:   connectedAt ? Math.round((Date.now() - connectedAt) / 1000) : null,
  });
});

// Test webhook — fires a fake message to verify the pipeline (no auth)
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

// QR code image as base64 data URL
app.get("/qr", async (req, res) => {
  if (!currentQR) {
    return res.status(404).json({
      error:  "No QR available",
      status: connectionState,
      hint:   connectionState === "connected"
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
    const jid  = phone.includes("@") ? phone : `${phone}@s.whatsapp.net`;
    const sent = await sock.sendMessage(jid, { text: message });
    res.json({ ok: true, messageId: sent?.key?.id });
  } catch (err) {
    console.error("[bridge] Send error:", err);
    res.status(500).json({ error: String(err) });
  }
});

// Logout (reset session — protected)
app.post("/logout", requireSecret, async (req, res) => {
  try { if (sock) await sock.logout(); } catch { /* ignore */ }
  await clearAuthFromDB();
  if (fs.existsSync(AUTH_DIR)) fs.rmSync(AUTH_DIR, { recursive: true, force: true });
  res.json({ ok: true, message: "Logged out — scan QR to reconnect" });
  setTimeout(connectToWhatsApp, 2000);
});

// ── Start ─────────────────────────────────────────────────────────────────────
connectToWhatsApp().catch(console.error);

app.listen(PORT, () => {
  console.log(`[bridge] 🚀 Running on port ${PORT}`);
  console.log(`[bridge] Webhook URL: ${WEBHOOK_URL || "(not set)"}`);
  console.log(`[bridge] DB persistence: ${DATABASE_URL ? "enabled" : "disabled"}`);
});
