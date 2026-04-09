// baileys/index.js
// Phase 1 — Full WhatsApp listener
// Read-only. sock.sendMessage() does not exist in this codebase.

const {
  default: makeWASocket,
  useMultiFileAuthState,
  DisconnectReason,
  fetchLatestBaileysVersion,
  makeInMemoryStore,
  jidNormalizedUser,
} = require('@whiskeysockets/baileys');

const axios = require('axios');
const qrcode = require('qrcode-terminal');
const pino = require('pino');
const dotenv = require('dotenv');
const path = require('path');
const fs = require('fs');

// Load .env from parent directory (mounted into container)
dotenv.config({ path: path.resolve(__dirname, '../.env') });

// ── Config ────────────────────────────────────────────────────────────────────

const TARGET_GROUP_JID = process.env.TARGET_GROUP_JID;
const INGEST_URL = process.env.INGEST_URL || 'http://fastapi:8000/ingest';
const SESSIONS_DIR = path.resolve(__dirname, 'sessions');
const BACKFILL_COUNT = 50;

// Reconnect backoff settings
const INITIAL_RECONNECT_DELAY_MS = 2000;
const MAX_RECONNECT_DELAY_MS = 60000;
const BACKOFF_MULTIPLIER = 2;

// ── Logger ────────────────────────────────────────────────────────────────────

const logger = pino({
  level: 'info',
  transport: {
    target: 'pino-pretty',
    options: { colorize: false, translateTime: 'SYS:standard' },
  },
});

// ── Validate config ───────────────────────────────────────────────────────────

if (!TARGET_GROUP_JID || TARGET_GROUP_JID === '120363XXXXXXXXXX@g.us') {
  logger.warn(
    'TARGET_GROUP_JID is not set or is still the placeholder value. ' +
    'The listener will start but will not forward any messages. ' +
    'Update TARGET_GROUP_JID in .env after identifying your group JID.'
  );
}

// Ensure sessions directory exists
if (!fs.existsSync(SESSIONS_DIR)) {
  fs.mkdirSync(SESSIONS_DIR, { recursive: true });
  logger.info({ sessionsDir: SESSIONS_DIR }, 'Created sessions directory');
}

// ── Helpers ───────────────────────────────────────────────────────────────────

/**
 * Extract plain text from a Baileys message object.
 * Handles regular messages, extended text, image captions, etc.
 */
function extractText(message) {
  if (!message) return null;
  return (
    message.conversation ||
    message.extendedTextMessage?.text ||
    message.imageMessage?.caption ||
    message.videoMessage?.caption ||
    message.documentMessage?.caption ||
    null
  );
}

/**
 * Build the payload expected by POST /ingest
 */
function buildPayload(msg, messageId, text, replyToId, replyToPreview) {
  return {
    message_id: messageId,
    text: text,
    timestamp: new Date(
      (msg.messageTimestamp?.toNumber
        ? msg.messageTimestamp.toNumber()
        : msg.messageTimestamp) * 1000
    ).toISOString(),
    sender: msg.key?.participant || msg.key?.remoteJid || 'unknown',
    reply_to_id: replyToId || null,
    reply_to_preview: replyToPreview
      ? replyToPreview.slice(0, 100)
      : null,
  };
}

/**
 * POST a single payload to FastAPI /ingest with retry on network error.
 */
async function postToIngest(payload, retries = 3) {
  for (let attempt = 1; attempt <= retries; attempt++) {
    try {
      const response = await axios.post(INGEST_URL, payload, {
        timeout: 10000,
        headers: { 'Content-Type': 'application/json' },
      });
      logger.info(
        { message_id: payload.message_id, status: response.status },
        'Posted to ingest'
      );
      return;
    } catch (err) {
      const isLast = attempt === retries;
      logger.warn(
        {
          message_id: payload.message_id,
          attempt,
          error: err.message,
        },
        isLast ? 'Failed to post to ingest after all retries' : 'Retrying ingest post'
      );
      if (!isLast) {
        await new Promise((r) => setTimeout(r, 1000 * attempt));
      }
    }
  }
}

/**
 * Process a raw Baileys message and POST it to /ingest if valid.
 */
async function handleMessage(msg) {
  // Only process messages from the target group
  const remoteJid = msg.key?.remoteJid;
  if (!remoteJid || remoteJid !== TARGET_GROUP_JID) return;

  // Skip messages sent by this bot account itself (should never happen — read-only)
  if (msg.key?.fromMe) return;

  const messageId = msg.key?.id;
  if (!messageId) return;

  // Extract text content
  const message = msg.message;
  if (!message) return;

  const text = extractText(message);
  if (!text || text.trim().length === 0) return;

  // Extract reply context if present
  const quotedMsg =
    message.extendedTextMessage?.contextInfo?.quotedMessage || null;
  const replyToId =
    message.extendedTextMessage?.contextInfo?.stanzaId || null;
  const replyToPreview = quotedMsg ? extractText(quotedMsg) : null;

  const payload = buildPayload(msg, messageId, text, replyToId, replyToPreview);

  logger.info(
    {
      message_id: messageId,
      sender: payload.sender,
      text_preview: text.slice(0, 60),
      has_reply: !!replyToId,
    },
    'Processing message'
  );

  await postToIngest(payload);
}

// ── Main connection loop ───────────────────────────────────────────────────────

async function connectToWhatsApp(reconnectDelay = INITIAL_RECONNECT_DELAY_MS) {
  const { state, saveCreds } = await useMultiFileAuthState(SESSIONS_DIR);
  const { version } = await fetchLatestBaileysVersion();

  logger.info({ version }, 'Baileys version');

  // const sock = makeWASocket({
  //   version,
  //   auth: state,
  //   logger: pino({ level: 'silent' }), // suppress internal Baileys logs
  //   printQRInTerminal: false,           // we handle QR ourselves
  //   browser: ['Placement Bot', 'Chrome', '1.0.0'],
  //   syncFullHistory: false,
  // });

  const sock = makeWASocket({
    version,
    auth: state,
    logger: pino({ level: 'silent' }),
    printQRInTerminal: false,
    browser: ['Placement Bot', 'Chrome', '1.0.0'],
    syncFullHistory: false,
    shouldSyncHistoryMessage: () => false,
    getMessage: async () => undefined,
    cachedGroupMetadata: async () => undefined,
  });

  // ── QR code ──────────────────────────────────────────────────────────────────

  sock.ev.on('connection.update', async (update) => {
    const { connection, lastDisconnect, qr } = update;

    if (qr) {
      logger.info('QR code received — scan with WhatsApp on your phone:');
      qrcode.generate(qr, { small: true });
    }

    if (connection === 'open') {
      logger.info('WhatsApp connection established');
      reconnectDelay = INITIAL_RECONNECT_DELAY_MS; // reset backoff on success

      // // ── TEMPORARY — list all groups to find TARGET_GROUP_JID ─────────────
      // try {
      //   const groups = await sock.groupFetchAllParticipating();
      //   Object.entries(groups).forEach(([jid, group]) => {
      //     logger.info({ jid, name: group.subject }, 'Group found');
      //   });
      // } catch (err) {
      //   logger.warn({ error: err.message }, 'Could not fetch group list');
      // }
      // // ── END TEMPORARY ─────────────────────────────────────────────────────

      // ── Backfill last 50 messages from target group ─────────────────────── // reset backoff on success

      // ── Backfill last 50 messages from target group ───────────────────────
      // if (TARGET_GROUP_JID && TARGET_GROUP_JID !== '120363XXXXXXXXXX@g.us') {
      //   try {
      //     logger.info(
      //       { group: TARGET_GROUP_JID, count: BACKFILL_COUNT },
      //       'Starting backfill'
      //     );
      //     const history = await sock.fetchMessageHistory(
      //       BACKFILL_COUNT,
      //       TARGET_GROUP_JID,
      //       undefined
      //     );
      //     if (history && history.length > 0) {
      //       logger.info({ count: history.length }, 'Backfill messages received');
      //       for (const msg of history) {
      //         await handleMessage(msg);
      //       }
      //     } else {
      //       logger.info('No backfill messages returned');
      //     }
      //   } catch (err) {
      //     logger.warn({ error: err.message }, 'Backfill failed — continuing without it');
      //   }
      // }

      // ── Backfill disabled — live messages only ────────────────────────────
      logger.info('Backfill disabled — listening for live messages only');
    }

    if (connection === 'close') {
      const statusCode =
        lastDisconnect?.error?.output?.statusCode;
      const isLoggedOut =
        statusCode === DisconnectReason.loggedOut;

      if (isLoggedOut) {
        logger.warn(
          'Logged out from WhatsApp. Clearing session and requesting new QR scan.'
        );
        // Clear session so next startup shows fresh QR
        try {
          fs.rmSync(SESSIONS_DIR, { recursive: true, force: true });
          fs.mkdirSync(SESSIONS_DIR, { recursive: true });
        } catch (e) {
          logger.error({ error: e.message }, 'Failed to clear sessions directory');
        }
        // Reconnect immediately for new QR
        await connectToWhatsApp(INITIAL_RECONNECT_DELAY_MS);
      } else {
        // Transient disconnect — exponential backoff reconnect
        const nextDelay = Math.min(
          reconnectDelay * BACKOFF_MULTIPLIER,
          MAX_RECONNECT_DELAY_MS
        );
        logger.warn(
          { statusCode, reconnectInMs: reconnectDelay },
          'Connection closed — reconnecting with backoff'
        );
        setTimeout(() => connectToWhatsApp(nextDelay), reconnectDelay);
      }
    }
  });

  // ── Save credentials whenever they update ────────────────────────────────────

  sock.ev.on('creds.update', saveCreds);

  // ── Live message listener ─────────────────────────────────────────────────────

  sock.ev.on('messages.upsert', async ({ messages, type }) => {
    // type === 'notify' means new incoming messages
    // type === 'append' means historical/backfill — skip here, handled above
    if (type !== 'notify') return;

    for (const msg of messages) {
      try {
        await handleMessage(msg);
      } catch (err) {
        logger.error(
          { error: err.message, message_id: msg.key?.id },
          'Unhandled error processing message'
        );
      }
    }
  });

  // ── Group participant updates (log only, no action) ───────────────────────────

  sock.ev.on('group-participants.update', (update) => {
    if (update.id === TARGET_GROUP_JID) {
      logger.info(
        { action: update.action, participants: update.participants },
        'Group participant update'
      );
    }
  });
}

// ── Entry point ───────────────────────────────────────────────────────────────

logger.info('Baileys WhatsApp listener starting — Phase 1');
logger.info({ targetGroup: TARGET_GROUP_JID, ingestUrl: INGEST_URL }, 'Config');

connectToWhatsApp().catch((err) => {
  logger.error({ error: err.message }, 'Fatal error in connectToWhatsApp');
  process.exit(1);
});