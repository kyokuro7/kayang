const path = require('path');
require('dotenv').config({ path: path.join(__dirname, '.env') });
const { Telegraf, Markup, session } = require('telegraf');
const axios = require('axios');
const crypto = require('crypto');

const clean = (v) => (v || '').trim().replace(/^["']|["']$/g, '');
const BOT_TOKEN = clean(process.env.BOT_TOKEN);
const ADMIN_ID = clean(process.env.ADMIN_ID);
const BASE_URL = process.env.BASE_URL || 'https://amfinder.web.id';
const DID = process.env.DID || '267f6fd3-34f4-4d9a-8d90-5aa28db8922b';

if (!BOT_TOKEN || !ADMIN_ID) {
  console.error('❌ BOT_TOKEN dan ADMIN_ID wajib diisi di .env');
  process.exit(1);
}

if (!/^\d+:[A-Za-z0-9_-]{30,}$/.test(BOT_TOKEN)) {
  console.error('❌ Format BOT_TOKEN salah. Harus seperti 123456789:AAxxxxxxxx (tanpa spasi/kutip)');
  process.exit(1);
}

const bot = new Telegraf(BOT_TOKEN);

// session in-memory hilang saat restart -> beri default agar tidak error
bot.use(session({ defaultSession: () => ({ waitingForUrl: false, step: null }) }));

const loadingFrames = ['⠋', '⠙', '⠹', '⠸', '⠼', '⠴', '⠦', '⠧', '⠇', '⠏'];
const loadingEmojis = ['🔍', '🔎', '🕵️', '🔍', '🔎', '🕵️'];

// HMAC berantai (sama dengan crypto.subtle versi asli, tapi sync & jalan di semua versi Node)
function computeProof(saltHex, messageStr, rounds) {
  const salt = Buffer.from(saltHex, 'hex');
  let data = Buffer.from(messageStr, 'utf8');
  const n = Math.max(1, Math.min(10000, Math.floor(Number(rounds) || 1)));
  for (let i = 0; i < n; i++) {
    data = crypto.createHmac('sha256', salt).update(data).digest();
  }
  return data.toString('hex');
}

const escapeHtml = (t) => String(t).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');

const UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36';

function apiError(error) {
  if (error.response) {
    let d = error.response.data;
    if (Buffer.isBuffer(d)) d = d.toString('utf8');
    if (d && typeof d.on === 'function') d = '(stream)';
    if (typeof d !== 'string') { try { d = JSON.stringify(d); } catch (_) { d = String(d); } }
    return `HTTP ${error.response.status}: ${String(d).slice(0, 300)}`;
  }
  return error.message;
}

// Parse blok SSE: "event: x\ndata: y\n\n"
function parseSSE(raw) {
  const events = [];
  for (const block of raw.replace(/\r/g, '').split('\n\n')) {
    let event = 'message';
    const dataLines = [];
    for (const line of block.split('\n')) {
      if (line.startsWith('event:')) event = line.slice(6).trim();
      else if (line.startsWith('data:')) dataLines.push(line.slice(5).trim());
    }
    if (dataLines.length) events.push({ event, data: dataLines.join('\n') });
  }
  return events;
}

async function scrapeOnce(targetUrl) {
  const base = BASE_URL.replace(/\/+$/, '');
  const headers = {
    'Content-Type': 'application/json',
    'Origin': base,
    'Referer': `${base}/`,
    'User-Agent': UA
  };

  const challengeRes = await axios.post(`${base}/api/v/2b51359f`, { did: DID }, { headers, timeout: 20000 });
  const { cid, salt, ts, rounds } = challengeRes.data || {};
  if (!cid || !salt) throw new Error('Challenge tidak valid: ' + JSON.stringify(challengeRes.data).slice(0, 200));

  const proof = computeProof(salt, `${cid}|${DID}|${ts}`, rounds);

  const verifyRes = await axios.post(`${base}/api/v/2b51359f`, {
    cid, did: DID, ts, proof, score: 0, reasons: []
  }, { headers, timeout: 20000 });

  const token = verifyRes.data && verifyRes.data.token;
  if (!token) throw new Error('Gagal mendapatkan token: ' + JSON.stringify(verifyRes.data).slice(0, 200));

  const sseUrl = `${base}/api/find?url=${encodeURIComponent(targetUrl)}&bat=88&chg=0&t=${encodeURIComponent(token)}`;
  const sseRes = await axios.get(sseUrl, {
    responseType: 'stream',
    headers: { 'Accept': 'text/event-stream', 'Referer': `${base}/`, 'User-Agent': UA }
    // sengaja tanpa timeout axios: untuk stream, timeout dihitung per-koneksi & memutus SSE yang lama
  });

  return new Promise((resolve, reject) => {
    let raw = '';
    let done = false;
    const finish = (fn, val) => {
      if (done) return;
      done = true;
      clearTimeout(timer);
      sseRes.data.destroy();
      fn(val);
    };
    const timer = setTimeout(() => finish(reject, new Error('Timeout: server tidak mengirim hasil dalam 90 detik')), 90000);

    const check = () => {
      for (const ev of parseSSE(raw)) {
        if (ev.event === 'result') {
          try { return finish(resolve, JSON.parse(ev.data)); } catch (_) {}
        }
        if (ev.event === 'error') {
          return finish(reject, new Error('Server: ' + ev.data.slice(0, 300)));
        }
      }
    };

    sseRes.data.on('data', (chunk) => { raw += chunk.toString(); check(); });
    sseRes.data.on('end', () => {
      check();
      console.log('[SSE raw]', raw.slice(-1500));
      finish(reject, new Error('Hasil SSE tidak ditemukan / gagal diparsing'));
    });
    sseRes.data.on('error', (err) => finish(reject, err));
  });
}

async function scrapePreset(targetUrl) {
  let lastErr;
  for (let attempt = 1; attempt <= 2; attempt++) {
    try {
      return await scrapeOnce(targetUrl);
    } catch (error) {
      lastErr = error;
      console.error(`[scrape] percobaan ${attempt} gagal:`, apiError(error));
      if (attempt < 2) await new Promise((r) => setTimeout(r, 1500));
    }
  }
  throw new Error(apiError(lastErr));
}

// Kumpulkan semua URL dari hasil JSON (nama field & struktur bisa berbeda-beda)
function collectUrls(obj, key = '', out = []) {
  if (obj == null) return out;
  if (typeof obj === 'string') {
    if (/^https?:\/\//i.test(obj.trim())) out.push({ key: key.toLowerCase(), url: obj.trim() });
  } else if (Array.isArray(obj)) {
    obj.forEach((v, i) => collectUrls(v, key || String(i), out));
  } else if (typeof obj === 'object') {
    for (const [k, v] of Object.entries(obj)) collectUrls(v, k, out);
  }
  return out;
}

function classifyResult(data) {
  const links = collectUrls(data);
  const seen = new Set();
  const r = { alight: [], xml: [], thumb: [], other: [], size: null };
  for (const { key, url } of links) {
    if (seen.has(url)) continue;
    seen.add(url);
    const u = url.toLowerCase();
    if (/thumb|cover|image|preview|poster/.test(key) || /\.(jpe?g|png|webp)(\?|$)/.test(u)) r.thumb.push(url);
    else if (/xml/.test(key) || u.includes('drive.google') || u.includes('docs.google')) r.xml.push(url);
    else if (/alight|creative|preset/.test(key) || /alight\.link|alightcreative|alightmotion|alight\.me/.test(u)) r.alight.push(url);
    else r.other.push(url);
  }
  const root = (data && typeof data.data === 'object' && data.data) || data || {};
  r.size = data.alight_creative_size || root.alight_creative_size || null;
  r.total = r.alight.length + r.xml.length + r.other.length + r.thumb.length;
  return r;
}

function formatResult(data) {
  const r = classifyResult(data);
  let msg = '<b>😆 PRESET KETEMU BOSQ ✨</b>\n\n';
  const a = (u, t) => `<a href="${escapeHtml(u)}">${escapeHtml(t)}</a>`;

  r.alight.forEach((u, i) => {
    msg += `<b>😼 ${escapeHtml(r.size || '? MB')} :</b> ${a(u, 'Link Alight Creative' + (r.alight.length > 1 ? ` ${i + 1}` : ''))}\n`;
  });
  r.xml.forEach((u, i) => {
    msg += `<b>🔥 XML :</b> ${a(u, 'Link XML Google Drive' + (r.xml.length > 1 ? ` ${i + 1}` : ''))}\n`;
  });
  r.other.forEach((u) => {
    msg += `<b>📱 URL  :</b> ${a(u, u)}\n`;
  });
  if (r.thumb.length) msg += `\n<b>🖼 Thumbnail:</b> ${a(r.thumb[0], 'Lihat')}`;
  return msg;
}

async function showLoadingAnimation(ctx, text = 'Sedang mencari preset...') {
  const sent = await ctx.reply(text);
  let frame = 0;
  const interval = setInterval(async () => {
    try {
      await ctx.telegram.editMessageText(
        ctx.chat.id,
        sent.message_id,
        null,
        `${loadingEmojis[frame % loadingEmojis.length]} ${loadingFrames[frame % loadingFrames.length]} ${text}`
      );
      frame++;
    } catch (e) {
      clearInterval(interval);
    }
  }, 300);
  return { sent, interval };
}

// ================= TEMP MAIL (Guerrilla Mail API) =================
// Dokumentasi resmi: https://www.guerrillamail.com/GuerrillaMailAPI.html
const GM_URL = 'https://api.guerrillamail.com/ajax.php';
let cheerio = null;
try { cheerio = require('cheerio'); } catch (_) { /* opsional: npm i cheerio */ }

function newTempSession() {
  return { email: null, sid: null, waiting: false };
}

function parseEmailContent(html) {
  if (!html) return { text: '', links: [] };
  let text = '';
  let links = [];
  if (cheerio) {
    const $ = cheerio.load(html);
    $('a[href]').each((i, el) => {
      const href = $(el).attr('href');
      if (href && /^https?:\/\//i.test(href)) links.push(href);
    });
    $('script, style').remove();
    text = $('body').text().replace(/\n\s*\n/g, '\n').trim();
  } else {
    links = [...html.matchAll(/href=["'](https?:\/\/[^"']+)["']/gi)].map((m) => m[1]);
    text = html.replace(/<(script|style)[\s\S]*?<\/\1>/gi, '').replace(/<[^>]+>/g, ' ').replace(/\s+/g, ' ').trim();
  }
  // email teks biasa: ambil URL dari teksnya juga
  links = links.concat(text.match(/https?:\/\/[^\s"'<>)]+/g) || []);
  return { text, links: [...new Set(links)] };
}

async function gmCall(sess, fn, extra = {}) {
  const params = { f: fn, ip: '127.0.0.1', agent: 'Mozilla/5.0', ...extra };
  if (sess.sid) params.sid_token = sess.sid;
  const res = await axios.get(GM_URL, { params, timeout: 20000 });
  if (!res.data || typeof res.data !== 'object') throw new Error('Respon Guerrilla Mail tidak valid');
  return res.data;
}

async function tempCreate(sess) {
  sess.sid = null;
  const r = await gmCall(sess, 'get_email_address', { lang: 'en' });
  if (!r.email_addr || !r.sid_token) throw new Error('Gagal membuat email: ' + JSON.stringify(r).slice(0, 200));
  sess.email = r.email_addr;
  sess.sid = r.sid_token;
  return r.email_addr;
}

async function tempInbox(sess) {
  const r = await gmCall(sess, 'check_email', { seq: 0 });
  // pesan sambutan otomatis dari Guerrilla Mail tidak ditampilkan
  const items = (r.list || []).filter((m) => !/^no-reply@guerrillamail\.com$/i.test(m.mail_from || ''));

  const messages = await Promise.all(items.slice(0, 5).map(async (m) => {
    let detail = m;
    try { detail = await gmCall(sess, 'fetch_email', { email_id: m.mail_id }); } catch (_) {}
    return {
      id: m.mail_id,
      subject: detail.mail_subject || m.mail_subject,
      fromName: detail.mail_from || m.mail_from || '-',
      date: m.mail_timestamp,
      content: detail.mail_body || m.mail_excerpt || ''
    };
  }));

  const raw = { provider: 'guerrillamail', total: items.length, count: r.count };
  if (!messages.length) console.log('[tempmail raw]', sess.email, JSON.stringify(raw));
  return { messages, raw };
}

function formatInbox(email, messages, raw) {
  let msg = `<b>📧 Temp Mail</b>\n<code>${escapeHtml(email)}</code>\n\n`;
  if (!messages.length) {
    let dbg = '';
    try { dbg = JSON.stringify(raw); } catch (_) {}
    return msg + '📭 Inbox masih kosong.\n\nKalau email baru dikirim, tunggu 10–30 detik lalu tekan <b>Cek Inbox</b> lagi, atau pakai <b>Tunggu otomatis</b>.' +
      (dbg ? `\n\n<i>Respon server:</i> <code>${escapeHtml(dbg.slice(0, 200))}</code>` : '');
  }
  msg += `📬 <b>${messages.length} email masuk</b>\n`;
  for (const m of messages.slice(0, 5)) {
    const { text, links } = parseEmailContent(m.content || m.html || m.body || m.text || m.preview || '');
    msg += `\n━━━━━━━━━━\n<b>${escapeHtml(m.subject || '(tanpa subjek)')}</b>\n`;
    msg += `Dari: ${escapeHtml(m.fromName || m.fromAddress || '-')}\n`;
    if (text) msg += `${escapeHtml(text.slice(0, 250))}${text.length > 250 ? '…' : ''}\n`;
    [...new Set(links)].slice(0, 4).forEach((l, i) => {
      msg += `🔗 <code>${escapeHtml(l.slice(0, 600))}</code>\n`;
    });
  }
  if (msg.length > 3900) msg = msg.slice(0, 3900) + '\n…(dipotong)';
  return msg;
}

const tempKeyboard = Markup.inlineKeyboard([
  [Markup.button.callback('📥 Cek Inbox', 'tm_check'), Markup.button.callback('⏳ Tunggu otomatis', 'tm_wait')],
  [Markup.button.callback('🆕 Email Baru', 'tm_new'), Markup.button.callback('⬅️ Menu', 'back_menu')]
]);

// edit pesan, abaikan error "message is not modified"
async function safeEdit(ctx, text, extra) {
  try {
    await ctx.editMessageText(text, { parse_mode: 'HTML', disable_web_page_preview: true, ...extra });
  } catch (e) {
    if (!/not modified/i.test(e.message)) throw e;
  }
}

// ================= AM PREMIUM (satriam magic link) =================
const SATRIAM_URL = 'https://satriam.satriadeveloperz.workers.dev';
const SATRIAM_HEADERS = {
  'User-Agent': 'Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Mobile Safari/537.36',
  'Accept-Language': 'id-ID,id;q=0.9,en-US;q=0.8,en;q=0.7',
  'Content-Type': 'application/json',
  'Origin': SATRIAM_URL,
  'Referer': `${SATRIAM_URL}/`
};

async function satriamSendLink(email) {
  const res = await axios.post(`${SATRIAM_URL}/api/satriam/send-link`, { email }, { headers: SATRIAM_HEADERS, timeout: 20000 });
  if (!res.data || !res.data.success) throw new Error(`Gagal kirim link: ${JSON.stringify(res.data).slice(0, 300)}`);
  return res.data;
}

async function satriamVerify(email, magicLink) {
  const res = await axios.post(`${SATRIAM_URL}/api/satriam/verify-link`, { email, magicLink }, { headers: SATRIAM_HEADERS, timeout: 20000 });
  if (!res.data || !res.data.success) throw new Error(`Verifikasi gagal: ${JSON.stringify(res.data).slice(0, 300)}`);
  return res.data;
}

const cancelKeyboard = Markup.inlineKeyboard([[Markup.button.callback('❌ Batal', 'back_menu')]]);

// ================= AI CHAT (kyzznekoo - claude haiku 4.5) =================
const AI_URL = process.env.AI_URL || 'https://app.kyzznekoo.my.id/api/ai/claude-haiku-4.5-landing';
const AI_LABEL = process.env.AI_LABEL || 'Claude Haiku 4.5';
const AI_APIKEY = clean(process.env.AI_APIKEY);

// format respon API belum pasti -> coba beberapa kemungkinan field
function extractAiText(d) {
  if (d == null) return '';
  if (typeof d === 'string') return d;
  const tryKeys = ['result', 'response', 'answer', 'message', 'text', 'content', 'output', 'reply', 'data'];
  for (const k of tryKeys) {
    if (d[k] != null) {
      const v = extractAiText(d[k]);
      if (v) return v;
    }
  }
  if (Array.isArray(d.choices) && d.choices[0]) {
    const c = d.choices[0];
    return extractAiText((c.message && c.message.content) || c.text || '');
  }
  return '';
}

async function askAI(prompt) {
  if (!AI_APIKEY) throw new Error('AI_APIKEY belum diisi di .env');
  const res = await axios.post(AI_URL, { prompt }, {
    headers: {
      'User-Agent': 'Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/155.0.0.0 Mobile Safari/537.36',
      'Content-Type': 'application/json',
      'X-apikey': AI_APIKEY
    },
    timeout: 90000
  });
  const text = extractAiText(res.data).trim();
  if (!text) {
    console.log('[ai raw]', JSON.stringify(res.data).slice(0, 800));
    return typeof res.data === 'string' ? res.data : JSON.stringify(res.data, null, 2);
  }
  return text;
}

// pecah teks panjang jadi potongan <= 4000 karakter (batas Telegram 4096)
function chunkText(t, size = 4000) {
  const out = [];
  while (t.length > size) {
    let cut = t.lastIndexOf('\n', size);
    if (cut < size * 0.5) cut = size;
    out.push(t.slice(0, cut));
    t = t.slice(cut).replace(/^\n/, '');
  }
  if (t) out.push(t);
  return out;
}

// ---------- AI Chat: memori obrolan (API hanya menerima 1 prompt, jadi riwayat disisipkan ke prompt) ----------
const AI_HISTORY_TURNS = 6;      // jumlah tanya-jawab terakhir yang diingat
const AI_HISTORY_MAX_CHARS = 6000;

function buildAiPrompt(history, question) {
  if (!history || !history.length) return question;
  let ctxText = '';
  for (const h of history) ctxText += `User: ${h.q}\nAI: ${h.a}\n\n`;
  if (ctxText.length > AI_HISTORY_MAX_CHARS) ctxText = ctxText.slice(-AI_HISTORY_MAX_CHARS);
  return `Riwayat percakapan sebelumnya:\n${ctxText}Lanjutkan percakapan. Jawab pesan terbaru berikut:\n${question}`;
}

async function askAiChat(session, question, rawPrompt) {
  if (!Array.isArray(session.aiHistory)) session.aiHistory = [];
  const answer = await askAI(buildAiPrompt(session.aiHistory, question));
  session.aiHistory.push({ q: rawPrompt.slice(0, 1500), a: answer.slice(0, 1500) });
  if (session.aiHistory.length > AI_HISTORY_TURNS) session.aiHistory.shift();
  return answer;
}

const aiKeyboard = Markup.inlineKeyboard([[Markup.button.callback('🆕 Chat Baru', 'ai_new'), Markup.button.callback('⬅️ Menu', 'back_menu')]]);

// ---------- AI Chat: kirim kode sebagai file ----------
const LANG_EXT = {
  javascript: 'js', js: 'js', node: 'js', nodejs: 'js', jsx: 'jsx', typescript: 'ts', ts: 'ts', tsx: 'tsx',
  css: 'css', scss: 'scss', html: 'html', xml: 'xml', json: 'json', yaml: 'yml', yml: 'yml', md: 'md', markdown: 'md',
  python: 'py', py: 'py', php: 'php', java: 'java', kotlin: 'kt', c: 'c', cpp: 'cpp', 'c++': 'cpp', csharp: 'cs', cs: 'cs',
  go: 'go', rust: 'rs', ruby: 'rb', swift: 'swift', sql: 'sql', bash: 'sh', sh: 'sh', shell: 'sh', zsh: 'sh',
  powershell: 'ps1', lua: 'lua', dart: 'dart', vue: 'vue', svelte: 'svelte', ini: 'ini', toml: 'toml',
  env: 'env', dockerfile: 'Dockerfile', txt: 'txt', text: 'txt', plaintext: 'txt'
};
function safeFilename(name) {
  return String(name || '').replace(/\\/g, '/').split('/').pop().replace(/[^\w.\-]+/g, '_').replace(/^\.+/, '').slice(0, 80);
}

// ambil blok ```lang:nama.ext ... ``` dari jawaban AI
function extractCodeFiles(answer) {
  const files = [];
  const used = new Set();
  const re = /```[ \t]*([^\n`]*)\n([\s\S]*?)```/g;
  let text = '';
  let last = 0;
  let m;
  let idx = 0;
  while ((m = re.exec(answer))) {
    const info = m[1].trim();
    let code = m[2].replace(/\s+$/, '');
    if (!code.trim()) continue;
    idx++;

    let lang = '';
    let name = '';
    const parts = info.split(/[\s:]+/).filter(Boolean);
    for (const part of parts) {
      if (/\.[A-Za-z0-9]{1,10}$/.test(part) || /^(Dockerfile|Makefile)$/i.test(part)) name = safeFilename(part);
      else if (!lang) lang = part.toLowerCase();
    }
    // nama file dari komentar baris pertama: // index.js, /* style.css */, <!-- index.html -->, # app.py
    if (!name) {
      const first = code.split('\n')[0];
      const c = first.match(/^\s*(?:\/\/|#|\/\*|<!--|--)\s*(?:file(?:name)?\s*:?\s*)?([\w\-./]+\.[A-Za-z0-9]{1,10})\s*(?:\*\/|-->)?\s*$/i);
      if (c) name = safeFilename(c[1]);
    }
    if (!name) {
      const ext = LANG_EXT[lang] || (lang && /^[a-z0-9]{1,6}$/.test(lang) ? lang : 'txt');
      name = ext === 'Dockerfile' ? 'Dockerfile' : `kode-${idx}.${ext}`;
    }
    if (!name) name = `kode-${idx}.txt`;
    while (used.has(name.toLowerCase())) {
      const dot = name.lastIndexOf('.');
      name = dot > 0 ? `${name.slice(0, dot)}-${idx}${name.slice(dot)}` : `${name}-${idx}`;
    }
    used.add(name.toLowerCase());

    files.push({ name, code, lines: code.split('\n').length });
    text += answer.slice(last, m.index) + `📎 ${name}`;
    last = m.index + m[0].length;
  }
  text += answer.slice(last);
  return { files, text: text.trim() };
}

// kirim jawaban AI Chat sebagai teks biasa (AI Chat hanya untuk ngobrol)
async function sendChatReply(ctx, answer) {
  const parts = chunkText(answer);
  for (let i = 0; i < parts.length; i++) {
    await ctx.reply(parts[i], i === parts.length - 1 ? aiKeyboard : undefined);
  }
}

// ---------- AI Coder (khusus bikin kode, hasil dikirim sebagai file) ----------
const CODER_API = process.env.CODER_API || 'https://api.synoxcloud.xyz/ai-chat/ai-coder';
const CODER_KEY = process.env.SYNOX_APIKEY || process.env.VN_APIKEY || 'FREE';
const coderKeyboard = Markup.inlineKeyboard([[Markup.button.callback('🆕 Proyek Baru', 'coder_new'), Markup.button.callback('⬅️ Menu', 'back_menu')]]);

const newCoderId = () => {
  const chars = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
  let id = '';
  for (let i = 0; i < 7; i++) id += chars[Math.floor(Math.random() * chars.length)];
  return id;
};

// cari objek file terstruktur {name, content} di mana pun dalam respons
function findStructuredFiles(obj, out = [], depth = 0) {
  if (!obj || typeof obj !== 'object' || depth > 6) return out;
  if (Array.isArray(obj)) {
    for (const it of obj) {
      if (it && typeof it === 'object' && !Array.isArray(it)) {
        const name = it.filename || it.fileName || it.name || it.path;
        const content = [it.content, it.code, it.text, it.source].find((v) => typeof v === 'string' && v.trim());
        if (typeof name === 'string' && /\.\w{1,10}$/.test(name) && content) {
          out.push({ name: safeFilename(name), code: content, lines: content.split('\n').length });
          continue;
        }
      }
      findStructuredFiles(it, out, depth + 1);
    }
    return out;
  }
  for (const v of Object.values(obj)) findStructuredFiles(v, out, depth + 1);
  return out;
}

function parseCoderResponse(data) {
  let files = findStructuredFiles(data);
  const text = extractAiText(data).trim();
  let rest = text;
  if (!files.length && text) {
    const ex = extractCodeFiles(text);
    files = ex.files;
    rest = ex.text;
    // jawaban berupa dokumen HTML mentah tanpa ```
    if (!files.length && /^\s*(<!doctype html|<html[\s>])/i.test(text)) {
      files = [{ name: 'index.html', code: text, lines: text.split('\n').length }];
      rest = '';
    }
  }
  // hindari nama file kembar
  const used = new Set();
  files = files.map((f, i) => {
    let name = f.name || `kode-${i + 1}.txt`;
    while (used.has(name.toLowerCase())) { const d = name.lastIndexOf('.'); name = d > 0 ? `${name.slice(0, d)}-${i + 1}${name.slice(d)}` : `${name}-${i + 1}`; }
    used.add(name.toLowerCase());
    return { ...f, name };
  });
  return { files, text: rest };
}

async function sendCoderResult(ctx, parsed) {
  const { files, text } = parsed;
  if (!files.length) {
    const parts = chunkText(text || 'AI tidak mengembalikan kode.');
    for (let i = 0; i < parts.length; i++) await ctx.reply(parts[i], i === parts.length - 1 ? coderKeyboard : undefined);
    return;
  }
  if (text) for (const part of chunkText(text)) await ctx.reply(part);
  await ctx.sendChatAction('upload_document').catch(() => {});
  for (let i = 0; i < files.length; i++) {
    const f = files[i];
    await ctx.replyWithDocument(
      { source: Buffer.from(f.code, 'utf8'), filename: f.name },
      { caption: `📄 ${f.name} (${f.lines} baris)`, ...(i === files.length - 1 ? coderKeyboard : {}) }
    );
  }
}

async function runCoder(ctx) {
  const prompt = ctx.message.text.replace(/^\/\w+(@\w+)?\s*/i, '').trim();
  if (!prompt) return;
  if (!ctx.session.coderId) ctx.session.coderId = newCoderId();
  const sessionId = ctx.session.coderId;
  const wait = await ctx.reply('⌨️ AI Coder sedang menulis kode, bisa sampai 1-3 menit...');
  // dijalankan terpisah supaya tidak kena handlerTimeout Telegraf (90 detik)
  (async () => {
    await ctx.sendChatAction('typing').catch(() => {});
    const typing = setInterval(() => ctx.sendChatAction('typing').catch(() => {}), 4000);
    try {
      const { data } = await axios.get(CODER_API, {
        params: { prompt, session: sessionId, apikey: CODER_KEY },
        timeout: 240000
      });
      clearInterval(typing);
      if (data && typeof data === 'object' && data.status === false) {
        throw new Error(data.message || data.error || 'AI Coder menolak permintaan');
      }
      const parsed = parseCoderResponse(data);
      if (!parsed.files.length && !parsed.text) {
        console.log('[coder raw]', JSON.stringify(data).slice(0, 800));
        throw new Error((data && (data.message || data.error)) || 'Respons kosong dari AI Coder');
      }
      await ctx.deleteMessage(wait.message_id).catch(() => {});
      await sendCoderResult(ctx, parsed);
    } catch (e) {
      clearInterval(typing);
      console.error('[coder]', apiError(e));
      await ctx.telegram.editMessageText(ctx.chat.id, wait.message_id, null,
        '❌ AI Coder error: ' + apiError(e).slice(0, 400), coderKeyboard).catch(() => {});
    }
  })();
}



// ================= FAKE ML (kyzznekoo canvas) =================
const FML_URL = process.env.FML_URL || 'https://app.kyzznekoo.my.id/api/canvas/fakeml';
const FML_RANKS = [
  ['epic', 'Epic'], ['legend', 'Legend'], ['gm', 'Grandmaster'],
  ['honor', 'Honor'], ['glory', 'Glory'], ['imo', 'Immortal'], ['mawi', 'Mawi']
];
const FML_MAX_BORDER = 16; // 0 = outline default, 1..16 = border

const rowsOf = (arr, n) => {
  const rows = [];
  for (let i = 0; i < arr.length; i += n) rows.push(arr.slice(i, i + n));
  return rows;
};

const fmlRankKeyboard = () => Markup.inlineKeyboard([
  ...rowsOf(FML_RANKS.map(([k, label]) => Markup.button.callback(label, `fml_r_${k}`)), 2),
  [Markup.button.callback('❌ Batal', 'back_menu')]
]);

const fmlBorderKeyboard = () => Markup.inlineKeyboard([
  ...rowsOf(
    Array.from({ length: FML_MAX_BORDER + 1 }, (_, i) =>
      Markup.button.callback(i === 0 ? '0 (Default)' : String(i), `fml_b_${i}`)),
    4
  ),
  [Markup.button.callback('⬅️ Ganti Rank', 'fml_again'), Markup.button.callback('❌ Batal', 'back_menu')]
]);

const fmlResultKeyboard = Markup.inlineKeyboard([
  [Markup.button.callback('🔄 Ganti Rank/Border', 'fml_again'), Markup.button.callback('🆕 Buat Baru', 'fake_ml')],
  [Markup.button.callback('⬅️ Menu', 'back_menu')]
]);

// edit pesan teks; kalau pesannya foto / tidak bisa diedit, kirim pesan baru
async function editOrReply(ctx, text, kb) {
  try {
    await ctx.editMessageText(text, { parse_mode: 'HTML', disable_web_page_preview: true, ...kb });
  } catch (e) {
    if (/not modified/i.test(e.message)) return;
    await ctx.reply(text, { parse_mode: 'HTML', disable_web_page_preview: true, ...kb });
  }
}

// API bisa mengembalikan gambar langsung, atau JSON berisi url/base64/buffer
function extractFmlImage(buf, contentType) {
  if (/^image\//i.test(contentType)) return { source: buf };
  let json;
  try { json = JSON.parse(buf.toString('utf8')); } catch (_) { return null; }
  const urls = collectUrls(json);
  if (urls.length) return { url: urls[0].url };
  const walk = (o) => {
    if (!o || typeof o !== 'object') return null;
    if (o.type === 'Buffer' && Array.isArray(o.data)) return { source: Buffer.from(o.data) };
    for (const v of Object.values(o)) {
      if (typeof v === 'string' && v.length > 200 && /^[A-Za-z0-9+/=\s]+$/.test(v.replace(/^data:[^,]+,/, ''))) {
        return { source: Buffer.from(v.replace(/^data:[^,]+,/, ''), 'base64') };
      }
      const r = walk(v);
      if (r) return r;
    }
    return null;
  };
  return walk(json);
}

async function generateFakeMl({ avatar, username, rank, border }) {
  if (!AI_APIKEY) throw new Error('AI_APIKEY belum diisi di .env');
  const params = { username, rank, border };
  if (avatar) params.avatar = avatar; // kosong = pakai avatar default API
  const res = await axios.get(FML_URL, {
    params,
    headers: {
      'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/155.0.0.0 Safari/537.36',
      'Content-Type': 'application/json',
      'X-apikey': AI_APIKEY
    },
    responseType: 'arraybuffer',
    timeout: 60000
  });
  const buf = Buffer.from(res.data);
  const img = extractFmlImage(buf, res.headers['content-type'] || '');
  if (!img) {
    console.log('[fakeml raw]', buf.toString('utf8').slice(0, 500));
    throw new Error('Respon API tidak berisi gambar');
  }
  return img;
}

const mainMenu = Markup.inlineKeyboard([
  [Markup.button.callback('🔍 Find Preset AM', 'find_preset')],
  [Markup.button.callback('📧 Temp Mail', 'tempmail'), Markup.button.callback('⭐ AM Premium', 'am_premium')],
  [Markup.button.callback('🤖 AI Chat', 'ai_chat'), Markup.button.callback('💻 AI Coder', 'ai_coder')],
  [Markup.button.callback('🎮 Fake ML', 'fake_ml'), Markup.button.callback('🎵 Play Musik', 'flow_play')],
  [Markup.button.callback('🖼 HD Foto', 'flow_hd'), Markup.button.callback('📸 Instagram', 'flow_ig')],
  [Markup.button.callback('📘 Facebook', 'flow_fb'), Markup.button.callback('🎬 TikTok', 'flow_tiktok')],
  [Markup.button.callback('🎧 Spotify', 'flow_spotify'), Markup.button.callback('📦 Terabox', 'flow_terabox')],
  [Markup.button.callback('🎨 Buat Gambar', 'flow_imagine'), Markup.button.callback('📱 Nomor Virtual', 'vn_menu')],
  [Markup.button.callback('🪪 ID Card', 'wiz_idcard'), Markup.button.callback('🎵 Spotify Card', 'wiz_spcard')],
  [Markup.button.callback('📖 Bantuan', 'help'), Markup.button.callback('👤 Profil', 'profile')]
]);

bot.start(async (ctx) => {
  if (ctx.from.id.toString() !== ADMIN_ID) {
    return ctx.reply('❌ Bot ini hanya untuk admin.');
  }
  
  const { sent, interval } = await showLoadingAnimation(ctx, 'Memuat menu...');
  await new Promise(r => setTimeout(r, 800));
  clearInterval(interval);
  
  await ctx.telegram.editMessageText(
    ctx.chat.id,
    sent.message_id,
    null,
    `<b>👋 Hai bosq! Selamat datang di AM Preset Bot</b>\n\n` +
    `Bot ini bisa cari preset Alight Motion dari link TikTok/YouTube.\n` +
    `Pilih menu di bawah ya:`,
    { parse_mode: 'HTML', ...mainMenu }
  );
});

bot.action('find_preset', async (ctx) => {
  if (ctx.from.id.toString() !== ADMIN_ID) return ctx.answerCbQuery('❌ Akses ditolak');
  await ctx.answerCbQuery();
  await ctx.editMessageText(
    `<b>🔍 Find Preset Alight Motion</b>\n\n` +
    `Kirim link TikTok/YouTube yang mau dicari presetnya.\n` +
    `Contoh: <code>https://vt.tiktok.com/xxx</code> atau <code>https://youtu.be/xxx</code>`,
    { parse_mode: 'HTML', ...Markup.inlineKeyboard([[Markup.button.callback('⬅️ Kembali', 'back_menu')]]) }
  );
  ctx.session.waitingForUrl = true;
});

bot.action('back_menu', async (ctx) => {
  if (ctx.from.id.toString() !== ADMIN_ID) return ctx.answerCbQuery('❌ Akses ditolak');
  await ctx.answerCbQuery();
  ctx.session.waitingForUrl = false;
  ctx.session.step = null;
  await ctx.editMessageText(
    `<b>👋 Hai bosq! Selamat datang di AM Preset Bot</b>\n\n` +
    `Bot ini bisa cari preset Alight Motion dari link TikTok/YouTube.\n` +
    `Pilih menu di bawah ya:`,
    { parse_mode: 'HTML', ...mainMenu }
  );
});

function helpText() {
  return (
    `<b>📖 Bantuan</b>\n\n` +
    `1. Tekan <b>Find Preset AM</b>\n` +
    `2. Kirim link TikTok/YouTube\n` +
    `3. Tunggu proses scraping\n` +
    `4. Dapatkan link Alight Creative & XML\n\n` +
    `Format link yang didukung:\n` +
    `• TikTok: vt.tiktok.com, tiktok.com\n` +
    `• YouTube: youtu.be, youtube.com\n\n` +
    `<b>📧 Temp Mail</b>: buat email sementara & baca inbox-nya.\n` +
    `<b>🎵 Play Musik</b>: kirim judul/link YouTube, bot kirim audionya.\n` +
    `<b>🖼 HD Foto</b>: kirim foto, bot upscale jadi HD.\n` +
    `<b>📸 Instagram</b>: download foto/video/reel (HD jika tersedia).\n` +
    `<b>🎬 TikTok</b>: download video atau slideshow foto, plus link MP3-nya.\n` +
    `<b>🎧 Spotify</b>: kirim link lagu, pilih MP3 atau cover.\n` +
    `<b>🪪 ID Card</b> &amp; <b>🎵 Spotify Card</b>: buat kartu gambar, bot menanyakan datanya satu per satu.\n` +
    `<b>📘 Facebook</b>: download video lengkap dengan metadata (butuh <code>npm i cheerio</code>).\n` +
    `<b>📦 Terabox</b>: ambil link download HD &amp; stream.\n` +
    `<b>🎨 Buat Gambar</b>: gambar dari teks, khusus SFW (butuh <code>npm i crypto-js</code>).\n` +
    `<b>📱 Nomor Virtual</b>: daftar nomor publik per negara, bisa ketik nama negara.\n` +
    `<b>🎮 Fake ML</b>: buat kartu profil Mobile Legends palsu (username, avatar, rank, border).\n` +
    `<b>🤖 AI Chat</b>: ngobrol biasa dengan AI, ingat obrolan sampai kamu tekan Chat Baru.\n` +
    `<b>💻 AI Coder</b>: khusus bikin kode, hasilnya dikirim sebagai file (.html/.css/.js, dll) dan bisa direvisi.\n` +
    `<b>⭐ AM Premium</b>: login Satriam lewat magic link (kirim email → tempel link dari inbox).\n\n` +
    `<b>⚡ Command cepat</b> (tanpa isi = buka mode input)\n` +
    `/play judul • /ig /fb /tiktok /spotify /terabox link • /hd (reply foto)\n` +
    `/imagine prompt • /nomor negara • /ai pesan • /coder perintah\n` +
    `/idcard nama | jabatan | teks | kontak\n` +
    `/spcard judul | artis | cover • /menu • /help\n\n` +
    `Kalau error, coba lagi atau hubungi admin.`
  );
}

bot.action('help', async (ctx) => {
  if (ctx.from.id.toString() !== ADMIN_ID) return ctx.answerCbQuery('❌ Akses ditolak');
  await ctx.answerCbQuery();
  await ctx.editMessageText(
    helpText(),
    { parse_mode: 'HTML', ...Markup.inlineKeyboard([[Markup.button.callback('⬅️ Kembali', 'back_menu')]]) }
  );
});

bot.action('profile', async (ctx) => {
  if (ctx.from.id.toString() !== ADMIN_ID) return ctx.answerCbQuery('❌ Akses ditolak');
  await ctx.answerCbQuery();
  await ctx.editMessageText(
    `<b>👤 Profil Admin</b>\n\n` +
    `ID: <code>${ctx.from.id}</code>\n` +
    `Nama: ${ctx.from.first_name}${ctx.from.last_name ? ' ' + ctx.from.last_name : ''}\n` +
    `Username: @${ctx.from.username || '-'}\n` +
    `Bot: @${ctx.botInfo.username}`,
    { parse_mode: 'HTML', ...Markup.inlineKeyboard([[Markup.button.callback('⬅️ Kembali', 'back_menu')]]) }
  );
});

bot.action('tempmail', async (ctx) => {
  if (ctx.from.id.toString() !== ADMIN_ID) return ctx.answerCbQuery('❌ Akses ditolak');
  await ctx.answerCbQuery('Membuat email...');
  ctx.session.step = null;
  try {
    if (!ctx.session.temp) ctx.session.temp = newTempSession();
    if (!ctx.session.temp.email) await tempCreate(ctx.session.temp);
    await safeEdit(ctx,
      `<b>📧 Temp Mail</b>\n<code>${escapeHtml(ctx.session.temp.email)}</code>\n\nPakai email ini, lalu tekan <b>Cek Inbox</b>.`,
      tempKeyboard);
  } catch (e) {
    console.error('[tempmail]', apiError(e));
    await safeEdit(ctx, `❌ <b>Gagal membuat email</b>\n\n<code>${escapeHtml(apiError(e))}</code>`, tempKeyboard);
  }
});

bot.action('tm_new', async (ctx) => {
  if (ctx.from.id.toString() !== ADMIN_ID) return ctx.answerCbQuery('❌ Akses ditolak');
  await ctx.answerCbQuery('Membuat email baru...');
  try {
    ctx.session.temp = newTempSession();
    await tempCreate(ctx.session.temp);
    await safeEdit(ctx,
      `<b>📧 Temp Mail (baru)</b>\n<code>${escapeHtml(ctx.session.temp.email)}</code>\n\nPakai email ini, lalu tekan <b>Cek Inbox</b>.`,
      tempKeyboard);
  } catch (e) {
    console.error('[tm_new]', apiError(e));
    await safeEdit(ctx, `❌ <b>Gagal membuat email</b>\n\n<code>${escapeHtml(apiError(e))}</code>`, tempKeyboard);
  }
});

bot.action('tm_check', async (ctx) => {
  if (ctx.from.id.toString() !== ADMIN_ID) return ctx.answerCbQuery('❌ Akses ditolak');
  const sess = ctx.session.temp;
  if (!sess || !sess.email) {
    await ctx.answerCbQuery('Buat email dulu');
    return safeEdit(ctx, '❌ Belum ada email. Tekan <b>Email Baru</b>.', tempKeyboard);
  }
  await ctx.answerCbQuery('Mengecek inbox...');
  try {
    const { messages, raw } = await tempInbox(sess);
    await safeEdit(ctx, formatInbox(sess.email, messages, raw), tempKeyboard);
  } catch (e) {
    console.error('[tm_check]', apiError(e));
    await safeEdit(ctx, `❌ <b>Gagal cek inbox</b>\n\n<code>${escapeHtml(apiError(e))}</code>`, tempKeyboard);
  }
});

const aiIntro = () =>
  `<b>🤖 AI Chat</b> (${escapeHtml(AI_LABEL)})\n\nKirim pertanyaan/prompt apa saja, nanti dijawab AI.\n` +
  `Menu ini khusus ngobrol. Untuk minta dibuatkan kode, pakai <b>💻 AI Coder</b>.\n` +
  `AI <b>ingat obrolan</b> selama sesi berjalan. Tekan <b>Chat Baru</b> untuk mulai dari nol, atau <b>Menu</b> untuk keluar.` +
  (AI_APIKEY ? '' : `\n\n⚠️ <b>AI_APIKEY</b> belum diisi di .env`);

bot.action('ai_chat', async (ctx) => {
  if (ctx.from.id.toString() !== ADMIN_ID) return ctx.answerCbQuery('❌ Akses ditolak');
  await ctx.answerCbQuery();
  ctx.session.waitingForUrl = false;
  ctx.session.step = 'ai_chat';
  ctx.session.aiHistory = [];
  await safeEdit(ctx, aiIntro(), aiKeyboard);
});

bot.action('ai_new', async (ctx) => {
  if (ctx.from.id.toString() !== ADMIN_ID) return ctx.answerCbQuery('❌ Akses ditolak');
  ctx.session.step = 'ai_chat';
  ctx.session.aiHistory = [];
  await ctx.answerCbQuery('🆕 Obrolan baru dimulai');
  await ctx.reply('🆕 Obrolan baru dimulai. Silakan kirim pertanyaan.', aiKeyboard);
});

const coderIntro = () =>
  `<b>💻 AI Coder</b>\n\nKhusus bikin kode. Jelaskan apa yang mau dibuat, hasilnya dikirim sebagai file (.html, .css, .js, dll).\n` +
  `Contoh: <code>buat landing page portfolio modern</code>\n\n` +
  `AI ingat konteks proyek yang sama, jadi kamu bisa minta revisi (mis. "ganti warna jadi biru"). Tekan <b>Proyek Baru</b> untuk mulai dari nol.`;

bot.action('ai_coder', async (ctx) => {
  if (ctx.from.id.toString() !== ADMIN_ID) return ctx.answerCbQuery('❌ Akses ditolak');
  await ctx.answerCbQuery();
  ctx.session.waitingForUrl = false;
  ctx.session.step = 'ai_coder';
  ctx.session.coderId = newCoderId();
  await safeEdit(ctx, coderIntro(), coderKeyboard);
});

bot.action('coder_new', async (ctx) => {
  if (ctx.from.id.toString() !== ADMIN_ID) return ctx.answerCbQuery('❌ Akses ditolak');
  ctx.session.step = 'ai_coder';
  ctx.session.coderId = newCoderId();
  await ctx.answerCbQuery('🆕 Proyek baru dimulai');
  await ctx.reply('🆕 Proyek baru dimulai. Jelaskan kode yang mau dibuat.', coderKeyboard);
});

bot.action('fake_ml', async (ctx) => {
  if (ctx.from.id.toString() !== ADMIN_ID) return ctx.answerCbQuery('❌ Akses ditolak');
  await ctx.answerCbQuery();
  ctx.session.waitingForUrl = false;
  ctx.session.step = 'fml_username';
  ctx.session.fml = {};
  await editOrReply(ctx,
    `<b>🎮 Fake ML</b>\n\nKirim <b>username</b> (maks 15 karakter).` +
    (AI_APIKEY ? '' : `\n\n⚠️ <b>AI_APIKEY</b> belum diisi di .env`),
    Markup.inlineKeyboard([[Markup.button.callback('❌ Batal', 'back_menu')]]));
});

bot.action('fml_skip_avatar', async (ctx) => {
  if (ctx.from.id.toString() !== ADMIN_ID) return ctx.answerCbQuery('❌ Akses ditolak');
  if (!ctx.session.fml || !ctx.session.fml.username) return ctx.answerCbQuery('Sesi habis, mulai lagi');
  await ctx.answerCbQuery();
  ctx.session.step = null;
  ctx.session.fml.avatar = '';
  await editOrReply(ctx, `<b>🎮 Fake ML</b>\nUsername: <code>${escapeHtml(ctx.session.fml.username)}</code>\nAvatar: default\n\nPilih <b>rank</b>:`, fmlRankKeyboard());
});

bot.action('fml_again', async (ctx) => {
  if (ctx.from.id.toString() !== ADMIN_ID) return ctx.answerCbQuery('❌ Akses ditolak');
  if (!ctx.session.fml || !ctx.session.fml.username) return ctx.answerCbQuery('Sesi habis, tekan Buat Baru');
  await ctx.answerCbQuery();
  ctx.session.step = null;
  await editOrReply(ctx, `<b>🎮 Fake ML</b>\nUsername: <code>${escapeHtml(ctx.session.fml.username)}</code>\n\nPilih <b>rank</b>:`, fmlRankKeyboard());
});

bot.action(/^fml_r_(\w+)$/, async (ctx) => {
  if (ctx.from.id.toString() !== ADMIN_ID) return ctx.answerCbQuery('❌ Akses ditolak');
  const rank = FML_RANKS.find(([k]) => k === ctx.match[1]);
  if (!rank || !ctx.session.fml || !ctx.session.fml.username) return ctx.answerCbQuery('Sesi habis, mulai lagi');
  await ctx.answerCbQuery();
  ctx.session.fml.rank = rank[0];
  await editOrReply(ctx,
    `<b>🎮 Fake ML</b>\nUsername: <code>${escapeHtml(ctx.session.fml.username)}</code>\nRank: ${rank[1]}\n\nPilih <b>border</b>:`,
    fmlBorderKeyboard());
});

bot.action(/^fml_b_(\d+)$/, async (ctx) => {
  if (ctx.from.id.toString() !== ADMIN_ID) return ctx.answerCbQuery('❌ Akses ditolak');
  const border = parseInt(ctx.match[1], 10);
  const f = ctx.session.fml;
  if (!f || !f.username || !f.rank || border > FML_MAX_BORDER) return ctx.answerCbQuery('Sesi habis, mulai lagi');
  await ctx.answerCbQuery('Membuat gambar...');
  f.border = border;
  await editOrReply(ctx, '⏳ Membuat gambar...');
  try {
    await ctx.sendChatAction('upload_photo').catch(() => {});
    const img = await generateFakeMl(f);
    const label = (FML_RANKS.find(([k]) => k === f.rank) || [, f.rank])[1];
    await ctx.replyWithPhoto(img, {
      caption: `🎮 <b>${escapeHtml(f.username)}</b>\nRank: ${escapeHtml(label)} | Border: ${border}`,
      parse_mode: 'HTML',
      ...fmlResultKeyboard
    });
    await ctx.deleteMessage().catch(() => {});
  } catch (e) {
    console.error('[fakeml]', apiError(e));
    await editOrReply(ctx,
      `❌ <b>Gagal membuat gambar</b>\n\n<code>${escapeHtml(apiError(e).slice(0, 500))}</code>`,
      Markup.inlineKeyboard([[Markup.button.callback('🔄 Pilih Ulang', 'fml_again'), Markup.button.callback('⬅️ Menu', 'back_menu')]]));
  }
});

bot.action('tm_wait', async (ctx) => {
  if (ctx.from.id.toString() !== ADMIN_ID) return ctx.answerCbQuery('❌ Akses ditolak');
  const sess = ctx.session.temp;
  if (!sess || !sess.email) return ctx.answerCbQuery('Buat email dulu');
  if (sess.waiting) return ctx.answerCbQuery('Masih menunggu...');
  await ctx.answerCbQuery('Menunggu email masuk...');
  sess.waiting = true;

  // jalan di background (tanpa await) supaya handler lain tidak ikut tertahan
  (async () => {
    const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
    const TOTAL = 20;
    let last = { messages: [], raw: null };
    try {
      for (let i = 1; i <= TOTAL; i++) {
        try { last = await tempInbox(sess); } catch (e) { console.error('[tm_wait]', apiError(e)); }
        if (last.messages.length) {
          await safeEdit(ctx, formatInbox(sess.email, last.messages, last.raw), tempKeyboard);
          return;
        }
        if (i % 3 === 1) {
          await safeEdit(ctx, `<b>📧 Temp Mail</b>\n<code>${escapeHtml(sess.email)}</code>\n\n⏳ Menunggu email masuk... (${i}/${TOTAL})`, tempKeyboard).catch(() => {});
        }
        await sleep(3000);
      }
      await safeEdit(ctx, formatInbox(sess.email, [], last.raw), tempKeyboard);
    } catch (e) {
      console.error('[tm_wait]', e.message);
    } finally {
      sess.waiting = false;
    }
  })();
});

bot.action('am_premium', async (ctx) => {
  if (ctx.from.id.toString() !== ADMIN_ID) return ctx.answerCbQuery('❌ Akses ditolak');
  await ctx.answerCbQuery();
  ctx.session.waitingForUrl = false;
  ctx.session.step = 'am_email';
  await safeEdit(ctx,
    `<b>⭐ AM Premium (Satriam)</b>\n\nKirim <b>email</b> yang mau dipakai login.\n` +
    `Tips: bisa pakai email dari menu 📧 Temp Mail.`,
    cancelKeyboard);
});

// ================= HD FOTO (upscaler 4x, imgupscaler.com) =================
const UP_BASE = 'https://imgupscaler.com';
const UP_SCALE = process.env.UPSCALE_SCALE || '4'; // 2 / 4 / 8 sesuai dukungan situs
const UP_UA = 'Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Mobile Safari/537.36';

// Link file Telegram berisi TOKEN BOT, jadi tidak dikirim ke pihak lain: foto diunduh di sini lalu diunggah langsung.
async function tgFileToBuffer(ctx, fileId) {
  const link = await ctx.telegram.getFileLink(fileId);
  const res = await axios.get(link.href, { responseType: 'arraybuffer', timeout: 60000 });
  return Buffer.from(res.data);
}

const randomIPv4 = () => {
  const r = () => Math.floor(Math.random() * 254) + 1;
  return `${r()}.${r()}.${r()}.${r()}`;
};

async function upscaleBuffer(buffer, mime = 'image/jpeg', scale = UP_SCALE) {
  const ip = randomIPv4();
  const headers = {
    'Origin': UP_BASE,
    'Referer': `${UP_BASE}/`,
    'User-Agent': UP_UA,
    'X-Client-Ipv4': ip,
    'X-Forwarded-For': ip
  };

  const form = new FormData();
  form.append('tool', 'upscaler');
  form.append('mode', 'batch');
  form.append('scaleRadio', String(scale));
  form.append('file', new Blob([buffer], { type: mime }), 'image.jpg');

  const up = await fetch(`${UP_BASE}/api/legacy/upload`, {
    method: 'POST', body: form, headers, signal: AbortSignal.timeout(90000)
  });
  const upText = await up.text();
  let upData = null;
  try { upData = JSON.parse(upText); } catch (_) { /* bukan JSON */ }
  const taskId = upData && upData.taskId;
  if (!taskId) {
    console.log('[hd raw upload]', up.status, upText.slice(0, 400));
    throw new Error('Gagal mendapatkan taskId dari server' + (up.ok ? '' : ` (HTTP ${up.status})`));
  }

  const start = Date.now();
  for (let i = 0; i < 50; i++) {
    await new Promise((r) => setTimeout(r, 2000));
    const { data } = await axios.post(`${UP_BASE}/api/legacy/status`,
      { tool: 'upscaler', taskId, scaleRadio: String(scale) },
      { headers: { 'Content-Type': 'application/json', ...headers }, timeout: 30000 });
    if (data && data.status === 'success' && Array.isArray(data.downloadUrls) && data.downloadUrls.length) {
      return { url: data.downloadUrls[0], taskId, seconds: Math.round((Date.now() - start) / 1000) };
    }
    if (!data || data.status !== 'waiting') {
      console.log('[hd raw status]', JSON.stringify(data).slice(0, 400));
      throw new Error('Proses gagal di server');
    }
  }
  throw new Error('Timeout: proses upscale terlalu lama');
}

async function runHd(ctx, src) {
  if (ctx.from.id.toString() !== ADMIN_ID) return;
  const q = src || ctx.message;
  const isImgDoc = q.document && /^image\//i.test(q.document.mime_type || '');
  const fileId = (q.photo && q.photo[q.photo.length - 1].file_id) || (isImgDoc && q.document.file_id);
  if (!fileId) return ctx.reply('❌ Kirim fotonya (sebagai foto atau file gambar) ya.');
  const mime = isImgDoc ? q.document.mime_type : 'image/jpeg';

  const wait = await ctx.reply(`⏳ Upscale ${UP_SCALE}x sedang diproses, tunggu sebentar...`);
  try {
    await ctx.sendChatAction('upload_photo').catch(() => {});
    const buf = await tgFileToBuffer(ctx, fileId);
    const r = await upscaleBuffer(buf, mime);
    const extra = {
      caption: `✅ <b>Berhasil di-upscale!</b>\n🔍 <b>Skala</b>: ${escapeHtml(UP_SCALE)}x\n⏱️ <b>Proses</b>: ${r.seconds} detik`,
      parse_mode: 'HTML',
      reply_to_message_id: ctx.message.message_id
    };
    try {
      await ctx.replyWithPhoto(r.url, extra);
    } catch (e1) {
      try {
        // hasil 4x sering melewati batas foto Telegram -> kirim sebagai file
        await ctx.replyWithDocument(r.url, extra);
      } catch (e2) {
        console.error('[hd kirim via URL gagal, unduh manual]', e2.message);
        const dl = await axios.get(r.url, { responseType: 'arraybuffer', timeout: 120000 });
        await ctx.replyWithDocument({ source: Buffer.from(dl.data), filename: 'upscaled.jpg' }, extra);
      }
    }
    await ctx.deleteMessage(wait.message_id).catch(() => {});
  } catch (e) {
    console.error('[hd]', apiError(e));
    await ctx.telegram.editMessageText(ctx.chat.id, wait.message_id, null,
      '❌ Terjadi kesalahan: ' + apiError(e).slice(0, 300)).catch(() => {});
  }
}

// ================= /play (YouTube audio) =================
// Fitur axcell
// Fitur SC
const FEATURE_CREDIT = 'Fitur axcell\nFitur SC';

let yts = null;
try { yts = require('yt-search'); } catch (_) { /* wajib: npm i yt-search */ }

// Fallback opsional (ytdl.js milik bot WA). Isi PLAY_YTDL_PATH di .env jika ingin dipakai,
// contoh: PLAY_YTDL_PATH=/root/bot/src/scraper/ytdl.js
const PLAY_YTDL_PATH = clean(process.env.PLAY_YTDL_PATH);
let ytdlMod = null;
async function loadYtdl() {
  if (ytdlMod || !PLAY_YTDL_PATH) return ytdlMod;
  try {
    ytdlMod = await import(require('url').pathToFileURL(PLAY_YTDL_PATH).href);
  } catch (e) {
    console.error('[play ytdl load]', e.message);
  }
  return ytdlMod;
}

const PLAY_API = process.env.PLAY_API || 'https://my.izuka-api.xyz/api/downloader/ytmp3';

const fmtDuration = (sec) => {
  sec = Math.max(0, Math.floor(Number(sec) || 0));
  return `${Math.floor(sec / 60)}:${String(sec % 60).padStart(2, '0')}`;
};

const formatNumber = (num = 0) => {
  num = Number(num) || 0;
  if (num >= 1e9) return (num / 1e9).toFixed(1) + 'B';
  if (num >= 1e6) return (num / 1e6).toFixed(1) + 'M';
  if (num >= 1e3) return (num / 1e3).toFixed(1) + 'K';
  return String(num);
};

async function getAudio(url) {
  try {
    const { data } = await axios.get(PLAY_API, { params: { url }, timeout: 60000 });
    const download = data && data.result && (data.result.download_url || data.result.download);
    if (download) return { download, title: data.result.title };
  } catch (e) {
    console.error('[play api]', e.message);
  }

  const mod = await loadYtdl();
  const ytdl = mod && mod.default;
  if (typeof ytdl === 'function') {
    const fb = await ytdl(url, 'mp3');
    if (fb && fb.status && fb.dl) return { download: fb.dl, title: fb.title, fallback: true };
    throw new Error((fb && fb.mess) || 'Gagal mendapatkan audio');
  }
  throw new Error('Gagal mendapatkan audio');
}

async function downloadAudio(url, fallback = false) {
  const mod = fallback ? await loadYtdl() : null;
  if (fallback && mod && typeof mod.fallbackToMp3Buffer === 'function') {
    return Buffer.from(await mod.fallbackToMp3Buffer(url));
  }

  const res = await axios.get(url, {
    responseType: 'arraybuffer',
    timeout: 120000,
    maxContentLength: 50 * 1024 * 1024, // batas upload bot Telegram
    maxBodyLength: 50 * 1024 * 1024,
  });

  const type = String(res.headers['content-type'] || '').toLowerCase();
  if (type.includes('text/html') || type.includes('application/json')) {
    throw new Error('URL download tidak mengembalikan audio');
  }
  return Buffer.from(res.data);
}

async function findVideo(query) {
  if (!yts) throw new Error('Modul yt-search belum terpasang (npm i yt-search)');
  const m = query.match(/(?:youtu\.be\/|youtube\.com\/(?:watch\?(?:.*&)?v=|shorts\/|embed\/|live\/))([\w-]{11})/i);
  if (m) {
    const v = await yts({ videoId: m[1] });
    if (v) return v;
  }
  const search = await yts(query);
  if (!search.videos || !search.videos.length) throw new Error('Lagu/video tidak ditemukan');
  return search.videos[0];
}

// Fallback kedua (play2): dipakai otomatis jika jalur utama gagal.
// Isi FAA_API di .env (base URL, tanpa slash di akhir), contoh: FAA_API=https://domain-api-kamu
const FAA_API = clean(process.env.FAA_API).replace(/\/+$/, '');

async function playFallback(ctx, query, skipThumb = false) {
  if (!FAA_API) throw new Error('FAA_API belum diisi di .env');

  const { data: json } = await axios.get(`${FAA_API}/faa/ytplay`, { params: { query }, timeout: 90000 });
  if (!json || !json.status || !json.result || !json.result.mp3) throw new Error('API fallback error');
  const d = json.result;

  const caption =
    `🎵 NOW PLAYING\n\n` +
    `📌 Judul: ${d.title || query}\n` +
    `👤 Channel: ${d.author || '-'}\n` +
    `👀 Views: ${d.views != null ? formatNumber(d.views) : '-'}\n\n` +
    `🔗 YouTube: ${d.url || '-'}`;

  await ctx.sendChatAction('upload_voice').catch(() => {});
  if (d.thumbnail && !skipThumb) {
    await ctx.replyWithPhoto(d.thumbnail, { caption: caption.slice(0, 1000) })
      .catch((e) => console.error('[play2 thumb]', e.message));
  }

  const title = String(d.title || query);
  const opts = { title: title.slice(0, 100), performer: d.author ? String(d.author) : undefined };
  try {
    await ctx.replyWithAudio(d.mp3, opts);
  } catch (e) {
    console.error('[play2 url gagal, unduh manual]', e.message);
    const dl = await axios.get(d.mp3, { responseType: 'arraybuffer', timeout: 120000, maxContentLength: 50 * 1024 * 1024 });
    const filename = title.replace(/[\\/:*?"<>|]/g, '_').trim().slice(0, 80) || 'audio';
    await ctx.replyWithAudio({ source: Buffer.from(dl.data), filename: `${filename}.mp3` }, opts);
  }
}

async function runPlay(ctx) {
  if (ctx.from.id.toString() !== ADMIN_ID) return;
  const query = ctx.message.text.replace(/^\/\w+(@\w+)?\s*/i, '').trim();
  if (!query) return ctx.reply('🎵 PLAY\n\nKirim judul lagu atau link YouTube.\nContoh: komang');

  const wait = await ctx.reply('🔎 Mencari lagu...');
  let thumbSent = false;
  try {
    const video = await findVideo(query);
    const result = await getAudio(video.url);
    const audio = await downloadAudio(result.download, result.fallback);

    const caption =
      `🎵 NOW PLAYING\n\n` +
      `📌 Judul: ${video.title}\n` +
      `👤 Channel: ${(video.author && video.author.name) || '-'}\n` +
      `⏱️ Durasi: ${video.timestamp || '-'}\n` +
      `👀 Views: ${formatNumber(video.views)}\n` +
      `📅 Upload: ${video.ago || '-'}\n\n` +
      `🔗 YouTube: ${video.url}`;

    await ctx.sendChatAction('upload_voice').catch(() => {});
    if (video.thumbnail) {
      await ctx.replyWithPhoto(video.thumbnail, { caption: caption.slice(0, 1000) })
        .then(() => { thumbSent = true; })
        .catch((e) => console.error('[play thumb]', e.message));
    }

    const filename = String(video.title).replace(/[\\/:*?"<>|]/g, '_').trim().slice(0, 80) || 'audio';
    await ctx.replyWithAudio(
      { source: audio, filename: `${filename}.mp3` },
      {
        title: String(video.title).slice(0, 100),
        performer: (video.author && video.author.name) || undefined,
        duration: Math.floor(Number(video.seconds) || 0) || undefined,
      }
    );
    await ctx.deleteMessage(wait.message_id).catch(() => {});
  } catch (e) {
    console.error('[play]', apiError(e));
    await ctx.telegram.editMessageText(ctx.chat.id, wait.message_id, null,
      '⚠️ Jalur utama gagal, mencoba jalur cadangan...').catch(() => {});
    try {
      await playFallback(ctx, query, thumbSent);
      await ctx.deleteMessage(wait.message_id).catch(() => {});
    } catch (e2) {
      console.error('[play2]', apiError(e2));
      await ctx.telegram.editMessageText(ctx.chat.id, wait.message_id, null,
        '❌ PLAY GAGAL\n\n' + String(apiError(e) || 'Gagal mengambil audio.').slice(0, 200) +
        '\n\nCadangan: ' + String(apiError(e2)).slice(0, 150)).catch(() => {});
    }
  }
}

// ================= /imagine (text to image, live3d) =================
let CryptoJS = null;
try { CryptoJS = require('crypto-js'); } catch (_) { /* wajib: npm i crypto-js */ }

const L3D = {
  PUBLIC_KEY: `-----BEGIN PUBLIC KEY-----
MIGfMA0GCSqGSIb3DQEBAQUAA4GNADCBiQKBgQCwlO+boC6cwRo3UfXVBadaYwcX
0zKS2fuVNY2qZ0dgwb1NJ+/Q9FeAosL4ONiosD71on3PVYqRUlL5045mvH2K9i8b
AFVMEip7E6RMK6tKAAif7xzZrXnP1GZ5Rijtqdgwh+YmzTo39cuBCsZqK9oEoeQ3
r/myG9S+9cR5huTuFQIDAQAB
-----END PUBLIC KEY-----`,
  APP_ID: 'aifaceswap',
  U_ID: '1H5tRtzsBkqXcaJ',
  FN_NAME: 'demo-ai-body-v1',
  BRAND_KEY: '8f3f0c7387123ae0',
  BASE: 'https://app-v1.live3d.io',
  IMG_BASE: 'https://temp.live3d.io/',
  THEME: '83EmcUoQTUv50LhNx0VrdcK8rcGexcP35FcZDcpgWsAXEyO4xqL5shCY6sFIWB2Q',
  MODEL: 'AbsoluteReality_v1.8.1.safetensors'
};
// bot ini hanya untuk gambar SFW: prompt eksplisit ditolak & negative prompt selalu menyertakan filter
const IMAGINE_BLOCK_RE = /\b(nude|nudes|nudity|naked|nsfw|porn\w*|hentai|topless|bugil|telanjang|ngentot|sex\w*|explicit|erotic\w*|genital\w*|nipple\w*|undress\w*)\b/i;
const IMAGINE_BASE_NEG = '(worst quality, low quality:1.4), deformed, ugly, bad anatomy, extra limbs, nsfw, nude, nudity';

function l3dRandom(len) {
  const chars = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
  let out = '';
  for (let i = 0; i < len; i++) out += chars.charAt(Math.floor(Math.random() * chars.length));
  return out;
}
function l3dAes(data, key) {
  const k = CryptoJS.enc.Utf8.parse(key);
  return CryptoJS.AES.encrypt(data, k, { iv: k, mode: CryptoJS.mode.CBC, padding: CryptoJS.pad.Pkcs7 }).toString();
}
function l3dRsa(data) {
  return crypto.publicEncrypt(
    { key: L3D.PUBLIC_KEY, padding: crypto.constants.RSA_PKCS1_PADDING },
    Buffer.from(data, 'utf8')
  ).toString('base64');
}
function l3dHeaders(type, fp = null) {
  const e = new Date();
  const n = Math.floor(new Date(e.getUTCFullYear(), e.getUTCMonth(), e.getUTCDate(), e.getUTCHours(), e.getUTCMinutes(), e.getUTCSeconds()).getTime() / 1000);
  const r = crypto.randomUUID();
  const i = l3dRandom(16);
  const fingerPrint = fp || crypto.randomBytes(16).toString('hex');
  const s = l3dRsa(i);
  const signStr = type === 'upload' ? `${L3D.APP_ID}:${r}:${s}` : `${L3D.APP_ID}:${L3D.U_ID}:${n}:${r}:${s}`;
  return {
    fp: fingerPrint,
    fp1: l3dAes(`${L3D.APP_ID}:${fingerPrint}`, i),
    'x-guide': s,
    'x-sign': l3dAes(signStr, i),
    'x-code': Date.now().toString()
  };
}

async function l3dCreate(prompt, negative) {
  const h = l3dHeaders('create');
  const payload = {
    fn_name: L3D.FN_NAME, call_type: 3, data: '',
    input: { cfg: 7, lora: [], model: L3D.MODEL, negative_prompt: negative, prompt, request_from: 9 },
    origin_from: L3D.BRAND_KEY, request_from: 9
  };
  const res = await axios.post(`${L3D.BASE}/aitools/of/create`, payload, {
    headers: { 'User-Agent': UA, 'theme-version': L3D.THEME, ...h }, timeout: 30000
  });
  if (res.data.code !== 200) throw new Error(res.data.message || 'Gagal membuat job');
  return { taskId: res.data.data.task_id, fp: h.fp };
}

async function l3dCheck(taskId, fp) {
  const h = l3dHeaders('check', fp);
  const res = await axios.post(`${L3D.BASE}/aitools/of/check-status`, {
    task_id: taskId, fn_name: L3D.FN_NAME, call_type: 3, request_from: 9, origin_from: L3D.BRAND_KEY
  }, { headers: { 'User-Agent': UA, 'theme-version': L3D.THEME, ...h }, timeout: 30000 });
  return res.data.data;
}

async function generateImagine(prompt, userNegative) {
  const negative = userNegative ? `${IMAGINE_BASE_NEG}, ${userNegative}` : IMAGINE_BASE_NEG;
  const { taskId, fp } = await l3dCreate(prompt, negative);
  for (let i = 0; i < 30; i++) {
    await new Promise((r) => setTimeout(r, 5000));
    const r = await l3dCheck(taskId, fp);
    if (r && r.status === 2 && r.result_image) return L3D.IMG_BASE + r.result_image;
    if (r && r.status === 3) throw new Error('Task gagal / diblokir filter keamanan');
  }
  throw new Error('Timeout menunggu hasil gambar');
}

async function runImagine(ctx) {
  if (ctx.from.id.toString() !== ADMIN_ID) return;
  const raw = ctx.message.text.replace(/^\/\w+(@\w+)?\s*/i, '').trim();
  if (!raw) {
    return ctx.reply('Kirim deskripsi gambar.\nContoh: (masterpiece), 1girl, kimono, cherry blossom\n\nNegative prompt (opsional) pakai tanda |\nprompt | negative prompt');
  }
  if (!CryptoJS) return ctx.reply('❌ Modul crypto-js belum terpasang. Jalankan: npm i crypto-js');

  const [prompt, ...negParts] = raw.split('|');
  const userNeg = negParts.join('|').trim();
  if (!prompt.trim()) return ctx.reply('❌ Prompt kosong.');
  if (IMAGINE_BLOCK_RE.test(prompt)) {
    return ctx.reply('❌ Prompt ditolak: fitur ini hanya untuk gambar non-eksplisit (SFW).');
  }

  const wait = await ctx.reply('🎨 Membuat gambar, tunggu sekitar 15-60 detik...');
  // dijalankan terpisah supaya tidak kena handlerTimeout Telegraf (90 detik)
  (async () => {
    try {
      const start = Date.now();
      await ctx.sendChatAction('upload_photo').catch(() => {});
      const url = await generateImagine(prompt.trim(), userNeg);
      await ctx.replyWithPhoto(url, {
        caption: `🎨 ${prompt.trim().slice(0, 900)}\n⏱️ ${((Date.now() - start) / 1000).toFixed(1)} detik`
      }).catch(async (e) => {
        console.error('[imagine url gagal, coba upload manual]', e.message);
        const dl = await axios.get(url, { responseType: 'arraybuffer', timeout: 60000 });
        await ctx.replyWithPhoto({ source: Buffer.from(dl.data) }, { caption: `🎨 ${prompt.trim().slice(0, 900)}` });
      });
      await ctx.deleteMessage(wait.message_id).catch(() => {});
    } catch (e) {
      console.error('[imagine]', apiError(e));
      await ctx.telegram.editMessageText(ctx.chat.id, wait.message_id, null,
        '❌ Gagal membuat gambar.\n' + apiError(e).slice(0, 200)).catch(() => {});
    }
  })();
}

// ================= /terabox (playterabox) =================
const TERA_SALT = 'T9do@SM1?xGn5';
const TERA_PATH = '/api/stream.php';
const TERA_MAX_FILES = 5;          // maksimal file yang ditampilkan per link

function teraToken(ts) {
  return crypto.createHash('md5').update(TERA_SALT + ts + TERA_PATH).digest('hex');
}

async function fetchTerabox(teraUrl) {
  const ts = Math.floor(Date.now() / 1000);
  const endpoint = `https://playterabox.com/api/fetch-video?token=${teraToken(ts)}&t=${ts}`;
  const { data } = await axios.post(endpoint, { url: teraUrl }, {
    headers: {
      'Accept': '*/*',
      'Content-Type': 'application/json',
      'Origin': 'https://playterabox.com',
      'Referer': 'https://playterabox.com/',
      'User-Agent': UA
    },
    timeout: 30000
  });
  if (!data || data.status !== 'success' || !Array.isArray(data.list)) {
    const err = new Error((data && data.message) || 'Gagal mengekstrak file.');
    err.noRetry = true;
    throw err;
  }
  return {
    totalFiles: data.total_files,
    totalFolders: data.total_folders,
    files: data.list.map((f) => ({
      name: f.name,
      size: f.size_formatted,
      sizeBytes: Number(f.size) || 0,
      duration: f.duration,
      quality: f.quality,
      thumbnail: f.thumbnail,
      downloadLink: f.download_link,
      normalDlink: f.normal_dlink,
      fastStreamUrl: f.fast_stream_url,
      subtitleUrl: f.subtitle_url
    }))
  };
}

const isHttp = (u) => typeof u === 'string' && /^https?:\/\//i.test(u) && u.length <= 2000;

async function runTerabox(ctx) {
  if (ctx.from.id.toString() !== ADMIN_ID) return;
  const raw = ctx.message.text.replace(/^\/\w+(@\w+)?\s*/i, '').trim();
  const m = raw.match(/https?:\/\/\S+/);
  if (!m) {
    return ctx.reply('Kirim link Terabox.\nContoh: https://1024terabox.com/s/1xxxxxxxx');
  }

  const wait = await ctx.reply('⏳ Mengambil data Terabox...');
  const removeWait = () => ctx.deleteMessage(wait.message_id).catch(() => {});
  try {
    const r = await fetchTerabox(m[0]);
    if (!r.files.length) {
      await removeWait();
      return ctx.reply('❌ Tidak ada file di link ini' + (r.totalFolders ? ` (hanya ${r.totalFolders} folder)` : '') + '.');
    }
    await removeWait();

    const shown = r.files.slice(0, TERA_MAX_FILES);
    for (const f of shown) {
      const lines = [
        `📁 ${f.name || 'Tanpa nama'}`,
        `📦 Ukuran: ${f.size || '-'}`,
        f.duration ? `⏱️ Durasi: ${Number.isFinite(Number(f.duration)) ? fmtDuration(f.duration) : f.duration}` : null,
        f.quality ? `🎞️ Kualitas: ${f.quality}` : null
      ].filter(Boolean);
      const caption = lines.join('\n').slice(0, 1000);

      const fname = (f.name || 'terabox').replace(/[^\w.\- ]+/g, '_').slice(0, 80);
      const options = [];
      if (isHttp(f.downloadLink)) options.push({ kind: 'video', label: '🎬 HD', url: f.downloadLink, size: f.sizeBytes || 0, filename: fname });
      if (isHttp(f.normalDlink) && f.normalDlink !== f.downloadLink) options.push({ kind: 'video', label: '🎬 Normal', url: f.normalDlink, filename: fname });
      const extra = [];
      if (isHttp(f.fastStreamUrl)) extra.push([Markup.button.url('▶️ Stream', f.fastStreamUrl)]);
      if (isHttp(f.subtitleUrl)) extra.push([Markup.button.url('💬 Subtitle', f.subtitleUrl)]);
      if (!options.length) {
        await ctx.reply(`${caption}\n\n⚠️ Link download tidak tersedia.`, extra.length ? Markup.inlineKeyboard(extra) : {});
        continue;
      }
      await showDownloadMenu(ctx, { caption, thumbnail: f.thumbnail, options, extraRows: extra });
    }

    if (r.files.length > shown.length) {
      await ctx.reply(`ℹ️ Menampilkan ${shown.length} dari ${r.files.length} file.`);
    }
  } catch (e) {
    console.error('[terabox]', apiError(e));
    await removeWait();
    await ctx.reply('❌ Gagal mengambil data Terabox.\n' + apiError(e).slice(0, 200));
  }
}

// ================= /ig (Instagram downloader, igexport) =================
const IG_BASE = process.env.IG_BASE || 'https://igexport.com';
const IG_UA = 'Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Mobile Safari/537.36';
const IG_MAX_MEDIA = 10;

const igEndpoint = (url) => (/\/(reel|reels|tv|stories)\//i.test(url) ? '/api/ig-reels' : '/api/ig-photo');

async function igExportFetch(igUrl, retries = 3) {
  const endpoint = igEndpoint(igUrl);
  let lastErr;
  for (let attempt = 1; attempt <= retries; attempt++) {
    try {
      if (attempt > 1) await new Promise((r) => setTimeout(r, attempt * 5000));
      const headers = {
        'User-Agent': IG_UA,
        'Referer': `${IG_BASE}/id/video-download/`,
        'Accept': 'application/json, text/plain, */*',
        'Sec-Fetch-Dest': 'empty', 'Sec-Fetch-Mode': 'cors', 'Sec-Fetch-Site': 'same-origin'
      };
      // ambil cookie sesi dulu (pengganti cookiejar, tanpa dependensi tambahan)
      const page = await axios.get(`${IG_BASE}/id/video-download/`, { headers, timeout: 30000 });
      const cookie = (page.headers['set-cookie'] || []).map((c) => c.split(';')[0]).join('; ');
      const res = await axios.get(`${IG_BASE}${endpoint}`, {
        params: { url: igUrl },
        headers: cookie ? { ...headers, Cookie: cookie } : headers,
        timeout: 180000
      });
      if (!res.data) throw new Error('Respons kosong dari server');
      return { endpoint, data: res.data };
    } catch (e) {
      lastErr = e;
      const code = e.response && e.response.status;
      if (!([502, 503, 504].includes(code) || !e.response) || attempt === retries) break;
    }
  }
  throw lastErr;
}

// cari URL media di respons (struktur respons tidak pasti, jadi dipindai otomatis)
function pickIgMedia(data, endpoint) {
  const seen = new Set();
  const items = [];
  for (const { key, url } of collectUrls(data)) {
    if (seen.has(url)) continue;
    seen.add(url);
    if (/thumb|cover|avatar|profile|preview|poster|logo|icon/i.test(key)) continue;
    const isVideo = /\.mp4(\?|$)|\.mov(\?|$)|video/i.test(url) || /video/i.test(key);
    const isImage = /\.(jpe?g|png|webp)(\?|$)/i.test(url) || /image|photo|pic|display|src/i.test(key);
    if (!isVideo && !isImage) continue;
    items.push({ url, type: isVideo ? 'video' : 'photo', hd: /hd|high|1080|original/i.test(key + url) });
  }
  if (endpoint === '/api/ig-reels') {
    // reel: tampilkan sampai 3 pilihan video, yang berlabel HD di depan
    const vids = items.filter((i) => i.type === 'video').sort((a, b) => (b.hd ? 1 : 0) - (a.hd ? 1 : 0));
    return (vids.length ? vids : items).slice(0, 3);
  }
  const videos = items.filter((i) => i.type === 'video');
  const photos = items.filter((i) => i.type === 'photo');
  return [...photos, ...videos].slice(0, IG_MAX_MEDIA);
}

async function runInstagram(ctx) {
  if (ctx.from.id.toString() !== ADMIN_ID) return;
  const raw = ctx.message.text.replace(/^\/\w+(@\w+)?\s*/i, '').trim();
  const m = raw.match(/https?:\/\/(?:www\.)?instagram\.com\/\S+/i);
  const replyOpt = { reply_to_message_id: ctx.message.message_id };
  if (!m) {
    return ctx.reply('Kirim link Instagram (post/reel/story).\nContoh: https://www.instagram.com/reel/xxxx/', replyOpt);
  }

  const wait = await ctx.reply('⏳ Mengambil media Instagram...', replyOpt);
  const removeWait = () => ctx.deleteMessage(wait.message_id).catch(() => {});
  try {
    const { endpoint, data } = await igExportFetch(m[0]);
    if (data.ok === false || data.status === false) {
      console.log('[ig raw]', JSON.stringify(data).slice(0, 500));
      throw new Error(data.error || data.message || 'Server menolak link ini (pastikan akun/postingan publik)');
    }
    const media = pickIgMedia(data, endpoint);
    if (!media.length) {
      console.log('[ig raw]', JSON.stringify(data).slice(0, 800));
      throw new Error('Media tidak ditemukan di respons');
    }

    await removeWait();
    const isReel = endpoint === '/api/ig-reels';
    const many = media.length > 1;
    const options = [];
    if (!isReel && many) options.push({ kind: 'all', label: `📦 Semua (${media.length})`, items: media });
    media.slice(0, isReel ? 3 : 8).forEach((it, i) => {
      if (it.type === 'video') {
        const tag = isReel && it.hd ? ' HD' : many ? ` ${i + 1}` : '';
        options.push({ kind: 'video', label: `🎬 Video${tag}`, url: it.url, filename: 'instagram.mp4' });
      } else {
        options.push({ kind: 'photo', label: `🖼 Foto${many ? ` ${i + 1}` : ''}`, url: it.url, filename: 'instagram.jpg' });
      }
    });
    const thumb = (collectUrls(data).find(({ key, url }) => /thumb|cover|preview|poster/i.test(key) && /\.(jpe?g|png|webp)/i.test(url)) || {}).url
      || (media.find((x) => x.type === 'photo') || {}).url;
    const caption = `📸 Instagram${many && !isReel ? ` (${media.length} media)` : ''}\n🔗 ${m[0]}`;
    await showDownloadMenu(ctx, { caption, thumbnail: thumb, options, replyOpt });
  } catch (e) {
    console.error('[ig]', apiError(e));
    await removeWait();
    await ctx.reply('❌ Gagal mengambil media Instagram.\n' + apiError(e).slice(0, 250), replyOpt).catch(() => {});
  }
}

// ================= /nomor (nomor virtual, synoxcloud) =================
const VN_API = process.env.VN_API || 'https://api.synoxcloud.xyz/tools/virtual-numbers';
const VN_KEY = process.env.VN_APIKEY || 'FREE';
const VN_TTL = 10 * 60 * 1000; // cache daftar nomor 10 menit (responsnya besar, ±14.000 nomor)
const VN_PAGE_SIZE = 12;       // negara per halaman
const VN_SHOW = 10;            // nomor yang ditampilkan per negara
let vnCache = { at: 0, total: 0, countries: [] };

async function loadVirtualNumbers() {
  if (vnCache.countries.length && Date.now() - vnCache.at < VN_TTL) return vnCache;
  const { data } = await axios.get(VN_API, { params: { apikey: VN_KEY }, timeout: 60000 });
  const list = data && data.result && data.result.numbers;
  if (!data || !data.status || !Array.isArray(list) || !list.length) {
    console.log('[vnum raw]', JSON.stringify(data).slice(0, 300));
    throw new Error((data && (data.message || data.error)) || 'Daftar nomor kosong');
  }
  const map = new Map();
  for (const n of list) {
    if (!n || !n.number) continue;
    const name = n.country || 'Unknown';
    if (!map.has(name)) map.set(name, { name, flag: n.flag || '🏳️', code: n.countryCode || '', numbers: [] });
    map.get(name).numbers.push(String(n.number));
  }
  const countries = [...map.values()].sort((a, b) => b.numbers.length - a.numbers.length || a.name.localeCompare(b.name));
  vnCache = { at: Date.now(), total: list.length, countries };
  return vnCache;
}

function vnMenuView(cache, page) {
  const pages = Math.max(1, Math.ceil(cache.countries.length / VN_PAGE_SIZE));
  page = Math.min(Math.max(0, page), pages - 1);
  const slice = cache.countries.slice(page * VN_PAGE_SIZE, (page + 1) * VN_PAGE_SIZE);
  const rows = [];
  for (let i = 0; i < slice.length; i += 2) {
    rows.push(slice.slice(i, i + 2).map((c, j) =>
      Markup.button.callback(`${c.flag} ${c.name} (${c.numbers.length})`.slice(0, 40), `vn_c:${page * VN_PAGE_SIZE + i + j}`)));
  }
  const nav = [];
  if (page > 0) nav.push(Markup.button.callback('⬅️ Prev', `vn_p:${page - 1}`));
  nav.push(Markup.button.callback(`${page + 1}/${pages}`, `vn_p:${page}`));
  if (page < pages - 1) nav.push(Markup.button.callback('Next ➡️', `vn_p:${page + 1}`));
  rows.push(nav);
  rows.push([Markup.button.callback('⬅️ Menu', 'back_menu')]);
  return {
    text: `<b>📱 Nomor Virtual</b>\n${cache.total.toLocaleString('id-ID')} nomor dari ${cache.countries.length} negara.\nPilih negara, atau ketik nama negara (contoh: <code>indonesia</code>):\n\n` +
      `<i>Ini nomor publik: siapa pun bisa melihat SMS yang masuk, jadi jangan dipakai untuk akun penting.</i>`,
    kb: Markup.inlineKeyboard(rows)
  };
}

function vnCountryView(cache, idx) {
  const c = cache.countries[idx];
  if (!c) return null;
  const pool = [...c.numbers];
  for (let i = pool.length - 1; i > 0; i--) { const j = Math.floor(Math.random() * (i + 1)); [pool[i], pool[j]] = [pool[j], pool[i]]; }
  const picks = pool.slice(0, VN_SHOW);
  const body = picks.map((n, i) => `${i + 1}. <code>+${escapeHtml(n)}</code>`).join('\n');
  return {
    text: `<b>${c.flag} ${escapeHtml(c.name)}</b>${c.code ? ` (+${escapeHtml(c.code)})` : ''}\n${c.numbers.length} nomor tersedia, ${picks.length} acak:\n\n${body}\n\n<i>Ketuk nomor untuk menyalin.</i>`,
    kb: Markup.inlineKeyboard([
      [Markup.button.callback('🔄 Acak lagi', `vn_c:${idx}`)],
      [Markup.button.callback('⬅️ Negara', `vn_p:${Math.floor(idx / VN_PAGE_SIZE)}`), Markup.button.callback('🏠 Menu', 'back_menu')]
    ])
  };
}

async function runNomor(ctx) {
  if (ctx.from.id.toString() !== ADMIN_ID) return;
  const q = ctx.message.text.replace(/^\/\w+(@\w+)?\s*/i, '').trim().toLowerCase();
  const wait = await ctx.reply('⏳ Mengambil daftar nomor...');
  try {
    const cache = await loadVirtualNumbers();
    await ctx.deleteMessage(wait.message_id).catch(() => {});
    let view = null;
    if (q) {
      const idx = cache.countries.findIndex((c) => c.name.toLowerCase() === q || c.name.toLowerCase().includes(q) || c.code === q.replace('+', ''));
      if (idx >= 0) view = vnCountryView(cache, idx);
      else await ctx.reply(`❌ Negara "${q}" tidak ditemukan, pilih dari daftar:`);
    }
    if (!view) view = vnMenuView(cache, 0);
    await ctx.reply(view.text, { parse_mode: 'HTML', ...view.kb });
  } catch (e) {
    console.error('[vnum]', apiError(e));
    await ctx.telegram.editMessageText(ctx.chat.id, wait.message_id, null, '❌ Gagal mengambil nomor.\n' + apiError(e).slice(0, 200)).catch(() => {});
  }
}

async function vnAction(ctx, build) {
  if (ctx.from.id.toString() !== ADMIN_ID) return ctx.answerCbQuery('❌ Akses ditolak');
  try {
    const cache = await loadVirtualNumbers();
    const view = build(cache) || vnMenuView(cache, 0);
    await ctx.answerCbQuery();
    await safeEdit(ctx, view.text, view.kb);
  } catch (e) {
    console.error('[vnum action]', apiError(e));
    await ctx.answerCbQuery('❌ Gagal memuat').catch(() => {});
  }
}
bot.action('vn_menu', (ctx) => {
  ctx.session.waitingForUrl = false;
  ctx.session.step = 'in_nomor';
  return vnAction(ctx, (cache) => vnMenuView(cache, 0));
});
bot.action(/^vn_p:(\d+)$/, (ctx) => vnAction(ctx, (cache) => vnMenuView(cache, Number(ctx.match[1]))));
bot.action(/^vn_c:(\d+)$/, (ctx) => vnAction(ctx, (cache) => vnCountryView(cache, Number(ctx.match[1]))));

// ================= /fb (Facebook downloader, fget + wayin metadata) =================
const FB_UA = 'Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Mobile Safari/537.36';
const FB_URL_RE = /https?:\/\/(?:[\w-]+\.)?(?:facebook\.com|fb\.watch|fb\.com)\/\S+/i;

async function fetchFgetLinks(fbUrl) {
  if (!cheerio) throw new Error('Modul cheerio belum terpasang. Jalankan: npm i cheerio');
  const payload = new URLSearchParams({ id: fbUrl, locale: 'id' }).toString();
  const { data: html } = await axios.post('https://fget.io/process', payload, {
    headers: {
      'User-Agent': FB_UA,
      'Content-Type': 'application/x-www-form-urlencoded',
      'Hx-Current-Url': 'https://fget.io/id',
      'Hx-Request': 'true',
      'Hx-Target': 'target',
      'Hx-Trigger': 'form',
      'Origin': 'https://fget.io',
      'Referer': 'https://fget.io/id'
    },
    timeout: 45000
  });
  const $ = cheerio.load(html);
  const thumbnail = $('.result-thumbnail img').attr('src') || null;
  const downloads = [];
  $('.space-y-2 .flex').each((_, el) => {
    const quality = $(el).find('.text-sm').text().trim();
    const type = $(el).find('.text-xs').text().replace(/[()]/g, '').trim();
    const url = $(el).find('a').attr('href');
    if (quality && url) downloads.push({ quality, type, url });
  });
  return { thumbnail, downloads };
}

async function fetchWayinMeta(fbUrl) {
  try {
    const headers = {
      'User-Agent': FB_UA, 'Content-Type': 'application/json',
      'Origin': 'https://wayin.ai', 'Referer': 'https://wayin.ai/', 'X-Platform': 'web'
    };
    const parse = await axios.post(
      `https://wayinvideo-api.wayin.ai/api/parse_url?url=${encodeURIComponent(fbUrl)}`, {}, { headers, timeout: 30000 });
    const clean = (parse.data && parse.data.data) || fbUrl;
    const metaRes = await axios.post('https://wayinvideo-api.wayin.ai/api/p/v2/get_video_meta',
      { video_url: clean }, { headers, timeout: 30000 });
    const m = (metaRes.data && metaRes.data.data) || {};
    return {
      title: m.title || null,
      author: m.author || null,
      abstract: m.abstract || null,
      durationSec: m.duration ? Math.floor(m.duration / 1000) : null,
      views: m.view_count || 0,
      comments: m.comment_count || 0,
      likes: m.like_count || 0,
      published: m.published_at ? new Date(m.published_at * 1000) : null,
      resolution: m.res || null
    };
  } catch (e) {
    console.error('[fb meta]', e.message); // metadata hanya pelengkap, tidak fatal
    return null;
  }
}

const nfmt = (n) => Number(n || 0).toLocaleString('id-ID');

async function runFacebook(ctx) {
  if (ctx.from.id.toString() !== ADMIN_ID) return;
  const raw = ctx.message.text.replace(/^\/\w+(@\w+)?\s*/i, '').trim();
  const m = raw.match(FB_URL_RE);
  const replyOpt = { reply_to_message_id: ctx.message.message_id };
  if (!m) return ctx.reply('Kirim link video Facebook.\nContoh: https://www.facebook.com/share/r/xxxx/', replyOpt);

  const wait = await ctx.reply('⏳ Mengambil video Facebook...', replyOpt);
  const removeWait = () => ctx.deleteMessage(wait.message_id).catch(() => {});
  try {
    const [links, meta] = await Promise.all([fetchFgetLinks(m[0]), fetchWayinMeta(m[0])]);
    if (!links.downloads.length) throw new Error('Link download tidak ditemukan (pastikan video publik)');
    await removeWait();

    const lines = [`📘 ${meta && meta.title ? meta.title : 'Video Facebook'}`];
    if (meta) {
      if (meta.author) lines.push(`👤 ${meta.author}`);
      if (meta.durationSec) lines.push(`⏱️ ${fmtDuration(meta.durationSec)}${meta.resolution ? ` • ${meta.resolution}` : ''}`);
      lines.push(`👁 ${nfmt(meta.views)}  👍 ${nfmt(meta.likes)}  💬 ${nfmt(meta.comments)}`);
      if (meta.published) lines.push(`📅 ${meta.published.toISOString().slice(0, 10)}`);
    }
    const caption = lines.join('\n').slice(0, 1000);

    const dls = links.downloads.filter((d) => /^https?:\/\//i.test(d.url) && d.url.length <= 2000).slice(0, 8);
    const options = dls.map((d) => {
      const isAudio = /mp3|audio/i.test(`${d.type} ${d.quality}`);
      return {
        kind: isAudio ? 'audio' : 'video',
        label: `${isAudio ? '🎵' : '🎬'} ${d.quality}${!isAudio && d.type ? ` (${d.type})` : ''}`,
        url: d.url,
        filename: isAudio ? 'facebook.mp3' : 'facebook.mp4',
        meta: { title: (meta && meta.title) || 'Facebook', performer: (meta && meta.author) || 'Facebook' }
      };
    });
    await showDownloadMenu(ctx, { caption, thumbnail: links.thumbnail, options, replyOpt });
  } catch (e) {
    console.error('[fb]', apiError(e));
    await removeWait();
    await ctx.reply('❌ Gagal mengambil video Facebook.\n' + apiError(e).slice(0, 250), replyOpt).catch(() => {});
  }
}

// ================= Pengiriman media langsung (video/audio/foto) =================
const TG_UPLOAD_LIMIT = 50 * 1024 * 1024; // batas upload bot Telegram biasa
const DL_UA = 'Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Mobile Safari/537.36';

async function urlSize(url) {
  try {
    const r = await axios.head(url, { timeout: 15000, maxRedirects: 5, headers: { 'User-Agent': DL_UA } });
    const n = Number(r.headers['content-length']);
    return Number.isFinite(n) && n > 0 ? n : null;
  } catch (_) { return null; }
}

// kirim lewat URL dulu (cepat); kalau Telegram gagal, bot unduh sendiri lalu upload (maks 50MB)
async function sendMediaSmart(ctx, kind, url, opts = {}, filename = 'media') {
  const method = { video: 'replyWithVideo', audio: 'replyWithAudio', photo: 'replyWithPhoto' }[kind];
  try {
    return await ctx[method](url, opts);
  } catch (e) {
    console.error(`[kirim ${kind} via URL gagal, unduh manual]`, e.message);
  }
  const dl = await axios.get(url, {
    responseType: 'arraybuffer', timeout: 180000,
    maxContentLength: TG_UPLOAD_LIMIT, maxBodyLength: TG_UPLOAD_LIMIT,
    headers: { 'User-Agent': DL_UA }
  });
  const buf = Buffer.from(dl.data);
  try {
    return await ctx[method]({ source: buf, filename }, opts);
  } catch (e2) {
    console.error(`[upload ${kind} gagal, kirim sebagai dokumen]`, e2.message);
    const { caption, reply_markup, reply_to_message_id } = opts;
    return ctx.replyWithDocument({ source: buf, filename }, { caption, reply_markup, reply_to_message_id });
  }
}

// ================= Menu pilihan download (tombol) =================
// Bot menampilkan tombol pilihan (MP3 / resolusi video) dulu; file baru dikirim setelah tombol ditekan.
const DL_CACHE = new Map();
const DL_TTL = 30 * 60 * 1000;

const fmtBytes = (n) => (n >= 1073741824 ? `${(n / 1073741824).toFixed(1)} GB`
  : n >= 1048576 ? `${(n / 1048576).toFixed(1)} MB`
  : `${Math.max(1, Math.round(n / 1024))} KB`);

function dlStore(entry) {
  const now = Date.now();
  for (const [k, v] of DL_CACHE) if (now - v.at > DL_TTL) DL_CACHE.delete(k);
  while (DL_CACHE.size >= 300) DL_CACHE.delete(DL_CACHE.keys().next().value);
  let id;
  do { id = crypto.randomBytes(4).toString('hex'); } while (DL_CACHE.has(id));
  DL_CACHE.set(id, { ...entry, at: now });
  return id;
}

async function showDownloadMenu(ctx, { caption, thumbnail, options, replyOpt = {}, extraRows = [] }) {
  // ukuran tiap pilihan ditampilkan di tombol (kalau server memberi tahu)
  await Promise.all(options.map(async (o) => { if (!o.size && o.url) o.size = await urlSize(o.url); }));
  const id = dlStore({ options, caption });
  const btns = options.map((o, i) => Markup.button.callback(
    `${o.label}${o.size ? ` • ${fmtBytes(o.size)}` : ''}${o.size > TG_UPLOAD_LIMIT ? ' ⚠️' : ''}`.slice(0, 40), `dl:${id}:${i}`));
  const rows = [];
  for (let i = 0; i < btns.length; i += 2) rows.push(btns.slice(i, i + 2));
  rows.push(...extraRows);
  const kb = Markup.inlineKeyboard(rows);
  const text = `${caption.slice(0, 940)}\n\n👇 Pilih yang mau dikirim:`;
  if (typeof thumbnail === 'string' && /^https?:\/\//i.test(thumbnail)) {
    const ok = await ctx.replyWithPhoto(thumbnail, { caption: text, ...kb, ...replyOpt }).then(() => true).catch(() => false);
    if (ok) return;
  }
  await ctx.reply(text, { ...kb, ...replyOpt });
}

bot.action(/^dl:([0-9a-f]+):(\d+)$/, async (ctx) => {
  if (ctx.from.id.toString() !== ADMIN_ID) return ctx.answerCbQuery('❌ Akses ditolak');
  const entry = DL_CACHE.get(ctx.match[1]);
  const opt = entry && entry.options[Number(ctx.match[2])];
  if (!opt) return ctx.answerCbQuery('Pilihan sudah kedaluwarsa, kirim ulang linknya.', { show_alert: true });
  if (opt.sending) return ctx.answerCbQuery('⏳ Masih dikirim...');
  opt.sending = true;
  await ctx.answerCbQuery(`⏳ Mengirim ${opt.label}`.slice(0, 60)).catch(() => {});

  const replyTo = ctx.callbackQuery && ctx.callbackQuery.message ? ctx.callbackQuery.message.message_id : undefined;
  const base = replyTo ? { reply_to_message_id: replyTo } : {};
  try {
    if (opt.kind === 'album') {
      await ctx.sendChatAction('upload_photo').catch(() => {});
      const group = opt.urls.map((u, i) => ({ type: 'photo', media: u, ...(i === 0 ? { caption: entry.caption.slice(0, 1000) } : {}) }));
      try {
        await ctx.replyWithMediaGroup(group, base);
      } catch (e) {
        console.error('[album gagal]', e.message);
        const rows = opt.urls.slice(0, 8).map((u, i) => [Markup.button.url(`🖼 Foto ${i + 1}`, u)]);
        await ctx.reply('⚠️ Album tidak bisa dikirim langsung. Link foto:', { ...Markup.inlineKeyboard(rows), ...base });
      }
    } else if (opt.kind === 'photo') {
      await ctx.sendChatAction('upload_photo').catch(() => {});
      await sendMediaSmart(ctx, 'photo', opt.url, { caption: entry.caption.slice(0, 1000), ...base }, opt.filename || 'photo.jpg');
    } else if (opt.kind === 'all') {
      for (const it of opt.items) {
        const kind = it.type === 'video' ? 'video' : 'photo';
        try {
          await ctx.sendChatAction(kind === 'video' ? 'upload_video' : 'upload_photo').catch(() => {});
          await sendMediaSmart(ctx, kind, it.url, { ...base }, kind === 'video' ? 'instagram.mp4' : 'instagram.jpg');
        } catch (e) {
          console.error('[dl all]', apiError(e));
          await ctx.reply('⚠️ Satu file tidak bisa dikirim langsung. Link download:',
            { ...Markup.inlineKeyboard([[Markup.button.url('⬇️ Download', it.url)]]), ...base }).catch(() => {});
        }
      }
    } else if (opt.kind === 'audio') {
      await ctx.sendChatAction('upload_voice').catch(() => {});
      await sendMediaSmart(ctx, 'audio', opt.url, {
        title: (opt.meta && opt.meta.title) || 'Audio', performer: (opt.meta && opt.meta.performer) || undefined, ...base
      }, opt.filename || 'audio.mp3');
    } else {
      await ctx.sendChatAction('upload_video').catch(() => {});
      await sendMediaSmart(ctx, 'video', opt.url, {
        caption: entry.caption.slice(0, 1000), supports_streaming: true, ...base
      }, opt.filename || 'video.mp4');
    }
  } catch (e) {
    console.error('[dl]', apiError(e));
    await ctx.reply('⚠️ Tidak bisa dikirim lewat Telegram (kemungkinan lebih dari 50MB). Link download:',
      { ...Markup.inlineKeyboard([[Markup.button.url('⬇️ Download', opt.url)]]), ...base }).catch(() => {});
  } finally {
    opt.sending = false;
  }
});

// ================= TikTok downloader (synoxcloud) =================
const TT_API = process.env.TIKTOK_API || 'https://api.synoxcloud.xyz/download/tiktok';
const TT_KEY = process.env.SYNOX_APIKEY || process.env.VN_APIKEY || 'FREE';
const TT_URL_RE = /https?:\/\/(?:[\w-]+\.)?tiktok\.com\/\S+/i;

async function runTikTok(ctx) {
  if (ctx.from.id.toString() !== ADMIN_ID) return;
  const raw = ctx.message.text.trim();
  const m = raw.match(TT_URL_RE);
  const replyOpt = { reply_to_message_id: ctx.message.message_id };
  if (!m) return ctx.reply('Kirim link TikTok.\nContoh: https://vt.tiktok.com/ZSxxxxxx/', replyOpt);

  const wait = await ctx.reply('⏳ Mengambil video TikTok...', replyOpt);
  const removeWait = () => ctx.deleteMessage(wait.message_id).catch(() => {});
  try {
    const { data } = await axios.get(TT_API, { params: { url: m[0], apikey: TT_KEY }, timeout: 60000 });
    const r = data && data.result;
    const dls = (r && Array.isArray(r.downloads) ? r.downloads : []).filter((d) => d && /^https?:\/\//i.test(d.url || ''));
    if (!data || !data.status || !r || !dls.length) {
      console.log('[tiktok raw]', JSON.stringify(data).slice(0, 500));
      throw new Error((data && (data.message || data.error)) || 'Video tidak ditemukan (pastikan link publik)');
    }

    const videos = dls.filter((d) => d.type === 'video');
    const images = dls.filter((d) => /^(image|photo)$/i.test(d.type));
    const music = dls.find((d) => /^(music|audio)$/i.test(d.type));
    const author = r.author && r.author !== 'Unknown' ? `\n👤 ${r.author}` : '';
    const caption = `🎬 ${r.title || 'TikTok'}${author}`.slice(0, 1000);
    await removeWait();

    const meta = {
      title: String(r.title || 'TikTok').slice(0, 100),
      performer: r.author && r.author !== 'Unknown' ? String(r.author).slice(0, 100) : 'TikTok'
    };
    const options = [];
    if (videos.length) videos.slice(0, 3).forEach((v, i) => options.push({ kind: 'video', label: `🎬 Video ${i + 1}`, url: v.url, filename: 'tiktok.mp4' }));
    else if (images.length) options.push({ kind: 'album', label: `🖼 Foto (${Math.min(images.length, 10)})`, urls: images.slice(0, 10).map((d) => d.url) });
    if (music) options.push({ kind: 'audio', label: '🎵 MP3', url: music.url, filename: 'tiktok.mp3', meta });
    if (!options.length) throw new Error('Tidak ada media di hasil');
    await showDownloadMenu(ctx, { caption, thumbnail: r.thumbnail, options, replyOpt });
  } catch (e) {
    console.error('[tiktok]', apiError(e));
    await removeWait();
    await ctx.reply('❌ Gagal mengambil video TikTok.\n' + apiError(e).slice(0, 250), replyOpt).catch(() => {});
  }
}

// ================= Spotify downloader (synoxcloud) =================
const SPOTIFY_DL_API = process.env.SPOTIFY_DL_API || 'https://api.synoxcloud.xyz/download/spotify';
const SYNOX_KEY = process.env.SYNOX_APIKEY || process.env.VN_APIKEY || 'FREE';

async function runSpotifyDl(ctx) {
  if (ctx.from.id.toString() !== ADMIN_ID) return;
  const m = ctx.message.text.match(/https?:\/\/open\.spotify\.com\/\S+/i);
  const replyOpt = { reply_to_message_id: ctx.message.message_id };
  if (!m) return ctx.reply('Kirim link lagu Spotify.\nContoh: https://open.spotify.com/track/xxxx', replyOpt);

  const wait = await ctx.reply('⏳ Mengambil data Spotify...', replyOpt);
  const removeWait = () => ctx.deleteMessage(wait.message_id).catch(() => {});
  try {
    const { data } = await axios.get(SPOTIFY_DL_API, { params: { url: m[0], apikey: SYNOX_KEY }, timeout: 60000 });
    const r = data && data.result;
    const dls = (r && Array.isArray(r.downloads) ? r.downloads : []).filter((d) => d && /^https?:\/\//i.test(d.url || ''));
    if (!data || !data.status || !r || !dls.length) {
      console.log('[spotify raw]', JSON.stringify(data).slice(0, 500));
      throw new Error((data && (data.message || data.error)) || 'Lagu tidak ditemukan (pastikan link track Spotify)');
    }
    const title = String(r.title || 'Unknown');
    const artist = String(r.artist || 'Unknown');
    const options = dls.map((d) => (d.type === 'photo'
      ? { kind: 'photo', label: '🖼 Cover', url: d.url, filename: 'cover.jpg' }
      : {
        kind: 'audio', label: `🎵 MP3${d.quality ? ` ${d.quality}` : ''}`, url: d.url,
        filename: `${artist} - ${title}`.replace(/[^\w\- ]+/g, '').trim().slice(0, 60) + '.mp3',
        meta: { title: title.slice(0, 100), performer: artist.slice(0, 100) }
      }));
    await removeWait();
    await showDownloadMenu(ctx, { caption: `🎧 ${title}\n👤 ${artist}`, thumbnail: r.thumbnail, options, replyOpt });
  } catch (e) {
    console.error('[spotify]', apiError(e));
    await removeWait();
    await ctx.reply('❌ Gagal mengambil lagu Spotify.\n' + apiError(e).slice(0, 250), replyOpt).catch(() => {});
  }
}

// ================= Kartu canvas (ID Card & Spotify Card) =================
const CANVAS_BASE = process.env.CANVAS_BASE || 'https://api.synoxcloud.xyz/canvas';
const WIZARDS = {
  idcard: {
    icon: '🪪', title: 'ID Card', endpoint: 'idcard',
    fields: [
      { key: 'name', ask: 'Kirim <b>nama</b>:', max: 40 },
      { key: 'title', ask: 'Kirim <b>jabatan</b> (contoh: <code>Creator</code>):', max: 40 },
      { key: 'script', ask: 'Kirim <b>teks di kartu</b> (contoh: <code>Api synox</code>):', max: 60 },
      { key: 'contact', ask: 'Kirim <b>kontak/link</b> (contoh: <code>https://t.me/username</code>):', max: 100 }
    ]
  },
  spcard: {
    icon: '🎵', title: 'Spotify Card', endpoint: 'spotifycard',
    fields: [
      { key: 'title', ask: 'Kirim <b>judul lagu</b>:', max: 80 },
      { key: 'artist', ask: 'Kirim <b>nama artis</b>:', max: 60 },
      { key: 'cover', ask: 'Kirim <b>link gambar cover</b> (https://..., jpg/png).\nKirim <code>-</code> kalau mau cover bawaan.', max: 500, url: true, optional: true }
    ]
  }
};
const wizKb = Markup.inlineKeyboard([[Markup.button.callback('⬅️ Menu', 'back_menu')]]);

bot.action(/^wiz_(\w+)$/, async (ctx) => {
  if (ctx.from.id.toString() !== ADMIN_ID) return ctx.answerCbQuery('❌ Akses ditolak');
  const w = WIZARDS[ctx.match[1]];
  if (!w) return ctx.answerCbQuery('Menu tidak dikenal');
  await ctx.answerCbQuery();
  ctx.session.waitingForUrl = false;
  ctx.session.step = 'wiz';
  ctx.session.wiz = { key: ctx.match[1], i: 0, data: {} };
  await safeEdit(ctx, `<b>${w.icon} ${w.title}</b>\n\n${w.fields[0].ask}`, wizKb);
});

async function canvasImage(endpoint, params) {
  const res = await axios.get(`${CANVAS_BASE}/${endpoint}`, {
    params: { ...params, apikey: SYNOX_KEY }, responseType: 'arraybuffer', timeout: 60000, validateStatus: () => true
  });
  const type = String(res.headers['content-type'] || '');
  const buf = Buffer.from(res.data);
  if (res.status === 200 && /^image\//i.test(type) && buf.length > 100) return buf;
  let msg = `HTTP ${res.status}`;
  try { const j = JSON.parse(buf.toString('utf8')); msg = j.message || j.error || msg; } catch (_) { /* bukan JSON */ }
  throw new Error(msg);
}

async function generateCard(ctx, key, data) {
  const w = WIZARDS[key];
  const wait = await ctx.reply('⏳ Membuat kartu...');
  const again = Markup.inlineKeyboard([[Markup.button.callback('🔁 Buat lagi', `wiz_${key}`), Markup.button.callback('⬅️ Menu', 'back_menu')]]);
  try {
    await ctx.sendChatAction('upload_photo').catch(() => {});
    const buf = await canvasImage(w.endpoint, data);
    await ctx.deleteMessage(wait.message_id).catch(() => {});
    try {
      await ctx.replyWithPhoto({ source: buf, filename: `${w.endpoint}.png` }, { caption: `${w.icon} ${w.title} selesai`, ...again });
    } catch (e) {
      await ctx.replyWithDocument({ source: buf, filename: `${w.endpoint}.png` }, { caption: `${w.icon} ${w.title} selesai`, ...again });
    }
  } catch (e) {
    console.error('[canvas]', apiError(e));
    await ctx.telegram.editMessageText(ctx.chat.id, wait.message_id, null,
      '❌ Gagal membuat kartu: ' + apiError(e).slice(0, 250), again).catch(() => {});
  }
}

async function runWizardStep(ctx) {
  const wiz = ctx.session.wiz;
  const w = wiz && WIZARDS[wiz.key];
  if (!w) { ctx.session.step = null; return; }
  const f = w.fields[wiz.i];
  const value = ctx.message.text.trim();
  if (!value) return;
  if (f.url && !(f.optional && value === '-')) {
    if (!/^https?:\/\/\S+$/i.test(value)) return ctx.reply('❌ Harus berupa link gambar yang diawali https://', wizKb);
  }
  if (!(f.optional && value === '-')) wiz.data[f.key] = value.slice(0, f.max);
  wiz.i++;
  if (wiz.i < w.fields.length) {
    return ctx.reply(`<b>${w.icon} ${w.title}</b>\n\n${w.fields[wiz.i].ask}`, { parse_mode: 'HTML', ...wizKb });
  }

  const data = wiz.data;
  ctx.session.step = null;
  ctx.session.wiz = null;
  await generateCard(ctx, wiz.key, data);
}

// ================= MENU TOMBOL: input untuk fitur (menggantikan command) =================
const INPUT_FLOWS = {
  play: { icon: '🎵', title: 'Play Musik', run: runPlay,
    hint: 'Kirim judul lagu atau link YouTube.\nContoh: <code>dewa 19 kangen</code>' },
  hd: { icon: '🖼', title: 'HD Foto (Upscale)', run: runHd,
    hint: 'Kirim foto yang mau di-upscale jadi HD (sebagai foto atau file gambar).' },
  ig: { icon: '📸', title: 'Instagram Downloader', run: runInstagram,
    hint: 'Kirim link Instagram (post, reel, atau story).\nContoh: <code>https://www.instagram.com/reel/xxxx/</code>' },
  tiktok: { icon: '🎬', title: 'TikTok Downloader', run: runTikTok,
    hint: 'Kirim link TikTok (video atau slideshow foto).\nContoh: <code>https://vt.tiktok.com/ZSxxxxxx/</code>' },
  spotify: { icon: '🎧', title: 'Spotify Downloader', run: runSpotifyDl,
    hint: 'Kirim link lagu Spotify (bukan judul).\nContoh: <code>https://open.spotify.com/track/xxxx</code>' },
  fb: { icon: '📘', title: 'Facebook Downloader', run: runFacebook,
    hint: 'Kirim link video Facebook.\nContoh: <code>https://www.facebook.com/share/r/xxxx/</code>' },
  terabox: { icon: '📦', title: 'Terabox Downloader', run: runTerabox,
    hint: 'Kirim link Terabox.\nContoh: <code>https://1024terabox.com/s/1xxxxxxxx</code>' },
  imagine: { icon: '🎨', title: 'Buat Gambar (SFW)', run: runImagine,
    hint: 'Kirim deskripsi gambar (bahasa Inggris biasanya lebih bagus).\nContoh: <code>(masterpiece), 1girl, kimono, cherry blossom</code>\n\nNegative prompt opsional, pisahkan dengan <code>|</code>:\n<code>prompt | negative prompt</code>' }
};

bot.action(/^flow_(\w+)$/, async (ctx) => {
  if (ctx.from.id.toString() !== ADMIN_ID) return ctx.answerCbQuery('❌ Akses ditolak');
  const flow = INPUT_FLOWS[ctx.match[1]];
  if (!flow) return ctx.answerCbQuery('Menu tidak dikenal');
  await ctx.answerCbQuery();
  ctx.session.waitingForUrl = false;
  ctx.session.step = `in_${ctx.match[1]}`;
  await safeEdit(ctx,
    `<b>${flow.icon} ${flow.title}</b>\n\n${flow.hint}\n\n<i>Setelah selesai kamu bisa langsung kirim lagi. Tekan Menu untuk keluar.</i>`,
    Markup.inlineKeyboard([[Markup.button.callback('⬅️ Menu', 'back_menu')]]));
});

// foto/file gambar -> hanya diproses saat mode HD Foto aktif
bot.on(['photo', 'document'], async (ctx) => {
  if (ctx.from.id.toString() !== ADMIN_ID) return;
  // foto dengan caption /hd langsung diproses
  if (/^\/hd(@\w+)?(\s|$)/i.test(ctx.message.caption || '')) {
    ctx.session.step = 'in_hd';
    return runHd(ctx);
  }
  if (ctx.session.step !== 'in_hd') return;
  return runHd(ctx);
});

// ================= COMMAND SLASH (jalan pintas, selain tombol menu) =================
// Pakai langsung: /ig <link>. Tanpa isi (/ig saja) = masuk mode input seperti menekan tombol.
const argOf = (ctx) => ctx.message.text.replace(/^\/\w+(@\w+)?\s*/i, '').trim();
const adminOnly = (ctx) => ctx.from.id.toString() === ADMIN_ID;

async function runAiChat(ctx, prompt) {
  if (!prompt) return;
  await ctx.sendChatAction('typing').catch(() => {});
  const typing = setInterval(() => ctx.sendChatAction('typing').catch(() => {}), 4000);
  try {
    const answer = await askAiChat(ctx.session, prompt, prompt);
    clearInterval(typing);
    await sendChatReply(ctx, answer);
  } catch (e) {
    clearInterval(typing);
    console.error('[ai]', apiError(e));
    await ctx.reply('❌ AI error: ' + apiError(e).slice(0, 500), aiKeyboard);
  }
}

const CMD_FLOWS = [
  [['play'], 'play'], [['hd'], 'hd'], [['ig', 'instagram', 'igdl'], 'ig'], [['fb', 'facebook', 'fbdl'], 'fb'],
  [['tiktok', 'tt'], 'tiktok'], [['spotify', 'spotifydl'], 'spotify'], [['terabox', 'tera'], 'terabox'],
  [['imagine', 'txt2img'], 'imagine']
];
for (const [names, key] of CMD_FLOWS) {
  bot.command(names, async (ctx) => {
    if (!adminOnly(ctx)) return;
    const flow = INPUT_FLOWS[key];
    ctx.session.waitingForUrl = false;
    ctx.session.step = `in_${key}`; // setelah ini pesan berikutnya juga langsung diproses
    if (key === 'hd') {
      const rp = ctx.message.reply_to_message;
      if (rp && (rp.photo || (rp.document && /^image\//i.test(rp.document.mime_type || '')))) return runHd(ctx, rp);
    } else if (argOf(ctx)) {
      return flow.run(ctx);
    }
    return ctx.reply(`<b>${flow.icon} ${flow.title}</b>\n\n${flow.hint}\n\n<i>Setelah selesai kamu bisa langsung kirim lagi. Tekan Menu untuk keluar.</i>`,
      { parse_mode: 'HTML', ...wizKb });
  });
}

bot.command(['nomor', 'vnum', 'virtualnumber'], async (ctx) => {
  if (!adminOnly(ctx)) return;
  ctx.session.waitingForUrl = false;
  ctx.session.step = 'in_nomor';
  return runNomor(ctx);
});

bot.command('ai', async (ctx) => {
  if (!adminOnly(ctx)) return;
  ctx.session.waitingForUrl = false;
  ctx.session.step = 'ai_chat';
  const arg = argOf(ctx);
  if (arg) return runAiChat(ctx, arg);
  return ctx.reply(aiIntro(), { parse_mode: 'HTML', ...aiKeyboard });
});

bot.command('coder', async (ctx) => {
  if (!adminOnly(ctx)) return;
  ctx.session.waitingForUrl = false;
  ctx.session.step = 'ai_coder';
  if (!ctx.session.coderId) ctx.session.coderId = newCoderId();
  if (argOf(ctx)) return runCoder(ctx);
  return ctx.reply(coderIntro(), { parse_mode: 'HTML', ...coderKeyboard });
});

// /idcard nama | jabatan | teks | kontak   dan   /spcard judul | artis | cover
for (const [names, key] of [[['idcard'], 'idcard'], [['spcard', 'spotifycard'], 'spcard']]) {
  bot.command(names, async (ctx) => {
    if (!adminOnly(ctx)) return;
    const w = WIZARDS[key];
    ctx.session.waitingForUrl = false;
    const parts = argOf(ctx).split('|').map((x) => x.trim());
    const required = w.fields.filter((f) => !f.optional).length;
    if (parts.length >= required && parts.slice(0, required).every(Boolean)) {
      const data = {};
      for (let i = 0; i < w.fields.length; i++) {
        const f = w.fields[i];
        const v = parts[i];
        if (!v || v === '-') continue;
        if (f.url && !/^https?:\/\/\S+$/i.test(v)) return ctx.reply('❌ Cover harus berupa link gambar yang diawali https://');
        data[f.key] = v.slice(0, f.max);
      }
      ctx.session.step = null;
      return generateCard(ctx, key, data);
    }
    ctx.session.step = 'wiz';
    ctx.session.wiz = { key, i: 0, data: {} };
    return ctx.reply(`<b>${w.icon} ${w.title}</b>\n\n${w.fields[0].ask}`, { parse_mode: 'HTML', ...wizKb });
  });
}

bot.command('menu', async (ctx) => {
  if (!adminOnly(ctx)) return;
  ctx.session.waitingForUrl = false;
  ctx.session.step = null;
  return ctx.reply(
    `<b>👋 Hai bosq! Selamat datang di AM Preset Bot</b>\n\nPilih menu di bawah ya:`,
    { parse_mode: 'HTML', ...mainMenu });
});

bot.command('help', async (ctx) => {
  if (!adminOnly(ctx)) return;
  return ctx.reply(helpText(), { parse_mode: 'HTML', ...Markup.inlineKeyboard([[Markup.button.callback('⬅️ Menu', 'back_menu')]]) });
});

// daftar yang muncul saat mengetik "/" di Telegram
const CMD_MENU = [
  ['play', 'Cari & kirim lagu YouTube'], ['hd', 'Upscale foto jadi HD (reply foto)'],
  ['ig', 'Download Instagram'], ['fb', 'Download Facebook'], ['tiktok', 'Download TikTok'],
  ['spotify', 'Download lagu Spotify'], ['terabox', 'Ambil link Terabox'], ['imagine', 'Buat gambar dari teks (SFW)'],
  ['nomor', 'Nomor virtual publik'], ['ai', 'Ngobrol dengan AI'], ['coder', 'AI Coder (bikin kode)'],
  ['idcard', 'Buat ID Card'], ['spcard', 'Buat Spotify Card'], ['menu', 'Buka menu utama'], ['help', 'Bantuan']
].map(([command, description]) => ({ command, description }));

bot.on('text', async (ctx) => {
  if (ctx.from.id.toString() !== ADMIN_ID) return;

  // ---- fitur dari tombol menu (play, ig, fb, terabox, imagine, nomor, hd) ----
  const stepName = typeof ctx.session.step === 'string' && ctx.session.step.startsWith('in_') ? ctx.session.step.slice(3) : null;
  if (ctx.session.step === 'wiz') return runWizardStep(ctx);
  if (stepName === 'nomor') return runNomor(ctx);
  if (stepName === 'hd') return ctx.reply('🖼 Kirim fotonya (bukan teks) ya.');
  if (stepName && INPUT_FLOWS[stepName]) return INPUT_FLOWS[stepName].run(ctx);

  // ---- alur Fake ML: username & avatar ----
  if (ctx.session.step === 'fml_username') {
    const name = ctx.message.text.trim();
    if (!name) return;
    if (name.length > 15) return ctx.reply(`❌ Username maksimal 15 karakter (kamu kirim ${name.length}). Coba yang lebih pendek.`);
    ctx.session.fml = { username: name };
    ctx.session.step = 'fml_avatar';
    return ctx.reply(
      `✅ Username: ${name}\n\nSekarang kirim <b>URL foto avatar</b> (link gambar langsung, mis. https://.../foto.jpg), atau pakai avatar default.`,
      { parse_mode: 'HTML', ...Markup.inlineKeyboard([
        [Markup.button.callback('⏭ Pakai avatar default', 'fml_skip_avatar')],
        [Markup.button.callback('❌ Batal', 'back_menu')]
      ]) }
    );
  }
  if (ctx.session.step === 'fml_avatar') {
    const m = ctx.message.text.trim().match(/https?:\/\/\S+/);
    if (!m) return ctx.reply('❌ Itu bukan URL. Kirim link gambar, atau tekan "Pakai avatar default".');
    ctx.session.fml.avatar = m[0];
    ctx.session.step = null;
    return ctx.reply(
      `<b>🎮 Fake ML</b>\nUsername: <code>${escapeHtml(ctx.session.fml.username)}</code>\nAvatar: custom\n\nPilih <b>rank</b>:`,
      { parse_mode: 'HTML', ...fmlRankKeyboard() }
    );
  }

  // ---- mode AI Chat ----
  if (ctx.session.step === 'ai_coder') return runCoder(ctx);

  if (ctx.session.step === 'ai_chat') return runAiChat(ctx, ctx.message.text.trim());

  // ---- alur AM Premium: step email ----
  if (ctx.session.step === 'am_email') {
    const email = ctx.message.text.trim();
    if (!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email)) {
      return ctx.reply('❌ Email tidak valid. Kirim email yang benar.', cancelKeyboard);
    }
    const wait = await ctx.reply('⏳ Mengirim magic link ke ' + email + '...');
    try {
      const r = await satriamSendLink(email);
      ctx.session.amEmail = email;
      ctx.session.step = 'am_link';
      await ctx.telegram.editMessageText(ctx.chat.id, wait.message_id, null,
        `✅ <b>Magic link terkirim</b>${r.message ? '\n' + escapeHtml(r.message) : ''}\n\n` +
        `Cek inbox <code>${escapeHtml(email)}</code>, lalu <b>kirim URL magic link</b>-nya ke sini.`,
        { parse_mode: 'HTML', ...cancelKeyboard });
    } catch (e) {
      console.error('[satriam send]', apiError(e));
      await ctx.telegram.editMessageText(ctx.chat.id, wait.message_id, null,
        `❌ <b>Gagal kirim magic link</b>\n\n<code>${escapeHtml(apiError(e))}</code>\n\nKirim email lain atau batalkan.`,
        { parse_mode: 'HTML', ...cancelKeyboard });
    }
    return;
  }

  // ---- alur AM Premium: step magic link (jangan sampai diproses sebagai link preset) ----
  if (ctx.session.step === 'am_link') {
    const link = ctx.message.text.trim();
    if (!/^https?:\/\//i.test(link)) {
      return ctx.reply('❌ Itu bukan URL. Tempel URL magic link dari email.', cancelKeyboard);
    }
    const wait = await ctx.reply('⏳ Memverifikasi magic link...');
    try {
      const r = await satriamVerify(ctx.session.amEmail, link);
      ctx.session.step = null;
      const { success, ...rest } = r;
      let out = `<b>✅ Verifikasi berhasil</b>\n\n` +
        `Email: <code>${escapeHtml(ctx.session.amEmail)}</code>\n` +
        `UID: <code>${escapeHtml(r.uid ?? '-')}</code>\n` +
        `Plan: ${escapeHtml(r.planName ?? '-')}\n` +
        `Valid sampai: ${escapeHtml(r.validUntil ?? '-')}\n`;
      const extra = JSON.stringify(rest, null, 2);
      out += `\n<b>Data lengkap:</b>\n<pre>${escapeHtml(extra.slice(0, 2000))}</pre>`;
      await ctx.telegram.editMessageText(ctx.chat.id, wait.message_id, null, out,
        { parse_mode: 'HTML', ...Markup.inlineKeyboard([[Markup.button.callback('⬅️ Menu', 'back_menu')]]) });
    } catch (e) {
      console.error('[satriam verify]', apiError(e));
      await ctx.telegram.editMessageText(ctx.chat.id, wait.message_id, null,
        `❌ <b>Verifikasi gagal</b>\n\n<code>${escapeHtml(apiError(e))}</code>\n\nKirim ulang magic link yang benar, atau batalkan.`,
        { parse_mode: 'HTML', ...cancelKeyboard });
    }
    return;
  }

  const url = ctx.message.text.trim();
  const urlRegex = /(https?:\/\/[^\s]+)/;
  const match = url.match(urlRegex);
  
  if (!match) {
    if (ctx.session.waitingForUrl) {
      return ctx.reply('❌ Link tidak valid. Kirim link TikTok/YouTube yang benar.');
    }
    return; // bukan link & tidak sedang menunggu link
  }
  
  const targetUrl = match[0];
  ctx.session.waitingForUrl = false;
  
  const { sent, interval } = await showLoadingAnimation(ctx, `Mencari preset dari ${targetUrl}...`);
  
  try {
    const result = await scrapePreset(targetUrl);
    console.log('[scrape result]', JSON.stringify(result).slice(0, 800));
    clearInterval(interval);
    
    const parsed = classifyResult(result);
    if (parsed.alight.length + parsed.xml.length + parsed.other.length === 0) {
      await ctx.telegram.editMessageText(
        ctx.chat.id,
        sent.message_id,
        null,
        `❌ <b>Preset tidak ditemukan</b>\n\n` +
        `Link: ${escapeHtml(targetUrl)}\n` +
        `Pesan: ${escapeHtml(result.message || 'Tidak ada preset di link tersebut')}`,
        { parse_mode: 'HTML', ...Markup.inlineKeyboard([[Markup.button.callback('🔄 Coba Lagi', 'find_preset'), Markup.button.callback('⬅️ Menu', 'back_menu')]]) }
      );
      return;
    }
    
    await ctx.telegram.editMessageText(
      ctx.chat.id,
      sent.message_id,
      null,
      formatResult(result),
      { parse_mode: 'HTML', ...Markup.inlineKeyboard([[Markup.button.callback('🔄 Cari Lain', 'find_preset'), Markup.button.callback('⬅️ Menu', 'back_menu')]]) }
    );
    
  } catch (error) {
    clearInterval(interval);
    await ctx.telegram.editMessageText(
      ctx.chat.id,
      sent.message_id,
      null,
      `❌ <b>Error saat scraping</b>\n\n` +
      `<code>${escapeHtml(error.message)}</code>\n\n` +
      `Coba link lain atau hubungi admin.`,
      { parse_mode: 'HTML', ...Markup.inlineKeyboard([[Markup.button.callback('🔄 Coba Lagi', 'find_preset'), Markup.button.callback('⬅️ Menu', 'back_menu')]]) }
    );
  }
});

bot.catch((err, ctx) => console.error('Bot error:', err));

bot.telegram.getMe()
  .then(async (me) => {
    console.log(`✅ Token valid: @${me.username}`);
    await bot.telegram.setMyCommands(CMD_MENU).catch((e) => console.error('[setMyCommands]', e.message));
    return bot.launch();
  })
  .catch((err) => {
    console.error('❌ Gagal start bot:', err.message);
    process.exit(1);
  });
console.log('⏳ Menghubungkan ke Telegram...');

process.once('SIGINT', () => bot.stop('SIGINT'));
process.once('SIGTERM', () => bot.stop('SIGTERM'));
