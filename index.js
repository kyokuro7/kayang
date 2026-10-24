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
const FILE_INTENT_RE = /\b(buat(?:kan|in)?|bikin(?:kan)?|create|generate|tulis(?:kan)?|write|coding|kode|code|script|file|html|css|javascript|python|php)\b/i;
const FILE_INSTRUCTION =
  'Jika kamu diminta membuat kode, tulis setiap file dalam blok kode terpisah dengan format ```bahasa:namafile.ext ' +
  '(contoh: ```js:index.js atau ```css:style.css). Satu blok = satu file utuh. Jelaskan singkat di luar blok kode.\n\n';

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

// kirim jawaban AI: teks biasa + file untuk blok kode
async function sendAiAnswer(ctx, prompt, answer) {
  const { files, text } = extractCodeFiles(answer);
  const asFiles = files.length > 0 && (FILE_INTENT_RE.test(prompt) || files.some((f) => f.lines >= 15));

  if (!asFiles) {
    const parts = chunkText(answer);
    for (let i = 0; i < parts.length; i++) {
      await ctx.reply(parts[i], i === parts.length - 1 ? aiKeyboard : undefined);
    }
    return;
  }

  if (text) {
    for (const part of chunkText(text)) await ctx.reply(part);
  }
  await ctx.sendChatAction('upload_document').catch(() => {});
  for (let i = 0; i < files.length; i++) {
    const f = files[i];
    const isLast = i === files.length - 1;
    await ctx.replyWithDocument(
      { source: Buffer.from(f.code, 'utf8'), filename: f.name },
      { caption: `📄 ${f.name} (${f.lines} baris)`, ...(isLast ? aiKeyboard : {}) }
    );
  }
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
  [Markup.button.callback('🤖 AI Chat', 'ai_chat'), Markup.button.callback('🎮 Fake ML', 'fake_ml')],
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

bot.action('help', async (ctx) => {
  if (ctx.from.id.toString() !== ADMIN_ID) return ctx.answerCbQuery('❌ Akses ditolak');
  await ctx.answerCbQuery();
  await ctx.editMessageText(
    `<b>📖 Bantuan</b>\n\n` +
    `1. Tekan <b>Find Preset AM</b>\n` +
    `2. Kirim link TikTok/YouTube\n` +
    `3. Tunggu proses scraping\n` +
    `4. Dapatkan link Alight Creative & XML\n\n` +
    `Format link yang didukung:\n` +
    `• TikTok: vt.tiktok.com, tiktok.com\n` +
    `• YouTube: youtu.be, youtube.com\n\n` +
    `<b>📧 Temp Mail</b>: buat email sementara & baca inbox-nya.\n` +
    `<b>🎵 /play</b> &lt;judul/url&gt;: cari lagu YouTube dan kirim audionya.\n` +
    `<b>📦 /terabox</b> &lt;link&gt;: ambil link download HD &amp; stream dari file Terabox.\n` +
    `<b>📸 /ig</b> &lt;link&gt;: download foto/video/reel Instagram (otomatis deteksi, HD jika tersedia).\n` +
    `<b>🎨 /imagine</b> &lt;prompt&gt; | &lt;negative&gt;: buat gambar dari teks (khusus SFW, butuh <code>npm i crypto-js</code>).\n` +
    `<b>🖼 /hd</b>: reply sebuah foto dengan /hd untuk di-upscale jadi HD.\n` +
    `<b>🎮 Fake ML</b>: buat kartu profil Mobile Legends palsu (username, avatar, rank, border).\n` +
    `<b>🤖 AI Chat</b>: ngobrol dengan AI, tiap pesan dijawab (tanpa memori percakapan).\n` +
    `<b>⭐ AM Premium</b>: login Satriam lewat magic link (kirim email → tempel link dari inbox).\n\n` +
    `Kalau error, coba lagi atau hubungi admin.`,
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
  `Minta dibuatkan kode (mis. "buatkan landing page html css js") dan hasilnya dikirim sebagai file.\n` +
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

// ================= /hd (UPSCALE FOTO) =================
const UPSCALE_API = process.env.UPSCALE_API || 'https://api.ikyyxd.my.id/tools/upscale';

// Link file Telegram berisi TOKEN BOT, jadi tidak boleh dikirim ke API pihak lain.
// Foto diunduh di sini, lalu diunggah sementara (1 jam) ke litterbox untuk dapat URL publik.
async function tgFileToBuffer(ctx, fileId) {
  const link = await ctx.telegram.getFileLink(fileId);
  const res = await axios.get(link.href, { responseType: 'arraybuffer', timeout: 60000 });
  return Buffer.from(res.data);
}

async function uploadTemp(buffer, filename = 'image.jpg') {
  const form = new FormData();
  form.append('reqtype', 'fileupload');
  form.append('time', '1h');
  form.append('fileToUpload', new Blob([buffer]), filename);
  const res = await fetch('https://litterbox.catbox.moe/resources/internals/api.php', {
    method: 'POST',
    body: form,
    signal: AbortSignal.timeout(60000)
  });
  const url = (await res.text()).trim();
  if (!/^https?:\/\//i.test(url)) throw new Error('Upload sementara gagal: ' + url.slice(0, 100));
  return url;
}

bot.command('hd', async (ctx) => {
  if (ctx.from.id.toString() !== ADMIN_ID) return;
  const q = ctx.message.reply_to_message;
  const fileId = q && (
    (q.photo && q.photo[q.photo.length - 1].file_id) ||
    (q.document && /^image\//i.test(q.document.mime_type || '') && q.document.file_id)
  );
  if (!fileId) return ctx.reply('❌ Reply foto yang mau dibikin HD dengan perintah /hd');

  const wait = await ctx.reply('⏳ Tunggu sebentar...');
  try {
    const buf = await tgFileToBuffer(ctx, fileId);
    const publicUrl = await uploadTemp(buf);
    const res = await axios.get(UPSCALE_API, { params: { url: publicUrl }, timeout: 120000 });
    const r = (res.data && res.data.result) || {};
    // format API pernah berubah: sekarang `downloadUrl` / `allDownloadUrls`, dulu `image`
    const imageUrl = [r.image, r.downloadUrl, r.url, Array.isArray(r.allDownloadUrls) && r.allDownloadUrls[0]]
      .find((u) => typeof u === 'string' && /^https?:\/\//i.test(u))
      || (collectUrls(r).find((u) => /\.(jpe?g|png|webp)(\?|$)/i.test(u.url)) || {}).url;
    if (!(res.data && res.data.status && imageUrl)) {
      console.log('[hd raw]', JSON.stringify(res.data).slice(0, 500));
      throw new Error((res.data && (res.data.error || res.data.message)) || 'API tidak mengembalikan gambar');
    }
    const info = [
      r.scale ? `\n🔍 <b>Skala</b>: ${escapeHtml(r.scale)}` : '',
      r.size ? `\n📦 <b>Size</b>: ${escapeHtml(r.size)}` : '',
      res.data.runtime ? `\n⏱️ <b>Proses</b>: ${escapeHtml(res.data.runtime)}` : ''
    ].join('');
    const extra = {
      caption: `✅ <b>Berhasil di-upscale!</b>${info}`,
      parse_mode: 'HTML',
      reply_to_message_id: ctx.message.message_id
    };
    try {
      await ctx.replyWithPhoto(imageUrl, extra);
    } catch (e) {
      // foto terlalu besar untuk dikirim sebagai photo -> kirim sebagai file
      await ctx.replyWithDocument(imageUrl, extra);
    }
    await ctx.deleteMessage(wait.message_id).catch(() => {});
  } catch (e) {
    console.error('[hd]', apiError(e));
    await ctx.telegram.editMessageText(ctx.chat.id, wait.message_id, null,
      '❌ Terjadi kesalahan: ' + apiError(e).slice(0, 300)).catch(() => {});
  }
});

// ================= /play (YouTube audio) =================
const PLAY_API = process.env.PLAY_API || 'https://api.ikyyxd.my.id/search/ytplayv2';

const fmtDuration = (sec) => {
  sec = Math.max(0, Math.floor(Number(sec) || 0));
  return `${Math.floor(sec / 60)}:${String(sec % 60).padStart(2, '0')}`;
};

bot.command('play', async (ctx) => {
  if (ctx.from.id.toString() !== ADMIN_ID) return;
  const query = ctx.message.text.replace(/^\/play(@\w+)?\s*/i, '').trim();
  if (!query) return ctx.reply('Masukkan judul/url setelah /play\nContoh: /play dewa 19 kangen');

  const wait = await ctx.reply('🔎 Mencari lagu...');
  try {
    const { data } = await axios.get(PLAY_API, { params: { q: query }, timeout: 90000 });
    const res = data && data.result;
    const audioUrl = res && res.audio && res.audio.url;
    if (!data || !data.status || !audioUrl) {
      console.log('[play raw]', JSON.stringify(data).slice(0, 500));
      return ctx.telegram.editMessageText(ctx.chat.id, wait.message_id, null, '❌ Lagu tidak ditemukan').catch(() => {});
    }

    const title = String(res.title || query);
    const caption = `🎵 ${title}\n⏱️ ${fmtDuration(res.duration)}${res.source ? `\n🔗 ${res.source}` : ''}`.slice(0, 1000);

    await ctx.sendChatAction('upload_voice').catch(() => {});
    if (res.thumbnail) {
      await ctx.replyWithPhoto(res.thumbnail, { caption }).catch((e) => console.error('[play thumb]', e.message));
    }

    const opts = { title: title.slice(0, 100), duration: Math.floor(Number(res.duration) || 0) || undefined };
    try {
      await ctx.replyWithAudio(audioUrl, opts);
    } catch (e) {
      // Telegram gagal mengambil URL (file besar / server sumber menolak) -> unduh sendiri lalu upload
      console.error('[play url gagal, coba upload manual]', e.message);
      const dl = await axios.get(audioUrl, { responseType: 'arraybuffer', timeout: 120000, maxContentLength: 50 * 1024 * 1024 });
      const filename = title.replace(/[^\w\- ]+/g, '').trim().slice(0, 60) || 'audio';
      await ctx.replyWithAudio({ source: Buffer.from(dl.data), filename: `${filename}.mp3` }, opts);
    }
    await ctx.deleteMessage(wait.message_id).catch(() => {});
  } catch (e) {
    console.error('[play]', apiError(e));
    await ctx.telegram.editMessageText(ctx.chat.id, wait.message_id, null,
      '❌ Gagal memproses, coba lagi nanti.\n' + apiError(e).slice(0, 200)).catch(() => {});
  }
});

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

bot.command(['imagine', 'txt2img'], async (ctx) => {
  if (ctx.from.id.toString() !== ADMIN_ID) return;
  const raw = ctx.message.text.replace(/^\/\w+(@\w+)?\s*/i, '').trim();
  if (!raw) {
    return ctx.reply('Masukkan deskripsi gambar setelah /imagine\nContoh: /imagine (masterpiece), 1girl, kimono, cherry blossom\n\nNegative prompt (opsional) pakai tanda |\n/imagine prompt | negative prompt');
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
});

// ================= /terabox (playterabox) =================
const TERA_SALT = 'T9do@SM1?xGn5';
const TERA_PATH = '/api/stream.php';
const TERA_MAX_FILES = 5;          // maksimal file yang ditampilkan per link
const TERA_TG_URL_LIMIT = 20 * 1024 * 1024; // Telegram hanya mau ambil video dari URL jika <= 20MB

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

bot.command(['terabox', 'tera'], async (ctx) => {
  if (ctx.from.id.toString() !== ADMIN_ID) return;
  const raw = ctx.message.text.replace(/^\/\w+(@\w+)?\s*/i, '').trim();
  const m = raw.match(/https?:\/\/\S+/);
  if (!m) {
    return ctx.reply('Masukkan link Terabox setelah /terabox\nContoh: /terabox https://1024terabox.com/s/1xxxxxxxx');
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

      const buttons = [];
      const dl = f.downloadLink || f.normalDlink;
      if (isHttp(f.downloadLink)) buttons.push([Markup.button.url('⬇️ Download HD', f.downloadLink)]);
      if (isHttp(f.normalDlink) && f.normalDlink !== f.downloadLink) buttons.push([Markup.button.url('⬇️ Download Normal', f.normalDlink)]);
      if (isHttp(f.fastStreamUrl)) buttons.push([Markup.button.url('▶️ Stream', f.fastStreamUrl)]);
      if (isHttp(f.subtitleUrl)) buttons.push([Markup.button.url('💬 Subtitle', f.subtitleUrl)]);
      const kb = buttons.length ? Markup.inlineKeyboard(buttons) : {};

      // file kecil: coba kirim langsung sebagai video; kalau gagal, jatuh ke kartu info + tombol link
      if (isHttp(dl) && f.sizeBytes > 0 && f.sizeBytes <= TERA_TG_URL_LIMIT) {
        try {
          await ctx.sendChatAction('upload_video').catch(() => {});
          await ctx.replyWithVideo(dl, { caption, supports_streaming: true, ...kb });
          continue;
        } catch (e) {
          console.error('[terabox kirim langsung gagal]', e.message);
        }
      }

      if (isHttp(f.thumbnail)) {
        const ok = await ctx.replyWithPhoto(f.thumbnail, { caption, ...kb }).then(() => true).catch(() => false);
        if (ok) continue;
      }
      await ctx.reply(caption, kb);
    }

    if (r.files.length > shown.length) {
      await ctx.reply(`ℹ️ Menampilkan ${shown.length} dari ${r.files.length} file.`);
    }
  } catch (e) {
    console.error('[terabox]', apiError(e));
    await removeWait();
    await ctx.reply('❌ Gagal mengambil data Terabox.\n' + apiError(e).slice(0, 200));
  }
});

// ================= /ig (Instagram downloader, igexport) =================
const IG_BASE = process.env.IG_BASE || 'https://igexport.com';
const IG_UA = 'Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Mobile Safari/537.36';
const IG_MAX_MEDIA = 10;
const IG_MAX_BYTES = 50 * 1024 * 1024;

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
    // reel = 1 video; kalau ada beberapa kualitas, utamakan yang berlabel HD
    const vids = items.filter((i) => i.type === 'video');
    const best = vids.find((v) => v.hd) || vids[0];
    return best ? [best] : items.slice(0, 1);
  }
  const videos = items.filter((i) => i.type === 'video');
  const photos = items.filter((i) => i.type === 'photo');
  return [...photos, ...videos].slice(0, IG_MAX_MEDIA);
}

async function sendIgMedia(ctx, item, caption, replyOpt) {
  const send = (src, extra = {}) => (item.type === 'video'
    ? ctx.replyWithVideo(src, { caption, supports_streaming: true, ...replyOpt, ...extra })
    : ctx.replyWithPhoto(src, { caption, ...replyOpt, ...extra }));
  try {
    return await send(item.url);
  } catch (e) {
    console.error('[ig kirim via URL gagal, unduh manual]', e.message);
    const dl = await axios.get(item.url, {
      responseType: 'arraybuffer', timeout: 120000, maxContentLength: IG_MAX_BYTES,
      headers: { 'User-Agent': IG_UA, Referer: 'https://www.instagram.com/' }
    });
    const buf = Buffer.from(dl.data);
    const filename = item.type === 'video' ? 'instagram.mp4' : 'instagram.jpg';
    try {
      return await send({ source: buf, filename });
    } catch (e2) {
      // terlalu besar / format tidak cocok -> kirim sebagai dokumen
      return ctx.replyWithDocument({ source: buf, filename }, { caption, ...replyOpt });
    }
  }
}

bot.command(['ig', 'instagram', 'igdl'], async (ctx) => {
  if (ctx.from.id.toString() !== ADMIN_ID) return;
  const raw = ctx.message.text.replace(/^\/\w+(@\w+)?\s*/i, '').trim();
  const m = raw.match(/https?:\/\/(?:www\.)?instagram\.com\/\S+/i);
  const replyOpt = { reply_to_message_id: ctx.message.message_id };
  if (!m) {
    return ctx.reply('Masukkan link Instagram setelah /ig\nContoh: /ig https://www.instagram.com/reel/xxxx/', replyOpt);
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
    await ctx.sendChatAction(media[0].type === 'video' ? 'upload_video' : 'upload_photo').catch(() => {});
    for (let i = 0; i < media.length; i++) {
      const cap = i === 0 ? `📸 Instagram${media.length > 1 ? ` (${media.length} media)` : ''}\n🔗 ${m[0]}`.slice(0, 1000) : undefined;
      await sendIgMedia(ctx, media[i], cap, replyOpt);
    }
  } catch (e) {
    console.error('[ig]', apiError(e));
    await removeWait();
    await ctx.reply('❌ Gagal mengambil media Instagram.\n' + apiError(e).slice(0, 250), replyOpt).catch(() => {});
  }
});

bot.on('text', async (ctx) => {
  if (ctx.from.id.toString() !== ADMIN_ID) return;

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
  if (ctx.session.step === 'ai_chat') {
    const prompt = ctx.message.text.trim();
    if (!prompt) return;
    await ctx.sendChatAction('typing').catch(() => {});
    const typing = setInterval(() => ctx.sendChatAction('typing').catch(() => {}), 4000);
    try {
      const wantsCode = FILE_INTENT_RE.test(prompt);
      const answer = await askAiChat(ctx.session, wantsCode ? FILE_INSTRUCTION + prompt : prompt, prompt);
      clearInterval(typing);
      await sendAiAnswer(ctx, prompt, answer);
    } catch (e) {
      clearInterval(typing);
      console.error('[ai]', apiError(e));
      await ctx.reply('❌ AI error: ' + apiError(e).slice(0, 500), aiKeyboard);
    }
    return;
  }

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
  .then((me) => {
    console.log(`✅ Token valid: @${me.username}`);
    return bot.launch();
  })
  .catch((err) => {
    console.error('❌ Gagal start bot:', err.message);
    process.exit(1);
  });
console.log('⏳ Menghubungkan ke Telegram...');

process.once('SIGINT', () => bot.stop('SIGINT'));
process.once('SIGTERM', () => bot.stop('SIGTERM'));
