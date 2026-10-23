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

// ================= TEMP MAIL (temp-mail.app) =================
const TEMPMAIL_URL = 'https://temp-mail.app';
let cheerio = null;
try { cheerio = require('cheerio'); } catch (_) { /* opsional: npm i cheerio */ }

function newTempSession() {
  return { visitorId: crypto.randomUUID(), cookies: {}, email: null };
}

async function tempApi(sess, apiPath, params = {}) {
  const cookie = Object.entries(sess.cookies).map(([k, v]) => `${k}=${v}`).join('; ');
  const res = await axios.get(`${TEMPMAIL_URL}${apiPath}`, {
    params,
    headers: {
      'Accept': '*/*',
      'User-Agent': 'Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Mobile Safari/537.36',
      'Cache-Control': 'no-cache',
      'Visitor-Id': sess.visitorId,
      ...(cookie ? { Cookie: cookie } : {})
    },
    timeout: 20000
  });
  for (const c of res.headers['set-cookie'] || []) {
    const [pair] = c.split(';');
    const i = pair.indexOf('=');
    if (i > 0) sess.cookies[pair.slice(0, i).trim()] = pair.slice(i + 1).trim();
  }
  let data = res.data;
  if (typeof data === 'string') { try { data = JSON.parse(data); } catch (_) { throw new Error('Respon temp-mail bukan JSON'); } }
  return data;
}

function parseEmailContent(html) {
  if (!html) return { text: '', links: [] };
  if (cheerio) {
    const $ = cheerio.load(html);
    const links = [];
    $('a[href]').each((i, el) => {
      const href = $(el).attr('href');
      if (href && /^https?:\/\//i.test(href)) links.push(href);
    });
    $('script, style').remove();
    return { text: $('body').text().replace(/\n\s*\n/g, '\n').trim(), links };
  }
  // fallback tanpa cheerio
  const links = [...html.matchAll(/href=["'](https?:\/\/[^"']+)["']/gi)].map((m) => m[1]);
  const text = html.replace(/<(script|style)[\s\S]*?<\/\1>/gi, '').replace(/<[^>]+>/g, ' ').replace(/\s+/g, ' ').trim();
  return { text, links };
}

async function tempCreate(sess) {
  const r = await tempApi(sess, '/api/mail/address', { refresh: 'true', expire: '1440', part: 'main' });
  if (!r || !r.address) throw new Error('Gagal generate email');
  sess.email = r.address;
  return r.address;
}

async function tempInbox(sess) {
  const r = await tempApi(sess, '/api/mail/list', { part: 'main' });
  return (r && r.message) || [];
}

function formatInbox(email, messages) {
  let msg = `<b>📧 Temp Mail</b>\n<code>${escapeHtml(email)}</code>\n\n`;
  if (!messages.length) return msg + '📭 Inbox masih kosong. Tekan <b>Cek Inbox</b> lagi beberapa detik lagi.';
  msg += `📬 <b>${messages.length} email masuk</b>\n`;
  for (const m of messages.slice(0, 5)) {
    const { text, links } = parseEmailContent(m.content);
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
  [Markup.button.callback('📥 Cek Inbox', 'tm_check'), Markup.button.callback('🆕 Email Baru', 'tm_new')],
  [Markup.button.callback('⬅️ Menu', 'back_menu')]
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

// ================= AI CHAT (kyzznekoo - ministral-8b) =================
const AI_URL = process.env.AI_URL || 'https://app.kyzznekoo.my.id/api/ai/ministral-8b-2512';
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

const aiKeyboard = Markup.inlineKeyboard([[Markup.button.callback('⬅️ Menu', 'back_menu')]]);

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
    const messages = await tempInbox(sess);
    await safeEdit(ctx, formatInbox(sess.email, messages), tempKeyboard);
  } catch (e) {
    console.error('[tm_check]', apiError(e));
    await safeEdit(ctx, `❌ <b>Gagal cek inbox</b>\n\n<code>${escapeHtml(apiError(e))}</code>`, tempKeyboard);
  }
});

bot.action('ai_chat', async (ctx) => {
  if (ctx.from.id.toString() !== ADMIN_ID) return ctx.answerCbQuery('❌ Akses ditolak');
  await ctx.answerCbQuery();
  ctx.session.waitingForUrl = false;
  ctx.session.step = 'ai_chat';
  await safeEdit(ctx,
    `<b>🤖 AI Chat</b> (Ministral 8B)\n\nKirim pertanyaan/prompt apa saja, nanti dijawab AI.\n` +
    `Tiap pesan berdiri sendiri (tidak ingat obrolan sebelumnya). Tekan <b>Menu</b> untuk keluar.` +
    (AI_APIKEY ? '' : `\n\n⚠️ <b>AI_APIKEY</b> belum diisi di .env`),
    aiKeyboard);
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
      const answer = await askAI(prompt);
      clearInterval(typing);
      const parts = chunkText(answer);
      for (let i = 0; i < parts.length; i++) {
        await ctx.reply(parts[i], i === parts.length - 1 ? aiKeyboard : undefined);
      }
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
