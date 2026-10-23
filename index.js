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
bot.use(session({ defaultSession: () => ({ waitingForUrl: false }) }));

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

const mainMenu = Markup.inlineKeyboard([
  [Markup.button.callback('🔍 Find Preset AM', 'find_preset')],
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
    `Kalau error, coba link lain atau hubungi admin.`,
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

bot.on('text', async (ctx) => {
  if (ctx.from.id.toString() !== ADMIN_ID) return;
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
