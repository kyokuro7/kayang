require('dotenv').config();
const { Telegraf, Markup, session } = require('telegraf');
const axios = require('axios');
const crypto = require('crypto');

const BOT_TOKEN = process.env.BOT_TOKEN;
const ADMIN_ID = process.env.ADMIN_ID;
const BASE_URL = process.env.BASE_URL || 'https://amfinder.web.id';
const DID = process.env.DID || '267f6fd3-34f4-4d9a-8d90-5aa28db8922b';

if (!BOT_TOKEN || !ADMIN_ID) {
  console.error('❌ BOT_TOKEN dan ADMIN_ID wajib diisi di .env');
  process.exit(1);
}

const bot = new Telegraf(BOT_TOKEN);

bot.use(session());

const loadingFrames = ['⠋', '⠙', '⠹', '⠸', '⠼', '⠴', '⠦', '⠧', '⠇', '⠏'];
const loadingEmojis = ['🔍', '🔎', '🕵️', '🔍', '🔎', '🕵️'];

async function computeProof(saltHex, messageStr, rounds) {
  const saltBuffer = Buffer.from(saltHex, 'hex');
  const key = await crypto.subtle.importKey(
    'raw',
    saltBuffer,
    { name: 'HMAC', hash: 'SHA-256' },
    false,
    ['sign']
  );

  let data = new TextEncoder().encode(messageStr);
  const actualRounds = Math.max(1, Math.min(10000, Math.floor(rounds)));

  for (let i = 0; i < actualRounds; i++) {
    const signature = await crypto.subtle.sign('HMAC', key, data);
    data = new Uint8Array(signature);
  }
  return Array.from(data, byte => byte.toString(16).padStart(2, '0')).join('');
}

async function scrapePreset(targetUrl) {
  try {
    const challengeRes = await axios.post(`${BASE_URL}/api/v/2b51359f`, { did: DID }, {
      headers: {
        'Content-Type': 'application/json',
        'Origin': BASE_URL,
        'Referer': `${BASE_URL}/`,
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
      },
      timeout: 15000
    });

    const { cid, salt, ts, rounds } = challengeRes.data;
    const messageToSign = `${cid}|${DID}|${ts}`;
    const proof = await computeProof(salt, messageToSign, rounds);

    const verifyRes = await axios.post(`${BASE_URL}/api/v/2b51359f`, {
      cid, did: DID, ts, proof, score: 0, reasons: []
    }, {
      headers: {
        'Content-Type': 'application/json',
        'Origin': BASE_URL,
        'Referer': `${BASE_URL}/`,
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
      },
      timeout: 15000
    });

    const token = verifyRes.data.token;
    if (!token) throw new Error('Gagal mendapatkan token valid!');

    const sseUrl = `${BASE_URL}/api/find?url=${encodeURIComponent(targetUrl)}&bat=88&chg=0&t=${encodeURIComponent(token)}`;
    
    const sseRes = await axios.get(sseUrl, {
      responseType: 'stream',
      headers: {
        'Accept': 'text/event-stream',
        'Referer': `${BASE_URL}/`,
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
      },
      timeout: 30000
    });

    return new Promise((resolve, reject) => {
      let rawData = '';
      sseRes.data.on('data', (chunk) => {
        rawData += chunk.toString();
      });

      sseRes.data.on('end', () => {
        const lines = rawData.split('\n');
        let currentEvent = '';
        let finalJson = null;

        for (let i = 0; i < lines.length; i++) {
          let line = lines[i].trim();
          if (line.startsWith('event:')) {
            currentEvent = line.replace('event:', '').trim();
          } else if (line.startsWith('data:')) {
            let dataContent = line.replace('data:', '').trim();
            if (currentEvent === 'result') {
              try {
                finalJson = JSON.parse(dataContent);
              } catch (e) {}
            }
          }
        }

        if (finalJson) {
          resolve(finalJson);
        } else {
          reject(new Error('Gagal memparsing hasil SSE'));
        }
      });

      sseRes.data.on('error', (err) => reject(err));
    });
  } catch (error) {
    throw new Error(error.response ? JSON.stringify(error.response.data) : error.message);
  }
}

function formatResult(data) {
  let msg = '<b>😆 PRESET KETEMU BOSQ ✨</b>\n\n';
  
  if (data.alight_creative_url) {
    msg += `<b>😼 ${data.alight_creative_size || '? MB'} :</b> <a href="${data.alight_creative_url}">Link Alight Creative</a>\n`;
  }
  if (data.xml_url) {
    msg += `<b>🔥 XML :</b> <a href="${data.xml_url}">Link XML Google Drive</a>\n`;
  }
  if (data.url && data.url !== data.alight_creative_url && data.url !== data.xml_url) {
    msg += `<b>📱 URL  :</b> <a href="${data.url}">${data.url}</a>\n`;
  }
  if (data.thumbnail) {
    msg += `\n<b>🖼 Thumbnail:</b> <a href="${data.thumbnail}">Lihat</a>`;
  }
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
  if (!ctx.session.waitingForUrl) return;
  
  const url = ctx.message.text.trim();
  const urlRegex = /(https?:\/\/[^\s]+)/;
  const match = url.match(urlRegex);
  
  if (!match) {
    return ctx.reply('❌ Link tidak valid. Kirim link TikTok/YouTube yang benar.');
  }
  
  const targetUrl = match[0];
  ctx.session.waitingForUrl = false;
  
  const { sent, interval } = await showLoadingAnimation(ctx, `Mencari preset dari ${targetUrl}...`);
  
  try {
    const result = await scrapePreset(targetUrl);
    clearInterval(interval);
    
    if (result.status === false || !result.alight_creative_url && !result.xml_url) {
      await ctx.telegram.editMessageText(
        ctx.chat.id,
        sent.message_id,
        null,
        `❌ <b>Preset tidak ditemukan</b>\n\n` +
        `Link: ${targetUrl}\n` +
        `Pesan: ${result.message || 'Tidak ada preset di link tersebut'}`,
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
      `<code>${error.message}</code>\n\n` +
      `Coba link lain atau hubungi admin.`,
      { parse_mode: 'HTML', ...Markup.inlineKeyboard([[Markup.button.callback('🔄 Coba Lagi', 'find_preset'), Markup.button.callback('⬅️ Menu', 'back_menu')]]) }
    );
  }
});

bot.launch().then(() => console.log('✅ Bot jalan...')).catch(console.error);

process.once('SIGINT', () => bot.stop('SIGINT'));
process.once('SIGTERM', () => bot.stop('SIGTERM'));
