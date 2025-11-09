// tools/ping-bot.js
import 'dotenv/config'
import axios from 'axios'

const token = process.env.TELEGRAM_BOT_TOKEN
const chatId = process.argv[2]
const text = process.argv.slice(3).join(' ') || 'ping'
if (!token) { console.log('TELEGRAM_BOT_TOKEN missing'); process.exit(1) }
if (!chatId) { console.log('usage: node tools/ping-bot.js <chatId> [text]'); process.exit(1) }

axios.post(`https://api.telegram.org/bot${token}/sendMessage`, { chat_id: Number(chatId), text }, { timeout: 8000 })
    .then(r => { console.log('ok:', r.data.ok); process.exit(0) })
    .catch(e => { console.log('err:', e?.response?.status, e?.response?.data || e.message); process.exit(1) })
