// tools/env-check.js
import 'dotenv/config'
import { MongoClient } from 'mongodb'
import axios from 'axios'

const out = (k, v) => console.log(`${k}: ${v}`)

const {
  MONGO_URI,
  DB_NAME = 'crypto_alert_dev',
  TELEGRAM_BOT_TOKEN,
  WATCHER_SCRIPT,
  WATCHER_TICK_MS,
} = process.env

out('cwd', process.cwd())
out('WATCHER_SCRIPT', WATCHER_SCRIPT || '—')
out('WATCHER_TICK_MS', WATCHER_TICK_MS || '—')
out('DB_NAME', DB_NAME)
out('BOT_TOKEN_present', TELEGRAM_BOT_TOKEN ? 'yes' : 'no')

if (!MONGO_URI) {
  console.log('MONGO_URI: MISSING')
  process.exit(1)
}

try {
  const c = new MongoClient(MONGO_URI, { serverSelectionTimeoutMS: 8000 })
  await c.connect()
  console.log('Mongo: OK (connected)')
  await c.close()
} catch (e) {
  console.log('Mongo: FAIL ->', e?.message || e)
}

if (TELEGRAM_BOT_TOKEN) {
  const chatId = process.argv[2]
  if (chatId) {
    try {
      const r = await axios.post(`https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage`, {
        chat_id: Number(chatId),
        text: 'env-check: OK',
        disable_web_page_preview: true
      }, { timeout: 8000 })
      console.log('Telegram send: OK=', r?.data?.ok)
    } catch (e) {
      console.log('Telegram send: FAIL ->', e?.response?.status, e?.response?.data || e?.message)
    }
  } else {
    console.log('Telegram: skip (no chatId provided). To test: node tools/env-check.js 791785172')
  }
} else {
  console.log('Telegram: BOT_TOKEN missing')
}
