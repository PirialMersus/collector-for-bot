// collector/tools/trigger-flag.js
import 'dotenv/config'
import { MongoClient } from 'mongodb'

const chatId = process.argv[2] ? Number(process.argv[2]) : null
if (!chatId || !Number.isFinite(chatId)) {
    console.log('ChatId is required, e.g.: node tools/trigger-flag.js 791785172')
    process.exit(1)
}

const MONGO_URI = process.env.MONGO_URI
const DB_NAME = process.env.DB_NAME || 'crypto_alert_dev'
const FLAG_COLL = process.env.WATCH_FLAG_COLL || 'flags'
const FLAG_ID = process.env.WATCH_FLAG_ID || 'collector'
if (!MONGO_URI) process.exit(1)

function makeToken() {
    const r = Math.random().toString(36).slice(2, 10)
    return `${Date.now()}_${r}`
}

const run = async () => {
    const c = new MongoClient(MONGO_URI)
    await c.connect()
    const db = c.db(DB_NAME)
    const token = makeToken()
    await db.collection(FLAG_COLL).updateOne(
      { _id: FLAG_ID },
      { $set: { run: true, notifyChatId: chatId, requestedAt: new Date(), token } },
      { upsert: true }
    )
    await c.close()
    console.log(`OK: flag(${FLAG_ID}) set for chat ${chatId} token ${token}`)
    process.exit(0)
}
run().catch(() => process.exit(1))
