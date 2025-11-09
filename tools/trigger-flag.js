// tools/trigger-flag.js
import 'dotenv/config'
import { MongoClient } from 'mongodb'

const chatId = process.argv[2] ? Number(process.argv[2]) : null
if (!chatId || !Number.isFinite(chatId)) {
    console.log('ChatId is required, e.g.: node tools/trigger-flag.js 791785172')
    process.exit(1)
}

const MONGO_URI = process.env.MONGO_URI
const DB_NAME = process.env.DB_NAME || 'crypto_alert_dev'
if (!MONGO_URI) process.exit(1)

const run = async () => {
    const c = new MongoClient(MONGO_URI)
    await c.connect()
    const db = c.db(DB_NAME)
    await db.collection('flags').updateOne(
        { _id: 'collector' },
        { $set: { run: true, notifyChatId: chatId, requestedAt: new Date() } },
        { upsert: true }
    )
    await c.close()
    console.log('OK: flag set for chat', chatId)
    process.exit(0)
}

run().catch(() => process.exit(1))
