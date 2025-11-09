// tools/flag-status.js
import 'dotenv/config'
import { MongoClient } from 'mongodb'

const MONGO_URI = process.env.MONGO_URI
const DB_NAME = process.env.DB_NAME || 'crypto_alert_dev'
if (!MONGO_URI) { console.log('MONGO_URI missing'); process.exit(1) }

const run = async () => {
    const c = new MongoClient(MONGO_URI)
    await c.connect()
    const db = c.db(DB_NAME)
    const doc = await db.collection('flags').findOne({ _id: 'collector' })
    console.log(JSON.stringify(doc, null, 2) || 'null')
    await c.close()
    process.exit(0)
}
run().catch(() => process.exit(1))
