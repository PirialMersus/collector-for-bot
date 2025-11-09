// tools/last-snapshot.js
import 'dotenv/config'
import { MongoClient } from 'mongodb'

const MONGO_URI = process.env.MONGO_URI
const DB_NAME = process.env.DB_NAME || 'crypto_alert_dev'
const COLLECTION = process.env.COLLECTION || 'marketSnapshots'
if (!MONGO_URI) { console.log('MONGO_URI missing'); process.exit(1) }

const run = async () => {
    const c = new MongoClient(MONGO_URI)
    await c.connect()
    const db = c.db(DB_NAME)
    const doc = await db.collection(COLLECTION).find().sort({ _id: -1 }).limit(1).next()
    if (!doc) { console.log('no snapshots'); process.exit(0) }
    const t = doc.snapshotAt || doc.createdAt || doc.computedAt || doc._id?.getTimestamp?.() || null
    console.log('last:', t, '| _id:', String(doc._id))
    await c.close()
    process.exit(0)
}
run().catch(() => process.exit(1))
