// collector/src/watcher.js
import 'dotenv/config'
import { MongoClient } from 'mongodb'
import { spawn } from 'child_process'
import path from 'path'
import axios from 'axios'

const MONGO_URI = process.env.MONGO_URI
const DB_NAME = process.env.DB_NAME || 'crypto_alert_dev'
const BOT_TOKEN = process.env.TELEGRAM_BOT_TOKEN || null

const TICK_MS = Number(process.env.WATCHER_TICK_MS || 60_000)
const PERIOD_MS = Number(process.env.WATCHER_PERIOD_MS || 15 * 60_000)
const TARGET_SCRIPT = process.env.WATCHER_SCRIPT || 'src/index.js'

const FLAG_COLL = process.env.WATCH_FLAG_COLL || 'flags'
const FLAG_ID = process.env.WATCH_FLAG_ID || 'collector'
const FLAG_FIELD = process.env.WATCH_FLAG_FIELD || 'run'
const FLAG_NOTIFY_FIELD = process.env.WATCH_FLAG_NOTIFY_FIELD || 'notifyChatId'

let running = false
let lastScheduledAt = 0

function sendTG(chatId, text) {
    if (!BOT_TOKEN || !chatId) return Promise.resolve()
    return axios.post(
        `https://api.telegram.org/bot${BOT_TOKEN}/sendMessage`,
        { chat_id: chatId, text, disable_web_page_preview: true },
        { timeout: 8000 }
    ).catch(() => {})
}

function runTarget(meta, reason, onExit) {
    if (running) return
    running = true
    lastScheduledAt = Date.now()

    const chatId = meta?.notifyChatId || null
    // Диагностическая одна строка — поможет понять, какая ветка сработала
    console.log(`[watcher] run ${reason} chat=${chatId ?? '-'}`)

    if (chatId) sendTG(chatId, '⏳ Сбор данных начат')

    const proc = spawn(process.execPath, [path.resolve(TARGET_SCRIPT)], { stdio: 'inherit' })
    proc.on('exit', (code) => {
        running = false
        if (chatId) {
            if (code === 0) sendTG(chatId, '✅ Данные обновлены')
            else sendTG(chatId, '❌ Обновление не удалось')
        }
        if (onExit) onExit(code, meta)
    })
}

async function tryRunByJob(db) {
    try {
        const jobs = db.collection('jobs')
        const job = await jobs.findOneAndUpdate(
            { type: 'collect', status: 'pending' },
            { $set: { status: 'running', startedAt: new Date() } },
            { sort: { createdAt: 1 }, returnDocument: 'after' }
        )
        if (job?.value) {
            const j = job.value
            runTarget({ notifyChatId: j.notifyChatId || null }, 'job', async (code) => {
                const $set = { finishedAt: new Date(), exitCode: code }
                $set.status = code === 0 ? 'done' : 'failed'
                await jobs.updateOne({ _id: j._id }, { $set }).catch(() => {})
            })
            return true
        }
    } catch {}
    return false
}

async function tryRunByFlag(db) {
    try {
        const res = await db.collection(FLAG_COLL).findOneAndUpdate(
            { _id: FLAG_ID, [FLAG_FIELD]: true },
            { $set: { [FLAG_FIELD]: false, lastTriggeredAt: new Date() }, $unset: { [FLAG_NOTIFY_FIELD]: '' } },
            { returnDocument: 'before' }
        )
        if (res?.value) {
            const meta = { notifyChatId: res.value[FLAG_NOTIFY_FIELD] || null }
            runTarget(meta, 'flag', () => {})
            return true
        }
    } catch {}
    return false
}

async function tick(client) {
    if (running) return
    const db = client.db(DB_NAME)

    if (await tryRunByJob(db)) return
    if (await tryRunByFlag(db)) return

    const now = Date.now()
    if (now - lastScheduledAt >= PERIOD_MS) runTarget({}, 'periodic', () => {})
}

;(async () => {
    if (!MONGO_URI) { process.exit(1) }
    const client = new MongoClient(MONGO_URI, { maxPoolSize: 3 })
    await client.connect()
    await tick(client)
    setInterval(() => { tick(client).catch(() => {}) }, TICK_MS)
})().catch(() => { process.exit(1) })
