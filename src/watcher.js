// collector/src/watcher.js
import 'dotenv/config'
import { MongoClient } from 'mongodb'
import { spawn } from 'child_process'
import path from 'path'
import axios from 'axios'
import os from 'os'

const MONGO_URI = process.env.MONGO_URI
const DB_NAME = process.env.DB_NAME || 'crypto_alert_dev'
const BOT_TOKEN = process.env.TELEGRAM_BOT_TOKEN || null

const TICK_MS = Number(process.env.WATCHER_TICK_MS || 10_000)
const PERIOD_MS = Number(process.env.WATCHER_PERIOD_MS || 900_000)
const TARGET_SCRIPT = process.env.WATCHER_SCRIPT || 'src/index.js'

const FLAG_COLL = process.env.WATCH_FLAG_COLL || 'flags'
const FLAG_ID = process.env.WATCHER_FLAG_ID || process.env.WATCH_FLAG_ID || 'collector_mac'
const FLAG_FIELD = process.env.WATCH_FLAG_FIELD || 'run'
const FLAG_NOTIFY_FIELD = process.env.WATCH_FLAG_NOTIFY_FIELD || 'notifyChatId'

let running = false
let tickCount = 0
let lastPrintedState = null
let lastPingAt = 0
let lastPeriodicAt = Date.now() - PERIOD_MS

function maskToken(token) {
    if (!token || token.length < 8) return token ? '***' : '—'
    return token.slice(0, 4) + '***' + token.slice(-4)
}

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
    const chatId = meta?.notifyChatId || null
    const resolved = path.resolve(TARGET_SCRIPT)
    console.log(`[watcher] run=${reason} flagId=${FLAG_ID} pid=${process.pid} host=${os.hostname()} cwd=${process.cwd()} script=${resolved} chat=${chatId ?? '-'}`)
    if (chatId) sendTG(chatId, '⏳ Сбор данных начат')
    const proc = spawn(process.execPath, [resolved], { stdio: 'inherit' })
    proc.on('error', (err) => { if (chatId) sendTG(chatId, `❌ Не удалось запустить сбор: ${err?.message || err}`) })
    proc.on('exit', (code) => {
        running = false
        lastPeriodicAt = Date.now()
        if (chatId) { if (code === 0) sendTG(chatId, '✅ Данные обновлены'); else sendTG(chatId, `❌ Обновление не удалось (code=${code})`) }
        if (onExit) onExit(code, meta)
    })
}

async function tryRunByFlag(db) {
    const doc = await db.collection(FLAG_COLL).findOne({ _id: FLAG_ID }).catch(() => null)
    const token = doc?.token || null
    const tokenShort = token ? `${String(token).slice(0, 6)}…` : '-'
    const stateStr = doc
      ? `id=${FLAG_ID} run=${!!doc[FLAG_FIELD]} notify=${doc[FLAG_NOTIFY_FIELD] ?? '-'} token=${tokenShort} updated=${doc?.lastTriggeredAt ?? doc?.requestedAt ?? '-'}`
      : `id=${FLAG_ID} missing`
    const shouldPrint = !lastPrintedState || lastPrintedState !== stateStr || (tickCount % 10 === 0)
    if (shouldPrint) { console.log(`[watcher] flagState ${stateStr}`); lastPrintedState = stateStr }

    if (!doc || doc[FLAG_FIELD] !== true || running) return false

    const now = new Date()
    const metaNotify = doc[FLAG_NOTIFY_FIELD] || null
    console.log(`[watcher] flag: trying to consume id=${FLAG_ID} token=${tokenShort}`)
    const upd = await db.collection(FLAG_COLL).updateOne(
      { _id: FLAG_ID, [FLAG_FIELD]: true, token },
      {
          $set: {
              [FLAG_FIELD]: false,
              lastTriggeredAt: now,
              lastTriggeredByHost: os.hostname(),
              lastTriggeredByPid: process.pid,
              lastTriggeredByWhen: now,
              consumedToken: token || null
          },
          $unset: { [FLAG_NOTIFY_FIELD]: '' }
      }
    ).catch(() => ({ matchedCount: 0 }))

    if (upd && upd.matchedCount === 1) {
        console.log(`[watcher] flag: consumed id=${FLAG_ID} pid=${process.pid} host=${os.hostname()} notify=${metaNotify ?? '-'} token=${tokenShort}`)
        runTarget({ notifyChatId: metaNotify }, 'flag', () => {})
        return true
    } else {
        console.log('[watcher] flag: nothing consumed (token mismatch or condition not matched)')
    }
    return false
}

async function tryRunPeriodic() {
    const now = Date.now()
    if (running) return false
    if (now - lastPeriodicAt < PERIOD_MS) return false
    runTarget({}, 'periodic', () => {})
    return true
}

async function tick(client) {
    if (running) return
    const db = client.db(DB_NAME)
    const now = Date.now()
    if (now - lastPingAt >= 60_000) { lastPingAt = now; try { await client.db(DB_NAME).admin().ping() } catch {} }
    const byFlag = await tryRunByFlag(db)
    if (!byFlag) await tryRunPeriodic()
}

;(async () => {
    if (!MONGO_URI) { console.log('[watcher] ERROR: MONGO_URI missing'); process.exit(1) }
    const client = new MongoClient(MONGO_URI, { maxPoolSize: 3, serverSelectionTimeoutMS: 8000 })
    await client.connect()
    const resolvedScript = path.resolve(TARGET_SCRIPT)
    const tokenOk = BOT_TOKEN ? 'ok' : 'missing'
    console.log(`[watcher] UP pid=${process.pid} host=${os.hostname()} cwd=${process.cwd()} script=${resolvedScript} tick=${TICK_MS}ms period=${PERIOD_MS}ms db=${DB_NAME} botToken=${tokenOk}(${maskToken(BOT_TOKEN)}) flagId=${FLAG_ID}`)
    await tick(client)
    setInterval(async () => {
        tickCount++
        try { await tick(client) } catch {}
        if ((tickCount % Math.max(1, Math.round(60_000 / Math.max(1, TICK_MS)))) === 0) {
            console.log(`[watcher] heartbeat pid=${process.pid} running=${running} time=${new Date().toISOString()}`)
        }
    }, TICK_MS)
    await new Promise(() => {})
})().catch((e) => { console.log('[watcher] FATAL:', e?.message || e); process.exit(1) })
