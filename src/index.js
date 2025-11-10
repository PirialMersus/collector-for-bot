// collector/src/index.js
import 'dotenv/config'
import axios from 'axios'
import { MongoClient } from 'mongodb'
import path from 'path'

const MONGO_URI = process.env.MONGO_URI
const DB_NAME = process.env.DB_NAME || 'crypto_alert_dev'
const COLLECTION = process.env.COLLECTION || 'marketSnapshots'
const SYMBOLS = (process.env.SYMBOLS || 'BTC,ETH,PAXG').split(',').map(s => s.trim().toUpperCase()).filter(Boolean)
const HEALTHCHECK_URL = process.env.HEALTHCHECK_URL || 'https://hc-ping.com/43b9b4a0-ea48-4826-b460-87015ba68be0'
const COINGECKO_KEY = process.env.COINGECKO_KEY || null
const OI_CVD_PERIOD = process.env.OI_CVD_PERIOD || '5m'
const OI_CVD_LIMIT = process.env.OI_CVD_LIMIT ? parseInt(process.env.OI_CVD_LIMIT, 10) : 6
const CAP_TOP_SCAN_MAX = Number(process.env.CAP_TOP_SCAN_MAX ?? 120)
const CAP_TOP_CONCURRENCY = Number(process.env.CAP_TOP_CONCURRENCY ?? 8)

const TIMEOUT_MS = 10000
const http = axios.create({ timeout: TIMEOUT_MS, headers: { 'User-Agent': 'crypto-alert-bot/1.0', 'Accept': 'application/json' } })

const ids = { BTC: 'bitcoin', ETH: 'ethereum', PAXG: 'pax-gold' }
const binance = { BTC: 'BTCUSDT', ETH: 'ETHUSDT' }

const BINANCE_FAPI_BASES = [
    'https://fapi.binance.com',
    'https://fapi1.binance.com',
    'https://fapi2.binance.com',
    'https://fapi3.binance.com'
]

const sleep = (ms) => new Promise(r => setTimeout(r, ms))
const nearZero = v => Number.isFinite(v) && Math.abs(v) < 1e-8
const isNum = v => Number.isFinite(Number(v)) && Number(v) !== 0
const avg = arr => arr.length ? arr.reduce((a, b) => a + b, 0) / arr.length : null

async function withRetry(fn, tries = 2, pauseMs = 400) {
    let last
    for (let i = 0; i < tries; i++) {
        try { return await fn() } catch (e) {
            last = e
            console.log('[retry]', i + 1, '/', tries, '-', e?.message || e)
            if (i + 1 < tries) await sleep(pauseMs)
        }
    }
    throw last
}

async function getFromRotatingBases(bases, path, params) {
    for (const base of bases) {
        try { const { data } = await http.get(`${base}${path}`, { params }); return data } catch {}
    }
    throw new Error('all_bases_failed')
}

const cgHttp = axios.create({ baseURL: 'https://api.coingecko.com', timeout: 9000, headers: { 'User-Agent': 'crypto-alert-bot/1.0', 'Accept': 'application/json' } })
const cgCache = new Map()
let cgQueue = Promise.resolve()
let lastCgAt = 0
const CG_MIN_INTERVAL_MS = 800

function buildCgKey(path, params) {
    const p = params ? ('?' + Object.entries(params).sort((a,b)=>a[0].localeCompare(b[0])).map(([k,v])=>`${k}=${v}`).join('&')) : ''
    return path + p
}

async function cgRequest(path, { params, retries = 2 } = {}) {
    const key = buildCgKey(path, params)
    if (cgCache.has(key)) return cgCache.get(key)
    cgQueue = cgQueue.then(async () => {
        const now = Date.now()
        const wait = Math.max(0, CG_MIN_INTERVAL_MS - (now - lastCgAt))
        if (wait) await sleep(wait)
        lastCgAt = Date.now()
        const headers = {}
        if (COINGECKO_KEY) headers['x-cg-demo-api-key'] = COINGECKO_KEY
        const res = await withRetry(() => cgHttp.get(path, { params, headers }), retries, 500)
        const data = res?.data
        cgCache.set(key, data)
        return data
    })
    return cgQueue
}

async function cgMarkets(idsArr) {
    const data = await cgRequest('/api/v3/coins/markets', { params: { vs_currency: 'usd', ids: idsArr.join(','), order: 'market_cap_desc', per_page: 250, page: 1, sparkline: false, price_change_percentage: '24h' }, retries: 2 }).catch(() => [])
    return Array.isArray(data) ? data : []
}

async function cgChart(id, days) {
    const data = await cgRequest(`/api/v3/coins/${id}/market_chart`, { params: { vs_currency: 'usd', days, interval: 'daily' }, retries: 2 }).catch(() => null)
    return data || {}
}

function rsi14FromCloses(closes) {
    const p = 14
    if (!Array.isArray(closes) || closes.length < p + 1) return null
    let gains = 0, losses = 0
    for (let i = 1; i <= p; i++) { const d = closes[i] - closes[i - 1]; if (d > 0) gains += d; else losses += -d }
    let ag = gains / p, al = losses / p
    for (let i = p + 1; i < closes.length; i++) { const d = closes[i] - closes[i - 1]; ag = ((ag * (p - 1)) + Math.max(0, d)) / p; al = ((al * (p - 1)) + Math.max(0, -d)) / p }
    if (al === 0) return 100
    const rs = ag / al
    const rsi = 100 - (100 / (1 + rs))
    return Number.isFinite(rsi) ? Number(rsi.toFixed(2)) : null
}

async function binanceTicker24h(sym) {
    try {
        const data = await getFromRotatingBases(BINANCE_FAPI_BASES, '/fapi/v1/ticker/24hr', { symbol: sym })
        const price = Number(data?.lastPrice)
        const pct24 = Number(data?.priceChangePercent)
        const volQuote = Number(data?.quoteVolume)
        return { price: Number.isFinite(price) ? price : null, pct24: Number.isFinite(pct24) ? pct24 : null, vol24: Number.isFinite(volQuote) ? volQuote : null }
    } catch { return { price: null, pct24: null, vol24: null } }
}

async function binanceFundingSeries(sym, limit = 24) {
    try {
        const data = await getFromRotatingBases(BINANCE_FAPI_BASES, '/fapi/v1/fundingRate', { symbol: sym, limit })
        const arr = Array.isArray(data) ? data : []
        return arr.map(x => Number(x.fundingRate)).filter(v => Number.isFinite(v) && !nearZero(v))
    } catch { return [] }
}

async function binanceFundingSeriesWWW(sym, limit = 48) {
    try {
        const url = 'https://www.binance.com/futures/data/fundingRate'
        const { data } = await http.get(url, { params: { symbol: sym, limit } })
        const arr = Array.isArray(data) ? data : []
        return arr.map(x => Number(x.fundingRate)).filter(v => Number.isFinite(v) && !nearZero(v))
    } catch { return [] }
}

async function binancePremiumIndex(sym) {
    try {
        const data = await getFromRotatingBases(BINANCE_FAPI_BASES, '/fapi/v1/premiumIndex', { symbol: sym })
        const v = Number(data?.lastFundingRate)
        return Number.isFinite(v) && !nearZero(v) ? v : null
    } catch { return null }
}

async function bybitFunding(sym) {
    try {
        const url = 'https://api.bybit.com/v5/market/funding/history'
        const { data } = await http.get(url, { params: { category: 'linear', symbol: sym, limit: 16 } })
        const arr = Array.isArray(data?.result?.list) ? data.result.list : []
        return arr.map(x => Number(x?.fundingRate)).filter(v => Number.isFinite(v) && !nearZero(v))
    } catch { return [] }
}

async function okxFunding(instId) {
    try {
        const url = 'https://www.okx.com/api/v5/public/funding-rate'
        const { data } = await http.get(url, { params: { instId } })
        const rec = Array.isArray(data?.data) ? data.data[0] : null
        const v = Number(rec?.fundingRate)
        return Number.isFinite(v) && !nearZero(v) ? v : null
    } catch { return null }
}

function deriveLsFromRatio(ratio) {
    if (!Number.isFinite(ratio) || ratio <= 0) return null
    const longPct = (ratio / (1 + ratio)) * 100
    const shortPct = 100 - longPct
    return { longPct: Number(longPct.toFixed(2)), shortPct: Number(shortPct.toFixed(2)), ls: Number(ratio.toFixed(2)) }
}

function isLSValid(r) { return r && Number.isFinite(r.longPct) && Number.isFinite(r.shortPct) && r.longPct >= 0 && r.shortPct >= 0 }

async function binanceGlobalLS(symbol, period, limit = 30) {
    try {
        const data = await getFromRotatingBases(BINANCE_FAPI_BASES, '/futures/data/globalLongShortAccountRatio', { symbol, period, limit })
        const arr = Array.isArray(data) ? data : []
        if (!arr.length) return null
        const last = arr[arr.length - 1]
        const ratio = Number(last?.longShortRatio)
        const longAccount = Number(last?.longAccount)
        const shortAccount = Number(last?.shortAccount)
        if (Number.isFinite(longAccount) && Number.isFinite(shortAccount) && (longAccount + shortAccount) > 0) {
            const sum = longAccount + shortAccount
            return { longPct: Number(((longAccount / sum) * 100).toFixed(2)), shortPct: Number(((shortAccount / sum) * 100).toFixed(2)), ls: Number((ratio && Number.isFinite(ratio) ? ratio : (longAccount / shortAccount)).toFixed(2)) }
        }
        return deriveLsFromRatio(ratio)
    } catch { return null }
}

async function binanceTopAccountsLS(symbol, period = '4h', limit = 30) {
    try {
        const data = await getFromRotatingBases(BINANCE_FAPI_BASES, '/futures/data/topLongShortAccountRatio', { symbol, period, limit })
        const arr = Array.isArray(data) ? data : []
        if (!arr.length) return null
        const last = arr[arr.length - 1]
        const ratio = Number(last?.longShortRatio)
        return deriveLsFromRatio(ratio)
    } catch { return null }
}

async function binanceTopPositionsLS(symbol, period = '4h', limit = 30) {
    try {
        const data = await getFromRotatingBases(BINANCE_FAPI_BASES, '/futures/data/topLongShortPositionRatio', { symbol, period, limit })
        const arr = Array.isArray(data) ? data : []
        if (!arr.length) return null
        const last = arr[arr.length - 1]
        const ratio = Number(last?.longShortRatio)
        return deriveLsFromRatio(ratio)
    } catch { return null }
}

async function bybitLS(symbol) {
    try {
        const url = 'https://api.bybit.com/v5/market/account-ratio'
        const { data } = await http.get(url, { params: { category: 'linear', symbol, period: 1800 } })
        const arr = Array.isArray(data?.result?.list) ? data.result.list : []
        const last = arr[arr.length - 1] || null
        const ratio = Number(last?.longShortRatio)
        return deriveLsFromRatio(ratio)
    } catch { return null }
}

async function fetchLongShortAgg(symbolKey) {
    const sym = binance[symbolKey]
    if (!sym) return { data: null, src: null }
    const attempts = [
        async () => ({ data: await binanceGlobalLS(sym, '1h'), src: 'binance_global_1h' }),
        async () => ({ data: await binanceGlobalLS(sym, '4h'), src: 'binance_global_4h' }),
        async () => ({ data: await binanceGlobalLS(sym, '30m'), src: 'binance_global_30m' }),
        async () => ({ data: await binanceTopAccountsLS(sym, '4h'), src: 'binance_top_acc_4h' }),
        async () => ({ data: await binanceTopPositionsLS(sym, '4h'), src: 'binance_top_pos_4h' }),
        async () => ({ data: await bybitLS(sym), src: 'bybit_30m' })
    ]
    for (const f of attempts) { try { const { data, src } = await f(); if (isLSValid(data)) return { data, src } } catch {} }
    return { data: null, src: null }
}

async function fearGreed() {
    try {
        const url = 'https://api.alternative.me/fng/'
        const { data } = await http.get(url, { params: { limit: 1 } })
        const it = Array.isArray(data?.data) ? data.data[0] : null
        if (!it) return { value: null, classification: null, ts: null }
        const value = Number(it.value)
        const ts = it.timestamp ? Number(it.timestamp) * 1000 : null
        const cl = it.value_classification || null
        return { value: Number.isFinite(value) ? value : null, classification: cl, ts: Number.isFinite(ts) ? ts : null }
    } catch { return { value: null, classification: null, ts: null } }
}

function normPoint(p) {
    const ts = Number(p?.timestamp ?? p?.t ?? p?.time ?? p?.date ?? p?.x ?? 0)
    const pick = (...vals) => { for (const v of vals) { const n = Number(v); if (Number.isFinite(n)) return n } return null }
    const native = pick(p?.balance, p?.amount, p?.qty, p?.tokenBalance, p?.asset, p?.value_native, p?.native, p?.n)
    const usd = pick(p?.usd, p?.usd_value, p?.value_usd, p?.valueUSD, p?.totalUSD, p?.y, p?.value, p?.usdValue, p?.sumUSD)
    return { ts, native, usd }
}

function readSeriesForSymbol(data, symbol) {
    symbol = String(symbol).toUpperCase()
    const out = []
    const add = arr => { for (const p of (arr || [])) { const pt = normPoint(p); if (Number.isFinite(pt.ts)) out.push(pt) } }
    add(Array.isArray(data?.tokens) ? data.tokens : [])
    add(Array.isArray(data?.assets) ? data.assets : [])
    add(Array.isArray(data?.data) ? data.data : [])
    add(Array.isArray(data?.series) ? data.series : [])
    if (Array.isArray(data?.charts)) {
        for (const ch of data.charts) {
            if ((ch?.symbol || ch?.token || ch?.name || '').toUpperCase() === symbol) add(ch?.data || [])
        }
    }
    return out.filter(pt => Number.isFinite(pt.ts) && (Number.isFinite(pt.native) || Number.isFinite(pt.usd))).sort((a, b) => a.ts - b.ts)
}

async function loadExchangeDataset(slug) {
    const urls = [ `https://api.llama.fi/cex/reserves/${slug}`, `https://preview.dl.llama.fi/cex/${slug}` ]
    for (const url of urls) { try { const { data } = await http.get(url, { params: { _t: Date.now() } }); if (data) return data } catch {} }
    return null
}

async function fetchCexDeltasTwoWindows(slug, symbol) {
    const data = await loadExchangeDataset(slug)
    if (!data) return { nowUSD: null, prevUSD: null }
    const series = readSeriesForSymbol(data, symbol)
    if (!series.length) return { nowUSD: null, prevUSD: null }
    const last = series[series.length - 1]
    const target1 = last.ts - 24 * 3600 * 1000
    const target2 = last.ts - 48 * 3600 * 1000
    let p1 = series[0], b1 = Infinity, p2 = series[0], b2 = Infinity
    for (const p of series) { const d1 = Math.abs(p.ts - target1); if (d1 < b1) { b1 = d1; p1 = p }; const d2 = Math.abs(p.ts - target2); if (d2 < b2) { b2 = d2; p2 = p } }
    const toUSD = (valUSD) => Number.isFinite(valUSD) ? valUSD : null
    const now = toUSD(last.usd) - toUSD(p1.usd)
    const prev = toUSD(p1.usd) - toUSD(p2.usd)
    return { nowUSD: Number.isFinite(now) ? now : null, prevUSD: Number.isFinite(prev) ? prev : null }
}

function kyivIso(ts) {
    try { return new Intl.DateTimeFormat('ru-RU', { timeZone: 'Europe/Kyiv', year: 'numeric', month: '2-digit', day: '2-digit', hour: '2-digit', minute: '2-digit' }).format(new Date(ts)) }
    catch { return new Date(ts).toISOString() }
}

async function fetchBtcDominanceNow() {
    let val = null
    try {
        const data = await cgRequest('/api/v3/global', { retries: 2 })
        const cg = data?.data?.market_cap_percentage?.btc
        const cgNum = Number(cg)
        if (Number.isFinite(cgNum) && cgNum > 0 && cgNum <= 100) val = cgNum
        console.log('[dom.cg] now=', val ?? null)
    } catch (e) { console.log('[dom.cg] fail', e?.message || e) }
    if (!Number.isFinite(val)) {
        try {
            const r2 = await http.get('https://api.coinlore.net/api/global/', { timeout: 7000 })
            const arr = Array.isArray(r2?.data) ? r2.data : null
            const cl = arr && arr[0] ? Number(arr[0].btc_d ?? arr[0].bitcoin_dominance_percentage ?? arr[0].btc_dominance) : null
            if (Number.isFinite(cl) && cl > 0 && cl <= 100) val = cl
            console.log('[dom.coinlore] now=', val ?? null)
        } catch (e) { console.log('[dom.coinlore] fail', e?.message || e) }
    }
    return Number.isFinite(val) ? Number(val.toFixed(2)) : null
}

function parseStooqCsvForChange(csv) {
    const s = String(csv || '').trim()
    const rows = s.split('\n').filter(Boolean)
    if (rows.length < 3) return null
    const last = rows[rows.length - 1].split(',')
    const prev = rows[rows.length - 2].split(',')
    const closeLast = Number(last[4])
    const closePrev = Number(prev[4])
    if (!Number.isFinite(closeLast) || !Number.isFinite(closePrev) || closePrev === 0) return null
    const pctDay = ((closeLast - closePrev) / closePrev) * 100
    return { price: closeLast, pct: pctDay }
}

async function fetchSpxNow() {
    let price = null, src = null
    try {
        const r = await http.get('https://query1.finance.yahoo.com/v7/finance/quote?symbols=%5EGSPC', { timeout: 7000 })
        const q = r?.data?.quoteResponse?.result?.[0] || {}
        const p = Number(q.regularMarketPrice)
        if (Number.isFinite(p)) { price = p; src = '^GSPC' }
        console.log('[spx.yahoo] ^GSPC', price ?? null)
    } catch (e) { console.log('[spx.yahoo] gspc fail', e?.message || e) }
    if (!Number.isFinite(price)) {
        try {
            const r2 = await http.get('https://query1.finance.yahoo.com/v7/finance/quote?symbols=SPY', { timeout: 7000 })
            const q2 = r2?.data?.quoteResponse?.result?.[0] || {}
            const p2 = Number(q2.regularMarketPrice)
            if (!Number.isFinite(price) && Number.isFinite(p2)) { price = p2; src = 'SPY' }
            console.log('[spx.yahoo] SPY', price ?? null)
        } catch (e) { console.log('[spx.yahoo] spy fail', e?.message || e) }
    }
    if (!Number.isFinite(price)) {
        try {
            const r3 = await http.get('https://stooq.com/q/d/l/?s=%5Espx&i=d', { responseType: 'text', timeout: 7000 })
            const parsed = parseStooqCsvForChange(r3?.data)
            if (parsed && Number.isFinite(parsed.price)) { price = parsed.price; src = 'STOOQ_^SPX' }
            console.log('[spx.stooq]', price ?? null)
        } catch (e) { console.log('[spx.stooq] fail', e?.message || e) }
    }
    return { price: Number.isFinite(price) ? Number(price) : null, src }
}

function getDeep(obj, path) {
    const parts = path.split('.')
    let cur = obj
    for (const k of parts) { if (!cur || typeof cur !== 'object') return undefined; cur = cur[k] }
    return cur
}

function setDeep(obj, path, val) {
    const parts = path.split('.')
    let cur = obj
    for (let i = 0; i < parts.length - 1; i++) { const k = parts[i]; if (!cur[k] || typeof cur[k] !== 'object') cur[k] = {}; cur = cur[k] }
    cur[parts[parts.length - 1]] = val
}

function markStale(meta, path, fromAt) {
    if (!meta.stale) meta.stale = {}
    if (Number.isFinite(Number(fromAt))) meta.stale[path] = { fromAt: Number(fromAt) }
}

async function latestWith(db, hasValue) {
    const cur = db.collection(COLLECTION).find({}, { projection: { at: 1, snapshots: 1, btcDominancePct: 1, btcDominanceDelta: 1, spx: 1, totals: 1, oiCvd: 1 } }).sort({ at: -1 }).limit(500)
    while (await cur.hasNext()) { const d = await cur.next(); if (hasValue(d)) return d }
    return null
}

async function findClosestWith(db, target, hasValue, windowMs = 72 * 3600 * 1000) {
    const minTs = target - windowMs
    const maxTs = target + windowMs
    const proj = { at: 1, snapshots: 1, btcDominancePct: 1, spx: 1, totals: 1, oiCvd: 1 }
    const cur = db.collection(COLLECTION).find({ at: { $gte: minTs, $lte: maxTs } }, { projection: proj }).sort({ at: 1 }).limit(1000)
    let best = null, bestDist = Infinity
    while (await cur.hasNext()) {
        const d = await cur.next()
        if (hasValue(d)) {
            const dist = Math.abs(Number(d.at) - target)
            if (dist < bestDist) { best = d; bestDist = dist }
        }
    }
    if (best) return best
    const cur2 = db.collection(COLLECTION).find({}, { projection: proj }).sort({ at: -1 }).limit(500)
    best = null; bestDist = Infinity
    while (await cur2.hasNext()) {
        const d = await cur2.next()
        if (hasValue(d)) {
            const dist = Math.abs(Number(d.at) - target)
            if (dist < bestDist) { best = d; bestDist = dist }
        }
    }
    return best
}

async function backfillIfMissing(db, docOut, meta, path, validator = isNum) {
    const val = getDeep(docOut, path)
    const ok = validator ? validator(val) : isNum(val)
    if (ok) return
    const prev = await latestWith(db, d => {
        const v = getDeep(d, path)
        return validator ? validator(v) : isNum(v)
    })
    if (prev) {
        setDeep(docOut, path, getDeep(prev, path))
        markStale(meta, path, prev.at)
    }
}

const marketsCache = {}

function totalWindowLabel(period, limit) {
    const mp = { '1m':1,'3m':3,'5m':5,'15m':15,'30m':30,'1h':60,'2h':120,'4h':240,'6h':360,'12h':720,'1d':1440 }
    const total = (mp[period] || 0) * (limit || 0)
    if (!total) return `${limit}×${period}`
    if (total % 60 === 0) return `${total/60}ч`
    return `${total}м`
}

function verdictBy(oiPct, cvd) {
    if (!Number.isFinite(oiPct) || !Number.isFinite(cvd)) return { emoji: '⚪️', text: 'нет данных' }
    const oiUp = oiPct > 0.2, oiDown = oiPct < -0.2, cvdUp = cvd > 0, cvdDown = cvd < 0
    if (oiUp && cvdUp)   return { emoji: '🟢', text: 'приток лонгов' }
    if (oiDown && cvdUp) return { emoji: '🟡', text: 'short-cover' }
    if (oiUp && cvdDown) return { emoji: '🟠', text: 'впитывание' }
    if (oiDown && cvdDown) return { emoji: '⚪️', text: 'охлаждение' }
    return { emoji: '⚪️', text: 'нейтрально' }
}

async function getJsonWithFallback(pathAndQuery) {
    let lastErr = null
    for (const base of BINANCE_FAPI_BASES) {
        try {
            const url = `${base}${pathAndQuery}${pathAndQuery.includes('?') ? '&' : '?'}_=${Date.now()}`
            const res = await withRetry(() => http.get(url, { headers: { 'Accept':'application/json' } }), 2, 500)
            const data = res?.data
            if (Array.isArray(data)) return data
            lastErr = new Error('unexpected_response')
        } catch (e) { lastErr = e }
    }
    throw lastErr || new Error('all_endpoints_failed')
}

const FUT_MAP = { BTC:'BTCUSDT', ETH:'ETHUSDT', SOL:'SOLUSDT', BNB:'BNBUSDT', XRP:'XRPUSDT', DOGE:'DOGEUSDT', TON:'TONUSDT' }
const toBinance = (s) => FUT_MAP[String(s).toUpperCase()] || `${String(s).toUpperCase()}USDT`
const num = (v) => { const n = Number(v); return Number.isFinite(n) ? n : NaN }

async function fetchOpenInterest(symbol, period = OI_CVD_PERIOD, limit = OI_CVD_LIMIT) {
    const sym = toBinance(symbol)
    const path = `/futures/data/openInterestHist?symbol=${encodeURIComponent(sym)}&period=${encodeURIComponent(period)}&limit=${encodeURIComponent(String(limit))}`
    const rows = await getJsonWithFallback(path)
    const series = rows.map(r => ({ ts: num(r?.timestamp), usd: num(r?.sumOpenInterestValue) })).filter(r => Number.isFinite(r.usd))
    if (!series.length) return { oiChangePct: NaN, oiNowUsd: NaN }
    const first = series[0].usd
    const last  = series[series.length - 1].usd
    const oiChangePct = (Number.isFinite(first) && first > 0) ? ((last - first) / first * 100) : NaN
    return { oiChangePct, oiNowUsd: last }
}

async function fetchCvd(symbol, period = OI_CVD_PERIOD, limit = OI_CVD_LIMIT) {
    const sym = toBinance(symbol)
    const path = `/futures/data/takerlongshortRatio?symbol=${encodeURIComponent(sym)}&period=${encodeURIComponent(period)}&limit=${encodeURIComponent(String(limit))}`
    const rows = await getJsonWithFallback(path)
    const series = rows.map(r => ({ buy: num(r?.buyVol), sell: num(r?.sellVol) })).filter(r => Number.isFinite(r.buy) && Number.isFinite(r.sell))
    if (!series.length) return { cvd: NaN, deltaLast: NaN, sumBuy: NaN, sumSell: NaN }
    let acc = 0, sumBuy = 0, sumSell = 0
    for (const x of series) { acc += (x.buy - x.sell); sumBuy += x.buy; sumSell += x.sell }
    const last = series[series.length - 1]
    return { cvd: acc, deltaLast: last.buy - last.sell, sumBuy, sumSell }
}

async function buildOiCvdSnapshot(symbol, priceNow) {
    const period = OI_CVD_PERIOD
    const limit = OI_CVD_LIMIT
    try {
        const [oi, cvd] = await Promise.all([ fetchOpenInterest(symbol, period, limit), fetchCvd(symbol, period, limit) ])
        const oiPct = Number.isFinite(oi?.oiChangePct) ? Number(oi.oiChangePct.toFixed(2)) : NaN
        const cvdVal = Number.isFinite(cvd?.cvd) ? Number(cvd.cvd.toFixed(2)) : NaN
        const v = verdictBy(oiPct, cvdVal)
        const cvdUSD = (Number.isFinite(cvdVal) && Number.isFinite(priceNow)) ? Number((cvdVal * priceNow).toFixed(2)) : null
        return {
            symbol: String(symbol).toUpperCase(),
            period,
            limit,
            windowLabel: totalWindowLabel(period, limit),
            oiChangePct: oiPct,
            cvd: cvdVal,
            deltaLast: Number.isFinite(cvd?.deltaLast) ? Number(cvd.deltaLast.toFixed(2)) : NaN,
            cvdUSD,
            verdictEmoji: v.emoji,
            verdictText: v.text
        }
    } catch (e) {
        return {
            symbol: String(symbol).toUpperCase(),
            period, limit, windowLabel: totalWindowLabel(period, limit),
            oiChangePct: NaN, cvd: NaN, deltaLast: NaN, cvdUSD: null,
            verdictEmoji: '⚪️', verdictText: 'нет данных'
        }
    }
}

async function buildSnapshots() {
    const out = {}
    const cg = await cgMarkets(SYMBOLS.map(s => ids[s]).filter(Boolean)).catch(() => [])
    for (const rec of cg) { if (rec?.id) marketsCache[rec.id] = rec }
    for (const s of SYMBOLS) {
        const id = ids[s]
        const m = cg.find(x => x.id === id)
        let price = Number(m?.current_price) || null
        let pct24 = typeof m?.price_change_percentage_24h === 'number' ? Number(m.price_change_percentage_24h) : null
        let vol24 = typeof m?.total_volume === 'number' ? Number(m.total_volume) : null
        if ((!Number.isFinite(price) || !Number.isFinite(pct24) || !Number.isFinite(vol24)) && binance[s]) {
            const t24 = await binanceTicker24h(binance[s]).catch(() => ({}))
            if (!Number.isFinite(price) && Number.isFinite(t24?.price)) price = t24.price
            if (!Number.isFinite(pct24) && Number.isFinite(t24?.pct24)) pct24 = t24.pct24
            if (!Number.isFinite(vol24) && Number.isFinite(t24?.vol24)) vol24 = t24.vol24
        }
        let rsi14 = null, rsi14Prev = null, pctPrev = null, volPrev = null, volDeltaPct = null
        try {
            if (id) {
                const ch = await cgChart(id, 16)
                const closes = Array.isArray(ch?.prices) ? ch.prices.map(p => Number(p[1])) : []
                if (closes.length >= 16) {
                    rsi14 = rsi14FromCloses(closes.slice(-15))
                    rsi14Prev = rsi14FromCloses(closes.slice(-16, -1))
                }
                if (closes.length >= 3) {
                    const c2 = closes[closes.length - 2], c3 = closes[closes.length - 3]
                    if (Number.isFinite(c2) && Number.isFinite(c3) && c3 !== 0) pctPrev = ((c2 - c3) / c3) * 100
                }
                const vols = Array.isArray(ch?.total_volumes) ? ch.total_volumes.map(p => Number(p[1])) : []
                if (vols.length >= 2) {
                    const prev = vols[vols.length - 2], last = vols[vols.length - 1]
                    if (Number.isFinite(prev) && Number.isFinite(last) && prev > 0) {
                        volPrev = prev
                        volDeltaPct = (last - prev) / prev * 100
                    }
                }
            }
        } catch {}
        let fundingNow = null, fundingPrev = null, fundingDelta = null
        if (binance[s]) {
            try {
                let series = await binanceFundingSeries(binance[s], 24).catch(() => [])
                if (!series.length) series = await binanceFundingSeriesWWW(binance[s], 48).catch(() => [])
                if (series.length >= 6) {
                    const nowArr = series.slice(-3)
                    const prevArr = series.slice(-6, -3)
                    fundingNow = avg(nowArr)
                    fundingPrev = avg(prevArr)
                    if (Number.isFinite(fundingNow) && Number.isFinite(fundingPrev)) fundingDelta = fundingNow - fundingPrev
                } else if (series.length >= 3) {
                    fundingNow = avg(series.slice(-3))
                }
                if (!Number.isFinite(fundingNow)) {
                    const fn = await binancePremiumIndex(binance[s]).catch(() => null)
                    if (Number.isFinite(fn)) fundingNow = fn
                }
                if (!Number.isFinite(fundingNow)) {
                    const by = await bybitFunding(binance[s]).catch(() => [])
                    if (by.length >= 3) {
                        const nowArr = by.slice(-3)
                        const prevArr = by.length >= 6 ? by.slice(-6, -3) : null
                        fundingNow = avg(nowArr)
                        if (prevArr && prevArr.length) {
                            fundingPrev = avg(prevArr)
                            fundingDelta = fundingNow - (avg(prevArr) ?? 0)
                        }
                    }
                }
                if (!Number.isFinite(fundingNow)) {
                    const inst = binance[s] === 'BTCUSDT' ? 'BTC-USDT-SWAP' : binance[s] === 'ETHUSDT' ? 'ETH-USDT-SWAP' : null
                    if (inst) {
                        const okx = await okxFunding(inst).catch(() => null)
                        if (Number.isFinite(okx)) fundingNow = okx
                    }
                }
                if (nearZero(fundingNow)) fundingNow = null
                if (nearZero(fundingPrev)) fundingPrev = null
                if (nearZero(fundingDelta)) fundingDelta = null
            } catch {}
        }
        let longShort = null, longShortSrc = null
        if (binance[s]) {
            const r = await fetchLongShortAgg(s).catch(() => ({ data: null, src: null }))
            if (isLSValid(r?.data)) { longShort = r.data; longShortSrc = r.src }
        }
        out[s] = {
            symbol: s,
            price,
            pct24,
            pctPrev,
            vol24,
            volPrev,
            volDeltaPct,
            rsi14,
            rsi14Prev,
            fundingNow,
            fundingPrev,
            fundingDelta,
            netFlowsUSDNow: null,
            netFlowsUSDPrev: null,
            netFlowsUSDDiff: null,
            longShort,
            longShortSrc,
            score: null,
            fgiValue: null,
            fgiClass: null,
            fgiTs: null
        }
    }
    const fgi = await fearGreed().catch(() => ({ value: null, classification: null, ts: null }))
    for (const s of SYMBOLS) {
        out[s].fgiValue = fgi.value
        out[s].fgiClass = fgi.classification
        out[s].fgiTs = fgi.ts
    }
    const oiCvd = {}
    for (const s of ['BTC','ETH']) {
        if (out[s]) {
            oiCvd[s] = await buildOiCvdSnapshot(s, out[s].price).catch(() => null)
        }
    }
    return { snapshots: out, oiCvd }
}

async function fetchTotalsNow() {
    try {
        const since = Date.now() - lastCgAt
        const extraJitter = 400 + Math.floor(Math.random() * 300)
        if (since < CG_MIN_INTERVAL_MS + extraJitter) {
            await sleep(CG_MIN_INTERVAL_MS + extraJitter - since)
        }
        const gRaw = await cgRequest('/api/v3/global', { retries: 2 })
        const g = gRaw?.data || {}
        const totalCap = Number(g?.total_market_cap?.usd)
        const totalPct24 = Number(g?.market_cap_change_percentage_24h_usd)
        let btcCap = null, btcPct24 = null, ethCap = null, ethPct24 = null
        const recBTC = marketsCache['bitcoin']
        const recETH = marketsCache['ethereum']
        if (recBTC && recETH) {
            btcCap = Number(recBTC?.market_cap)
            btcPct24 = Number(recBTC?.market_cap_change_percentage_24h)
            ethCap = Number(recETH?.market_cap)
            ethPct24 = Number(recETH?.market_cap_change_percentage_24h)
        } else {
            const mRaw = await cgRequest('/api/v3/coins/markets', { params: { vs_currency: 'usd', ids: 'bitcoin,ethereum', per_page: 2, page: 1, sparkline: false, price_change_percentage: '24h' }, retries: 2 })
            const arr = Array.isArray(mRaw) ? mRaw : []
            const rb = arr.find(x => String(x?.id) === 'bitcoin') || {}
            const re = arr.find(x => String(x?.id) === 'ethereum') || {}
            btcCap = Number(rb?.market_cap)
            btcPct24 = Number(rb?.market_cap_change_percentage_24h)
            ethCap = Number(re?.market_cap)
            ethPct24 = Number(re?.market_cap_change_percentage_24h)
        }
        console.log('[totals] source.inputs', {
            src: 'coingecko',
            endpoints: [
                '/api/v3/global',
                recBTC && recETH ? '(markets from cache)' : '/api/v3/coins/markets?ids=bitcoin,ethereum'
            ],
            totalCap, totalPct24, btcCap, btcPct24, ethCap, ethPct24
        })
        if (!Number.isFinite(totalCap) || !Number.isFinite(totalPct24) || !Number.isFinite(btcCap) || !Number.isFinite(btcPct24) || !Number.isFinite(ethCap) || !Number.isFinite(ethPct24)) {
            throw new Error('totals_incomplete')
        }
        const prev = (now,p) => now/(1+p/100)
        const totalPrev = prev(totalCap, totalPct24)
        const btcPrev   = prev(btcCap,   btcPct24)
        const ethPrev   = prev(ethCap,   ethPct24)
        const total2Now  = totalCap - btcCap
        const total3Now  = totalCap - btcCap - ethCap
        const total2Prev = totalPrev - btcPrev
        const total3Prev = totalPrev - btcPrev - ethPrev
        const pct = (now,pr) => Number.isFinite(now)&&Number.isFinite(pr)&&pr!== 0 ? ((now-pr)/pr)*100 : null
        const d1 = pct(totalCap,  totalPrev)
        const d2 = pct(total2Now, total2Prev)
        const d3 = pct(total3Now, total3Prev)
        console.log('[totals] computed', { method: 'prev = now/(1+pct/100)', total: totalCap, total2: total2Now, total3: total3Now, d1, d2, d3 })
        return { total: totalCap, total2: total2Now, total3: total3Now, d1, d2, d3 }
    } catch (e) {
        console.warn('[totals] cg path failed:', e?.message || e)
        return null
    }
}

async function binanceAll24hAll() {
    try {
        const data = await getFromRotatingBases(BINANCE_FAPI_BASES, '/fapi/v1/ticker/24hr', {})
        if (!Array.isArray(data)) return []
        console.log('[binanceAll24h] fetched', data.length)
        return data
    } catch {
        console.log('[binanceAll24h] fail')
        return []
    }
}

async function binanceExchangeInfo() {
    try {
        const data = await getFromRotatingBases(BINANCE_FAPI_BASES, '/fapi/v1/exchangeInfo', {})
        return data
    } catch { return null }
}

function uniqArr(a) { return Array.from(new Set(a)) }

async function buildCapTopLeaderboard() {
    const period = OI_CVD_PERIOD
    const slices = OI_CVD_LIMIT
    const windowLabel = totalWindowLabel(period, slices)
    const exInfo = await binanceExchangeInfo().catch(() => null)
    const symbolsInfo = Array.isArray(exInfo?.symbols) ? exInfo.symbols : []
    const usdtPerp = symbolsInfo.filter(s => s?.status === 'TRADING' && s?.quoteAsset === 'USDT' && s?.contractType === 'PERPETUAL').map(s => String(s.symbol))
    console.log('[capTop] exchangeInfo usdt_perp=', usdtPerp.length)
    const all24 = await binanceAll24hAll()
    const tmap = new Map(all24.map(x => [String(x.symbol), x]))
    const universe = usdtPerp
    const withVol = universe.filter(s => {
        const rec = tmap.get(s)
        const qv = Number(rec?.quoteVolume)
        return Number.isFinite(qv) && qv > 0
    })
    const scanLimit = Math.max(1, Math.min(CAP_TOP_SCAN_MAX, withVol.length))
    console.log('[capTop] universe=', universe.length, 'withVol=', withVol.length, 'scan=', scanLimit, 'limit=', scanLimit, 'period=', period, 'limitSlices=', slices)
    const sortedByVol = withVol.slice().sort((a,b) => {
        const qa = Number(tmap.get(a)?.quoteVolume) || 0
        const qb = Number(tmap.get(b)?.quoteVolume) || 0
        return qb - qa
    })
    const scanList = sortedByVol.slice(0, scanLimit)
    const weightPct = 1
    const weightUsd = 1e-6
    const results = []
    let scannedOk = 0, skipped = 0
    const pool = []
    for (const sym of scanList) {
        const task = (async () => {
            try {
                const asset = sym.endsWith('USDT') ? sym.slice(0, -4) : sym
                const [oi, cvd] = await Promise.all([
                    fetchOpenInterest(asset, period, slices).catch(() => ({ oiChangePct: NaN })),
                    fetchCvd(asset, period, slices).catch(() => ({ cvd: NaN }))
                ])
                const tick = tmap.get(sym)
                const price = Number(tick?.lastPrice)
                const oiPct = Number.isFinite(oi?.oiChangePct) ? Number(oi.oiChangePct.toFixed(2)) : null
                const cvdVal = Number.isFinite(cvd?.cvd) ? Number(cvd.cvd) : null
                const cvdUsd = (Number.isFinite(cvdVal) && Number.isFinite(price)) ? Number((cvdVal * price).toFixed(2)) : null
                const sortKey = (Math.abs(oiPct || 0) * weightPct) + (Math.abs(cvdUsd || 0) * weightUsd)
                const v = verdictBy(oiPct, cvdVal)
                results.push({ sym: asset, oiPct, cvdUsd, price: Number.isFinite(price) ? Number(price) : null, sortKey, verdictEmoji: v.emoji, verdictText: v.text })
                scannedOk++
            } catch { skipped++ }
        })()
        pool.push(task)
        if (pool.length >= CAP_TOP_CONCURRENCY) {
            await Promise.race(pool.map(p => p.then(() => 'ok').catch(() => 'err'))).catch(()=>{})
            for (let i = pool.length - 1; i >= 0; i--) {
                if (pool[i].isFulfilled || pool[i].isRejected) pool.splice(i,1)
            }
        }
    }
    await Promise.allSettled(pool)
    console.log('[capTop] scanned_ok=', scannedOk, 'skipped=', skipped, 'results_total=', results.length)
    results.sort((a,b) => b.sortKey - a.sortKey)
    const absTop10 = results.slice(0, 10).map(({ sortKey, ...keep }) => keep)
    const humanUsd = (x) => {
        if (!Number.isFinite(x)) return '—'
        const a = Math.abs(x)
        const sign = x >= 0 ? '' : '-'
        if (a >= 1_000_000) return `${sign}$${(a/1_000_000).toFixed(2)} M`
        if (a >= 1_000)     return `${sign}$${(a/1_000).toFixed(2)} K`
        return `${sign}$${a.toFixed(2)}`
    }
    console.log('[capTop] leaders_top10:')
    absTop10.forEach((r, i) => {
        const sOi  = Number.isFinite(r.oiPct)  ? `${r.oiPct.toFixed(2)}%` : '—'
        const sCvd = Number.isFinite(r.cvdUsd) ? humanUsd(r.cvdUsd)       : '—'
        const p    = Number.isFinite(r.price)  ? r.price                  : null
        console.log(`${i+1}. ${r.sym}: ${r.verdictEmoji || '⚪️'} oi=${sOi} cvd=${sCvd}${p!=null?` price=${p}`:''} verdict="${r.verdictText || ''}"`)
    })
    const dist = absTop10.reduce((a,x)=>{ const k = x.verdictEmoji || 'null'; a[k]=(a[k]||0)+1; return a; },{})
    console.log('[capTop] emojis_dist:', dist, 'window=', windowLabel)
    return { windowLabel, absTop10 }
}

async function buildAndPersist() {
    if (!MONGO_URI) { console.error('MONGO_URI required'); process.exit(1) }
    const client = new MongoClient(MONGO_URI)
    const started = Date.now()
    console.log(
      '[collector] START',
      new Date(started).toISOString(),
      JSON.stringify({
          pid: process.pid,
          node: process.version,
          cwd: process.cwd(),
          script: path.resolve(process.argv[1] || 'src/index.js'),
          DB_NAME, COLLECTION, SYMBOLS, OI_CVD_PERIOD, OI_CVD_LIMIT,
          CAP_TOP_SCAN_MAX, CAP_TOP_CONCURRENCY
      })
    )
    try {
        await client.connect()
        const db = client.db(DB_NAME)
        try { await db.collection(COLLECTION).createIndex({ expireAt: 1 }, { expireAfterSeconds: 0 }) } catch {}
        const { snapshots, oiCvd } = await buildSnapshots().catch(() => ({ snapshots: {}, oiCvd: {} }))
        if (oiCvd && (oiCvd.BTC || oiCvd.ETH)) {
            console.log('[OI/CVD DEBUG] at:', new Date().toISOString())
            if (oiCvd.BTC) console.log('[OI/CVD DEBUG] oiCvd.BTC:', JSON.stringify(oiCvd.BTC))
            if (oiCvd.ETH) console.log('[OI/CVD DEBUG] oiCvd.ETH:', JSON.stringify(oiCvd.ETH))
        }
        let capTop = null
        try { capTop = await buildCapTopLeaderboard() } catch (e) { console.warn('[capTop] failed:', e?.message || e) }
        if (capTop && Array.isArray(capTop.absTop10)) {
            const dist = capTop.absTop10.reduce((a,x)=>{ const k = x.verdictEmoji || 'null'; a[k]=(a[k]||0)+1; return a; },{})
            console.log('[OI/CVD DEBUG] leadersTop.window:', capTop.windowLabel)
            console.log('[OI/CVD DEBUG] leadersTop.items.length:', capTop.absTop10.length)
            console.log('[OI/CVD DEBUG] leadersTop.emojis:', dist)
        }
        console.log(
          '[collector] SNAPSHOT_READY',
          JSON.stringify({
              atIso: new Date().toISOString(),
              symbols: Object.keys(snapshots || {}).length,
              hasOiCvdBTC: Boolean(oiCvd?.BTC),
              hasOiCvdETH: Boolean(oiCvd?.ETH),
              leadersWindow: capTop?.windowLabel || null,
              leadersCount: Array.isArray(capTop?.absTop10) ? capTop.absTop10.length : 0
          })
        )
        const now = Date.now()
        const expireAt = new Date(now + 24 * 3600 * 1000)
        const meta = { stale: {} }
        let btcDominancePct = await fetchBtcDominanceNow().catch(() => null)
        let spxNow = await fetchSpxNow().catch(() => ({ price: null, src: null }))
        const domNowDoc = Number.isFinite(btcDominancePct) ? { at: now, btcDominancePct } : await latestWith(db, d => isNum(d?.btcDominancePct))
        if (!Number.isFinite(btcDominancePct) && domNowDoc) { btcDominancePct = Number(domNowDoc.btcDominancePct); markStale(meta, 'btcDominancePct', domNowDoc.at) }
        let btcDominanceDelta = null
        if (Number.isFinite(btcDominancePct)) {
            const refTarget = now - 24 * 3600 * 1000
            const refDoc = await findClosestWith(db, refTarget, d => isNum(d?.btcDominancePct))
            const refVal = refDoc ? Number(refDoc.btcDominancePct) : null
            if (Number.isFinite(refVal) && refVal !== 0) {
                btcDominanceDelta = ((btcDominancePct - refVal) / refVal) * 100
                console.log('[dom.delta] now=', btcDominancePct.toFixed(2), 'ref=', refVal.toFixed(2), 'refAt=', refDoc?.at || null, 'delta%=', (btcDominanceDelta).toFixed(4))
            } else {
                console.log('[dom.delta] ref missing or zero -> delta=null')
            }
        }
        let spx = { price: Number.isFinite(spxNow?.price) ? Number(spxNow.price) : null, pct: null, src: spxNow?.src || null }
        if (!Number.isFinite(spx.price)) {
            const prev = await latestWith(db, d => isNum(d?.spx?.price))
            if (prev) { spx.price = Number(prev.spx.price); spx.src = prev.spx.src || spx.src || null; markStale(meta, 'spx.price', prev.at) }
        }
        if (Number.isFinite(spx.price)) {
            const refTarget = now - 24 * 3600 * 1000
            const refDoc = await findClosestWith(db, refTarget, d => isNum(d?.spx?.price))
            const refPrice = refDoc ? Number(refDoc.spx.price) : null
            if (Number.isFinite(refPrice) && refPrice !== 0) spx.pct = ((spx.price - refPrice) / refPrice) * 100
        }
        const totals = await fetchTotalsNow().catch(() => null)
        if (totals) {
            console.log('[totals] ok', {
                src: 'coingecko',
                endpoints: [
                    'https://api.coingecko.com/api/v3/global',
                    marketsCache['bitcoin'] && marketsCache['ethereum'] ? '(markets from cache)' : 'https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&ids=bitcoin,ethereum'
                ],
                computedAt: new Date().toISOString(),
                snapshotAt: new Date(now).toISOString()
            })
        } else {
            console.log('[totals.save] totals=null')
        }
        const doc = {
            at: now,
            atIsoKyiv: kyivIso(now),
            snapshots,
            oiCvd,
            capTop: capTop ? { windowLabel: capTop.windowLabel, absTop10: capTop.absTop10 } : { windowLabel: totalWindowLabel(OI_CVD_PERIOD, OI_CVD_LIMIT), absTop10: [] },
            oiCvdPeriod: OI_CVD_PERIOD,
            oiCvdLimit: OI_CVD_LIMIT,
            btcDominancePct: Number.isFinite(btcDominancePct) ? Number(btcDominancePct.toFixed(2)) : null,
            btcDominanceDelta: Number.isFinite(btcDominanceDelta) ? Number(btcDominanceDelta.toFixed(2)) : null,
            spx: { price: Number.isFinite(spx.price) ? Number(spx.price) : null, pct: Number.isFinite(spx.pct) ? Number(spx.pct.toFixed(2)) : null, src: spx.src || null },
            totals: totals ? {
                total: Number(totals.total),
                total2: Number(totals.total2),
                total3: Number(totals.total3),
                d1: totals.d1 != null ? Number(totals.d1.toFixed(4)) : null,
                d2: totals.d2 != null ? Number(totals.d2.toFixed(4)) : null,
                d3: totals.d3 != null ? Number(totals.d3.toFixed(4)) : null
            } : null,
            expireAt,
            meta
        }
        const pathsToValidate = [
            'btcDominancePct',
            'btcDominanceDelta',
            'spx.price',
            'spx.pct',
            ...['BTC','ETH','PAXG'].flatMap(sym => [
                `snapshots.${sym}.price`,
                `snapshots.${sym}.pct24`,
                `snapshots.${sym}.pctPrev`,
                `snapshots.${sym}.vol24`,
                `snapshots.${sym}.volPrev`,
                `snapshots.${sym}.volDeltaPct`,
                `snapshots.${sym}.rsi14`,
                `snapshots.${sym}.rsi14Prev`,
                `snapshots.${sym}.fundingNow`,
                `snapshots.${sym}.fundingPrev`,
                `snapshots.${sym}.fundingDelta`,
                `snapshots.${sym}.netFlowsUSDNow`,
                `snapshots.${sym}.netFlowsUSDPrev`,
                `snapshots.${sym}.netFlowsUSDDiff`,
                `snapshots.${sym}.longShort.longPct`,
                `snapshots.${sym}.longShort.shortPct`,
                `snapshots.${sym}.longShort.ls`,
                `snapshots.${sym}.longShortSrc`,
                `snapshots.${sym}.fgiValue`,
                `snapshots.${sym}.fgiTs`
            ]),
            'totals.total',
            'totals.total2',
            'totals.total3',
            'totals.d1',
            'totals.d2',
            'totals.d3',
            'oiCvd.BTC.oiChangePct',
            'oiCvd.BTC.cvd',
            'oiCvd.ETH.oiChangePct',
            'oiCvd.ETH.cvd'
        ]
        for (const p of pathsToValidate) {
            await backfillIfMissing(db, doc, doc.meta, p, (v) => v !== null && v !== undefined && (typeof v === 'number' ? (Number.isFinite(v) || v === 0) : true))
        }
        await db.collection(COLLECTION).insertOne(doc)
        if (HEALTHCHECK_URL) { try { await http.get(HEALTHCHECK_URL) } catch {} }
        console.log('[collector] DONE', new Date().toISOString(), 'elapsed_ms=', Date.now() - started)
    } catch (e) {
        console.error('[collector] fail', String(e?.message || e))
        process.exit(2)
    } finally {
        try { await client.close() } catch {}
    }
    process.exit(0)
}

buildAndPersist()
