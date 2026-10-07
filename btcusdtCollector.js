import 'dotenv/config'
import pg from 'pg'
import pino from 'pino'
import { ProxyAgent, fetch as undiciFetch } from 'undici'
const { Pool } = pg

// Standalone Pino Logger & Burst Collapsing (Self-contained for standalone EC2 deployment)
const isProduction = process.env.NODE_ENV === 'production'

let prettyTransport = undefined
if (!isProduction) {
    try {
        if (typeof import.meta.resolve === 'function') {
            import.meta.resolve('pino-pretty')
        }
        prettyTransport = {
            target: 'pino-pretty',
            options: {
                colorize: true,
                translateTime: 'SYS:HH:MM:ss.l',
                ignore: 'pid,hostname',
            },
        }
    } catch {
        prettyTransport = undefined
    }
}

const pinoOptions = {
    level: process.env.LOG_LEVEL || 'info',
    hooks: {
        logMethod(inputArgs, method) {
            if (typeof inputArgs[0] === 'string' && typeof inputArgs[1] === 'object' && inputArgs[1] !== null) {
                const [msg, obj, ...rest] = inputArgs
                return method.call(this, obj, msg, ...rest)
            }
            return method.apply(this, inputArgs)
        },
    },
    ...(prettyTransport ? { transport: prettyTransport } : {}),
}

export function createBurstCollapsingLogger(targetLogger, options = {}) {
    const windowMs = options.windowMs ?? 200
    const maxWaitMs = options.maxWaitMs ?? 1000
    const activeBursts = new Map()

    function extractKey(level, msg, details) {
        const code =
            details?.code ||
            (details?.error && typeof details.error === 'object' && 'code' in details.error
                ? details.error.code
                : null) ||
            details?.status ||
            msg
        const identity = details?.identity || details?.source || details?.contractType || 'default'
        return `${level}:${String(code)}:${String(identity)}`
    }

    function handleBurstLog(level, arg1, arg2) {
        let msg = ''
        let details = {}

        if (typeof arg1 === 'string') {
            msg = arg1
            details = typeof arg2 === 'object' && arg2 !== null ? { ...arg2 } : {}
        } else if (typeof arg1 === 'object' && arg1 !== null) {
            details = { ...arg1 }
            msg = typeof arg2 === 'string' ? arg2 : ''
        } else {
            msg = String(arg1)
        }

        const key = extractKey(level, msg, details)
        const existing = activeBursts.get(key)

        if (!existing) {
            targetLogger[level](details, msg)

            const burst = {
                level,
                msg,
                details,
                count: 1,
                firstAt: Date.now(),
                lastAt: Date.now(),
                slidingTimeout: null,
                maxTimeout: null,
            }

            burst.slidingTimeout = setTimeout(() => {
                if (burst.count > 1) {
                    targetLogger[level]({ ...burst.details, repeated: burst.count }, burst.msg)
                }
                if (burst.maxTimeout) clearTimeout(burst.maxTimeout)
                activeBursts.delete(key)
            }, windowMs)

            burst.maxTimeout = setTimeout(() => {
                if (burst.count > 1) {
                    targetLogger[level]({ ...burst.details, repeated: burst.count }, burst.msg)
                    burst.count = 0
                }
            }, maxWaitMs)

            burst.slidingTimeout?.unref?.()
            burst.maxTimeout?.unref?.()

            activeBursts.set(key, burst)
        } else {
            existing.count += 1
            existing.lastAt = Date.now()
            existing.details = { ...existing.details, ...details }

            if (existing.slidingTimeout) {
                clearTimeout(existing.slidingTimeout)
            }

            existing.slidingTimeout = setTimeout(() => {
                if (existing.count > 1) {
                    targetLogger[level]({ ...existing.details, repeated: existing.count }, existing.msg)
                }
                if (existing.maxTimeout) clearTimeout(existing.maxTimeout)
                activeBursts.delete(key)
            }, windowMs)

            existing.slidingTimeout?.unref?.()
        }
    }

    function flush() {
        for (const burst of activeBursts.values()) {
            if (burst.slidingTimeout) clearTimeout(burst.slidingTimeout)
            if (burst.maxTimeout) clearTimeout(burst.maxTimeout)
            if (burst.count > 1) {
                targetLogger[burst.level]({ ...burst.details, repeated: burst.count }, burst.msg)
            }
        }
        activeBursts.clear()
    }

    return {
        info(arg1, arg2) {
            if (typeof arg1 === 'string' && typeof arg2 === 'object' && arg2 !== null) {
                targetLogger.info(arg2, arg1)
            } else {
                targetLogger.info(arg1, typeof arg2 === 'string' ? arg2 : undefined)
            }
        },
        warn(arg1, arg2) {
            handleBurstLog('warn', arg1, arg2)
        },
        error(arg1, arg2) {
            handleBurstLog('error', arg1, arg2)
        },
        debug(arg1, arg2) {
            if (typeof arg1 === 'string' && typeof arg2 === 'object' && arg2 !== null) {
                targetLogger.debug(arg2, arg1)
            } else {
                targetLogger.debug(arg1, typeof arg2 === 'string' ? arg2 : undefined)
            }
        },
        child(bindings) {
            const childPino = targetLogger.child(bindings)
            return createBurstCollapsingLogger(childPino, options)
        },
        flush,
        raw: targetLogger,
    }
}

export const baseLogger = pino(pinoOptions)
export const logger = createBurstCollapsingLogger(baseLogger)

export function createSourceLogger(source) {
    return logger.child({ source })
}

const SYMBOL = 'BTCUSDT'
const BASE_TIMEFRAME = '1m'
const BASE_TIMEFRAME_SECONDS = 60
const FOOTPRINT_BUCKET_SIZE = 5
const MIN_PROFILE_BASE_BUCKET_SIZE = 1.5
const DEFAULT_TICK_SIZE = 0.5
const DEFAULT_RETENTION_DAYS = 90
const DEFAULT_FLUSH_INTERVAL_MS = 1000
const DEFAULT_STATUS_INTERVAL_MS = 30000
const DEFAULT_MAX_DEDUPE_KEYS = 100000
const DEFAULT_RECONNECT_MIN_MS = 1000
const DEFAULT_RECONNECT_MAX_MS = 30000
const DEFAULT_HEARTBEAT_MS = 30000
const DEFAULT_EXPECTED_IDLE_THRESHOLD_MS = 90000
const DEFAULT_MAX_QUEUED_BUBBLE_EVENTS = 50000
const DEFAULT_MAX_BUFFERED_SLICES = 120
const DEFAULT_AGG_BUBBLE_MIN_VOLUME_BTC = 15
const DEFAULT_AGG_BUBBLE_MIN_TRADE_COUNT = 75
const DEFAULT_AGG_BUBBLE_MIN_TRADE_COUNT_VOLUME_BTC = 3
const DEFAULT_AGG_BUBBLE_FLUSH_SIZE = 1000
// Warn if any in-memory slice has been pending for longer than this before the hard buffer cap is hit
const PERSISTENCE_WARN_THRESHOLD_MS = 5 * 60 * 1000 // 5 minutes

// Maximum gap that will be backfilled automatically. Gaps larger than this
// (e.g. on first startup or after a multi-day outage) are intentionally skipped:
// the watermark is advanced to the current time and live collection resumes.
// Configurable via COLLECTOR_MAX_BACKFILL_GAP_MS (default: 4 hours).
const DEFAULT_MAX_BACKFILL_GAP_MS = 4 * 60 * 60 * 1000 // 4 hours
const MAX_BACKFILL_GAP_MS = process.env.COLLECTOR_MAX_BACKFILL_GAP_MS
    ? Number(process.env.COLLECTOR_MAX_BACKFILL_GAP_MS)
    : DEFAULT_MAX_BACKFILL_GAP_MS

// No longer using MongoDB collections

const SOURCES = ['spot', 'futures']
const TARGETS = [
    { contractType: 'spot', dataSourceMode: 'spot', activeSources: ['spot'] },
    { contractType: 'spot', dataSourceMode: 'futures', activeSources: ['futures'] },
    { contractType: 'spot', dataSourceMode: 'both', activeSources: ['spot', 'futures'] },
    { contractType: 'futures', dataSourceMode: 'spot', activeSources: ['spot'] },
    { contractType: 'futures', dataSourceMode: 'futures', activeSources: ['futures'] },
    { contractType: 'futures', dataSourceMode: 'both', activeSources: ['spot', 'futures'] },
]

const config = loadConfig()
const sourceLoggers = {
    spot: createSourceLogger('spot'),
    futures: createSourceLogger('futures'),
}
process.stdout.on('error', handlePipeError)
process.stderr.on('error', handlePipeError)
const metrics = {
    tradesReceived: { spot: 0, futures: 0 },
    tradesAccepted: 0,
    tradesSkippedDuplicate: 0,
    tradesSkippedMissingReference: 0,
    slicesPersisted: 0,
    footprintRowsInserted: 0,
    footprintRowsSkipped: 0,
    profileRowsInserted: 0,
    profileRowsSkipped: 0,
    writeFailures: 0,
    aggregateBubbles: {
        received: { spot: 0, futures: 0 },
        qualified: 0,
        skippedBelowThreshold: 0,
        skippedMissingAggregateTradeId: 0,
        skippedInvalidTradeRange: 0,
        duplicatesSkipped: 0,
        inserted: 0,
        insertFailed: 0,
        skippedPersistenceDisabled: 0,
    },
}

let pgPool = null
let shuttingDown = false
let persistPromise = null

const priceReferences = {
    spot: null,
    futures: null,
}

let runtimes = []
const streamClients = []
const queuedAggregateBubbleEvents = []
const queuedAggregateBubbleKeys = new Set()

const sourceState = {
    spot: { connected: false, isBackfilling: false, lastTradeTimeMs: null, lastMessageAtMs: null, connectedAtMs: null },
    futures: { connected: false, isBackfilling: false, lastTradeTimeMs: null, lastMessageAtMs: null, connectedAtMs: null },
}

let exitFn = (code) => process.exit(code)
let startTimeMs = Date.now()

async function safeExit(code, reason) {
    if (shuttingDown) return
    logger.error('CRITICAL: collector exiting due to watchdog/fatal condition', { reason, code })
    const uptimeMs = Date.now() - startTimeMs
    if (uptimeMs < 60000) {
        logger.warn('rapid exit detected, applying 10s cooldown before restart', { uptimeMs })
        await new Promise((r) => setTimeout(r, 10000))
    }
    exitFn(code)
}

process.on('unhandledRejection', (reason) => {
    logger.error('CRITICAL: unhandled rejection', { error: getErrorMessage(reason) })
    void safeExit(1, 'unhandledRejection')
})
process.on('uncaughtException', (err) => {
    logger.error('CRITICAL: uncaught exception', { error: getErrorMessage(err) })
    void safeExit(1, 'uncaughtException')
})

if (process.env.NODE_ENV !== 'test') {
    main().catch((error) => {
        logger.error('collector fatal error', { error: getErrorMessage(error) })
        process.exitCode = 1
    })
}

async function main() {
    assertRuntimeSupport()
    await initTimescale()
    await seedWatermarksFromDatabase()
    runtimes = TARGETS.map((target) => createRuntime(target))

    logger.info('active aggregation identities', {
        symbol: SYMBOL,
        identities: runtimes.map((runtime) => runtime.identity),
        footprintBucketSize: FOOTPRINT_BUCKET_SIZE,
        profileBaseBucketSize: config.profileBaseBucketSize,
        aggregateBubbleThresholds: {
            minVolume: config.aggregateBubbleMinVolume,
            minTradeCount: config.aggregateBubbleMinTradeCount,
            minTradeCountVolume: config.aggregateBubbleMinTradeCountVolume,
        },
        writesEnabled: config.enableWrites,
        dryRun: config.dryRun,
        aggregateBubbleWritesEnabled: config.aggregateBubbleWritesEnabled,
    })

    if (!config.enableWrites || config.dryRun) {
        logger.warn('collector is not writing to mongodb', {
            writesEnabled: config.enableWrites,
            dryRun: config.dryRun,
            requiredEnv: 'Set COLLECTOR_ENABLE_WRITES=true and COLLECTOR_DRY_RUN=false to persist rows.',
        })
    }

    streamClients.push(createBinanceStreamClient('spot'))
    streamClients.push(createBinanceStreamClient('futures'))
    streamClients.forEach((client) => client.connect())

    const flushTimer = setInterval(() => {
        void requestPersist('interval')
    }, config.flushIntervalMs)

    const statusTimer = setInterval(() => {
        logStatus().catch((error) => {
            logger.warn('status update failed', { error: getErrorMessage(error) })
        })
    }, config.statusIntervalMs)

    process.once('SIGINT', () => {
        void shutdown('SIGINT', flushTimer, statusTimer)
    })
    process.once('SIGTERM', () => {
        void shutdown('SIGTERM', flushTimer, statusTimer)
    })

    if (config.exitAfterMs > 0) {
        setTimeout(() => {
            void shutdown('COLLECTOR_EXIT_AFTER_MS', flushTimer, statusTimer)
        }, config.exitAfterMs)
    }
}

async function seedWatermarksFromDatabase() {
    if (!pgPool) return
    try {
        const metaRes = await pgPool.query(
            "SELECT key, value FROM collector_meta WHERE key IN ('last_spot_trade_time_ms', 'last_futures_trade_time_ms')"
        )
        const metaMap = new Map(metaRes.rows.map((r) => [r.key, Number(r.value)]))
        const minValidMs = Date.now() - (config.retentionSeconds * 1000)

        for (const source of SOURCES) {
            const metaVal = metaMap.get(`last_${source}_trade_time_ms`)
            if (Number.isFinite(metaVal) && metaVal > minValidMs && metaVal < Date.now()) {
                sourceState[source].lastTradeTimeMs = metaVal
                logger.info('seeded source watermark from collector_meta', { source, watermarkMs: metaVal, date: new Date(metaVal).toISOString() })
                continue
            }

            const rowRes = await pgPool.query(
                "SELECT MAX(candle_time_sec) as max_sec FROM footprint_cells WHERE symbol = $1 AND contract_type = $2",
                [SYMBOL, source]
            )
            const maxSec = Number(rowRes.rows[0]?.max_sec)
            const maxMs = maxSec * 1000
            if (Number.isFinite(maxMs) && maxMs > minValidMs && maxMs < Date.now()) {
                sourceState[source].lastTradeTimeMs = maxMs
                logger.info('seeded source watermark from footprint_cells', { source, watermarkMs: maxMs, date: new Date(maxMs).toISOString() })
            } else {
                sourceState[source].lastTradeTimeMs = null
                logger.info('no prior valid watermark found for source, starting fresh', { source })
            }
        }
    } catch (err) {
        logger.warn('could not seed watermarks from database, starting fresh', { error: getErrorMessage(err) })
    }
}

function loadConfig() {
    const tickSize = DEFAULT_TICK_SIZE

    return {
        timescaleUrl: process.env.TIMESCALEDB_URL || process.env.PG_URL || '',
        retentionSeconds: Math.floor(DEFAULT_RETENTION_DAYS * 24 * 60 * 60),
        flushIntervalMs: DEFAULT_FLUSH_INTERVAL_MS,
        statusIntervalMs: DEFAULT_STATUS_INTERVAL_MS,
        maxDedupeKeys: DEFAULT_MAX_DEDUPE_KEYS,
        aggregateBubbleFlushSize: DEFAULT_AGG_BUBBLE_FLUSH_SIZE,
        aggregateBubbleMinVolume: process.env.COLLECTOR_AGG_BUBBLE_MIN_VOLUME_BTC ? Number(process.env.COLLECTOR_AGG_BUBBLE_MIN_VOLUME_BTC) : DEFAULT_AGG_BUBBLE_MIN_VOLUME_BTC,
        aggregateBubbleMinTradeCount: process.env.COLLECTOR_AGG_BUBBLE_MIN_TRADE_COUNT ? Number(process.env.COLLECTOR_AGG_BUBBLE_MIN_TRADE_COUNT) : DEFAULT_AGG_BUBBLE_MIN_TRADE_COUNT,
        aggregateBubbleMinTradeCountVolume: process.env.COLLECTOR_AGG_BUBBLE_MIN_TRADE_COUNT_VOLUME_BTC ? Number(process.env.COLLECTOR_AGG_BUBBLE_MIN_TRADE_COUNT_VOLUME_BTC) : DEFAULT_AGG_BUBBLE_MIN_TRADE_COUNT_VOLUME_BTC,
        reconnectMinMs: DEFAULT_RECONNECT_MIN_MS,
        reconnectMaxMs: DEFAULT_RECONNECT_MAX_MS,
        heartbeatMs: DEFAULT_HEARTBEAT_MS,
        tradeStallThresholdMs: process.env.COLLECTOR_TRADE_STALL_THRESHOLD_MS ? Number(process.env.COLLECTOR_TRADE_STALL_THRESHOLD_MS) : 120000,
        expectedIdleThresholdMs: process.env.COLLECTOR_EXPECTED_IDLE_THRESHOLD_MS ? Number(process.env.COLLECTOR_EXPECTED_IDLE_THRESHOLD_MS) : DEFAULT_EXPECTED_IDLE_THRESHOLD_MS,
        maxQueuedBubbleEvents: process.env.COLLECTOR_MAX_QUEUED_BUBBLES ? Number(process.env.COLLECTOR_MAX_QUEUED_BUBBLES) : DEFAULT_MAX_QUEUED_BUBBLE_EVENTS,
        maxBufferedSlices: process.env.COLLECTOR_MAX_BUFFERED_SLICES ? Number(process.env.COLLECTOR_MAX_BUFFERED_SLICES) : DEFAULT_MAX_BUFFERED_SLICES,
        exitAfterMs: 0,
        tickSize,
        profileBaseBucketSize: Math.max(MIN_PROFILE_BASE_BUCKET_SIZE, tickSize),
        enableWrites: true,
        dryRun: false,
        aggregateBubbleWritesEnabled: true,
        logLevel: 'info',
        logFormat: 'pretty',
    }
}

function assertRuntimeSupport() {
    if (!config.timescaleUrl) {
        throw new Error('TIMESCALEDB_URL or PG_URL is required for the BTCUSDT collector')
    }

    if (typeof WebSocket === 'undefined') {
        throw new Error('Global WebSocket is unavailable. Run this collector with Node.js 22+ or a Node runtime that provides WebSocket.')
    }
}

async function initTimescale() {
    pgPool = new Pool({
        connectionString: config.timescaleUrl,
        max: 10,
        idleTimeoutMillis: 30000,
    })

    // Verify connection
    const client = await pgPool.connect()
    try {
        await client.query('SELECT 1')
    } finally {
        client.release()
    }

    logger.info('timescaledb connected')
}

function createRuntime(target) {
    const identity = `${SYMBOL}:${target.contractType}:${target.dataSourceMode}:${BASE_TIMEFRAME}`

    return {
        symbol: SYMBOL,
        contractType: target.contractType,
        dataSourceMode: target.dataSourceMode,
        activeSources: target.activeSources,
        identity,
        profileBaseBucketSize: config.profileBaseBucketSize,
        firstFullyCoveredBaseTimeBySource: { spot: null, futures: null },
        latestBaseTimeBySource: { spot: null, futures: null },
        taintedRangesBySource: { spot: [], futures: [] },
        firstWriteFailureAtMs: null,
        processedTradeKeys: new BoundedSet(config.maxDedupeKeys),
        persistedSlices: new BoundedSet(config.maxDedupeKeys),
        footprintSlices: new Map(),
        profileSlices: new Map(),
    }
}

function createBinanceStreamClient(source) {
    const log = sourceLoggers[source] || logger
    const lowerSymbol = SYMBOL.toLowerCase()
    const baseUrl = source === 'spot'
        ? 'wss://data-stream.binance.vision/stream'
        : 'wss://fstream.binance.com/market/stream'
    const streams = [`${lowerSymbol}@aggTrade`, `${lowerSymbol}@kline_1m`]
    const url = `${baseUrl}?streams=${streams.join('/')}`
    let ws = null
    let reconnectAttempts = 0
    let reconnectTimer = null
    let heartbeatTimer = null

    const clearTimers = () => {
        if (reconnectTimer) {
            clearTimeout(reconnectTimer)
            reconnectTimer = null
        }
        if (heartbeatTimer) {
            clearInterval(heartbeatTimer)
            heartbeatTimer = null
        }
    }

    const connect = () => {
        if (shuttingDown) return
        clearTimers()
        ws = new WebSocket(url)

        ws.onopen = () => {
            reconnectAttempts = 0
            log.info('stream connected', { source, streams })
            sourceState[source].connected = true
            sourceState[source].connectedAtMs = Date.now()
            sourceState[source].lastMessageAtMs = Date.now()

            const gapStart = sourceState[source].lastTradeTimeMs
            if (gapStart) {
                const gapMs = Date.now() - gapStart
                if (gapMs > MAX_BACKFILL_GAP_MS) {
                    // Gap is too large to backfill (e.g. first startup with old watermark,
                    // or a multi-day outage). Advance the watermark to now so that:
                    //  - no historical pending slices are created
                    //  - no HTTP 400 retry loop is triggered
                    //  - persistence health is not degraded
                    //  - live collection begins from the current position
                    const advancedTo = Date.now()
                    sourceState[source].lastTradeTimeMs = advancedTo
                    sourceState[source].isBackfilling = false
                    log.warn('gap exceeds max backfill window, skipping historical backfill and advancing watermark to current time', {
                        source,
                        gapMs,
                        maxBackfillGapMs: MAX_BACKFILL_GAP_MS,
                        skippedFrom: new Date(gapStart).toISOString(),
                        advancedTo: new Date(advancedTo).toISOString(),
                    })
                } else {
                    sourceState[source].isBackfilling = true
                    runBackfillUntilComplete(source, gapStart)
                        .then((result) => {
                            if (!result.ok) {
                                markSourceGap(source, result.cursor, Date.now())
                            }
                        })
                        .catch((error) => {
                            log.error('backfill fatal error', { source, error: getErrorMessage(error) })
                            markSourceGap(source, gapStart, Date.now())
                        })
                        .finally(() => {
                            sourceState[source].isBackfilling = false
                        })
                }
            } else {
                sourceState[source].isBackfilling = false
            }

            heartbeatTimer = setInterval(() => {
                if (!ws || ws.readyState !== WebSocket.OPEN) return
                const lastMsg = sourceState[source].lastMessageAtMs ?? Date.now()
                const idleMs = Date.now() - lastMsg
                if (idleMs > config.expectedIdleThresholdMs) {
                    log.warn('stream appears stalled, forcing reconnect', { source, idleMs, thresholdMs: config.expectedIdleThresholdMs })
                    ws.close()
                    return
                }

                // Trade-stall watchdog (checked only when not backfilling)
                if (!sourceState[source].isBackfilling) {
                    const lastTrade = sourceState[source].lastTradeTimeMs ?? sourceState[source].connectedAtMs ?? Date.now()
                    const tradeIdleMs = Date.now() - lastTrade
                    if (tradeIdleMs > config.tradeStallThresholdMs) {
                        log.error('trade stream stalled, triggering watchdog restart', {
                            source,
                            tradeIdleMs,
                            thresholdMs: config.tradeStallThresholdMs,
                        })
                        void safeExit(1, `trade_stall_${source}`)
                    }
                }
            }, config.heartbeatMs)
        }

        ws.onmessage = (event) => {
            sourceState[source].lastMessageAtMs = Date.now()
            handleStreamMessage(source, String(event.data)).catch((error) => {
                log.error('stream message handling failed', { source, error: getErrorMessage(error) })
            })
        }

        ws.onerror = (event) => {
            log.error('stream error', { source, error: describeWebSocketEvent(event) })
        }

        ws.onclose = (event) => {
            clearTimers()
            sourceState[source].connected = false
            if (shuttingDown) return
            log.warn('stream closed', { source, code: event.code, reason: event.reason })
            scheduleReconnect()
        }
    }

    const scheduleReconnect = () => {
        reconnectAttempts += 1
        const delay = Math.min(
            config.reconnectMaxMs,
            config.reconnectMinMs * 2 ** Math.min(reconnectAttempts - 1, 10),
        )
        log.warn('stream reconnect scheduled', { source, attempt: reconnectAttempts, delayMs: delay })
        reconnectTimer = setTimeout(connect, delay)
    }

    const close = () => {
        clearTimers()
        if (!ws) return
        ws.onopen = null
        ws.onmessage = null
        ws.onerror = null
        ws.onclose = null
        ws.close()
        ws = null
    }

    return { connect, close }
}

async function handleStreamMessage(source, raw) {
    const parsed = JSON.parse(raw)
    const stream = parsed.stream
    const data = parsed.data
    if (!stream || !data) return

    if (stream.includes('@kline_1m')) {
        const close = Number(data.k?.c)
        if (Number.isFinite(close) && close > 0) {
            priceReferences[source] = close
            const log = sourceLoggers[source] || logger
            log.debug('contract price reference updated', { source, price: close })
        }
        return
    }

    if (!stream.includes('@aggTrade')) return

    const trade = {
        source,
        id: Number.isFinite(Number(data.a)) ? Number(data.a) : undefined,
        firstTradeId: Number.isFinite(Number(data.f)) ? Number(data.f) : undefined,
        lastTradeId: Number.isFinite(Number(data.l)) ? Number(data.l) : undefined,
        time: Number(data.T),
        price: Number(data.p),
        quantity: Number(data.q),
        isBuyerMaker: Boolean(data.m),
    }

    if (!isValidTrade(trade)) return

    sourceState[source].lastTradeTimeMs = Date.now()
    metrics.tradesReceived[source] += 1
    queueAggregateBubbleCandidate(trade)
    for (const runtime of runtimes) {
        if (!runtime.activeSources.includes(source)) continue
        ingestTrade(runtime, trade)
    }
}

function getAggregateTradeCount(trade) {
    const firstTradeId = Number(trade.firstTradeId)
    const lastTradeId = Number(trade.lastTradeId)

    if (!Number.isFinite(firstTradeId) || !Number.isFinite(lastTradeId) || lastTradeId < firstTradeId) {
        return null
    }

    return Math.floor(lastTradeId - firstTradeId + 1)
}

function queueAggregateBubbleCandidate(trade) {
    const log = sourceLoggers[trade.source] || logger
    if (config.enableWrites && !config.dryRun && !config.aggregateBubbleWritesEnabled) {
        metrics.aggregateBubbles.skippedPersistenceDisabled += 1
        return
    }

    metrics.aggregateBubbles.received[trade.source] += 1

    if (!Number.isFinite(trade.id)) {
        metrics.aggregateBubbles.skippedMissingAggregateTradeId += 1
        return
    }

    const tradeCount = getAggregateTradeCount(trade)
    if (tradeCount === null) {
        metrics.aggregateBubbles.skippedInvalidTradeRange += 1
        return
    }

    const key = `${SYMBOL}:${trade.source}:${trade.id}`
    if (queuedAggregateBubbleKeys.has(key)) {
        metrics.aggregateBubbles.duplicatesSkipped += 1
        log.warn('duplicate aggregate bubble candidate skipped', { key })
        return
    }
    rememberBoundedSet(queuedAggregateBubbleKeys, key, config.maxDedupeKeys)

    const qualifiedBy = []
    if (trade.quantity >= config.aggregateBubbleMinVolume) {
        qualifiedBy.push('volume')
    }
    if (
        tradeCount >= config.aggregateBubbleMinTradeCount
        && trade.quantity >= config.aggregateBubbleMinTradeCountVolume
    ) {
        qualifiedBy.push('tradeCount')
    }

    if (qualifiedBy.length === 0) {
        metrics.aggregateBubbles.skippedBelowThreshold += 1
        return
    }

    if (queuedAggregateBubbleEvents.length >= config.maxQueuedBubbleEvents) {
        const dropCount = queuedAggregateBubbleEvents.length - config.maxQueuedBubbleEvents + 1
        queuedAggregateBubbleEvents.splice(0, dropCount)
        log.error('DATA_LOSS bubble queue cap exceeded, dropped oldest events', {
            droppedCount: dropCount,
            remainingCount: queuedAggregateBubbleEvents.length,
            maxCap: config.maxQueuedBubbleEvents,
        })
    }

    queuedAggregateBubbleEvents.push({
        symbol: SYMBOL,
        contractType: trade.source,
        aggregateTradeId: Math.floor(trade.id),
        eventTime: new Date(trade.time),
        eventTimeMs: trade.time,
        price: toStoredNumber(trade.price),
        side: trade.isBuyerMaker ? 'sell' : 'buy',
        volume: toStoredNumber(trade.quantity),
        tradeCount,
        firstTradeId: Math.floor(trade.firstTradeId),
        lastTradeId: Math.floor(trade.lastTradeId),
        createdAt: new Date(),
        storageVersion: 1,
        qualifiedBy,
        minVolumeAtIngest: toStoredNumber(config.aggregateBubbleMinVolume),
        minTradeCountAtIngest: config.aggregateBubbleMinTradeCount,
    })
    metrics.aggregateBubbles.qualified += 1
}

function ingestTrade(runtime, trade) {
    const log = sourceLoggers[trade.source] || logger
    const tradeKey = getTradeKey(trade)
    if (runtime.processedTradeKeys.has(tradeKey)) {
        metrics.tradesSkippedDuplicate += 1
        log.warn('duplicate trade skipped', { identity: runtime.identity, tradeKey })
        return
    }
    runtime.processedTradeKeys.add(tradeKey)

    const alignedPrice = getAlignedPrice(runtime.contractType, trade)
    if (!Number.isFinite(alignedPrice)) {
        metrics.tradesSkippedMissingReference += 1
        log.error('DATA_LOSS trade skipped due to missing price reference', {
            identity: runtime.identity,
            contractType: runtime.contractType,
            source: trade.source,
            tradeId: trade.id,
            tradePrice: trade.price,
        })
        return
    }

    const baseTime = getBaseTimeForTradeMs(trade.time)
    if (runtime.firstFullyCoveredBaseTimeBySource[trade.source] === null) {
        runtime.firstFullyCoveredBaseTimeBySource[trade.source] = baseTime + BASE_TIMEFRAME_SECONDS
    }
    runtime.latestBaseTimeBySource[trade.source] = Math.max(
        runtime.latestBaseTimeBySource[trade.source] ?? baseTime,
        baseTime,
    )

    const openRanges = runtime.taintedRangesBySource[trade.source]
    if (openRanges && openRanges.length > 0) {
        const lastRange = openRanges[openRanges.length - 1]
        if (lastRange && lastRange.end === null) {
            lastRange.end = baseTime
        }
    }

    aggregateFootprint(runtime, baseTime, alignedPrice, trade)
    aggregateProfile(runtime, baseTime, alignedPrice, trade)
    metrics.tradesAccepted += 1
}

function getAlignedPrice(contractType, trade) {
    if (trade.source === contractType) return trade.price
    return priceReferences[contractType]
}

function aggregateFootprint(runtime, baseTime, price, trade) {
    const bucketPrice = normalizePriceToBucket(price, FOOTPRINT_BUCKET_SIZE)
    const candleRows = runtime.footprintSlices.get(baseTime) ?? new Map()
    const row = candleRows.get(bucketPrice) ?? { bucketPrice, bidVol: 0, askVol: 0 }

    if (trade.isBuyerMaker) {
        row.bidVol += trade.quantity
    } else {
        row.askVol += trade.quantity
    }

    candleRows.set(bucketPrice, row)
    runtime.footprintSlices.set(baseTime, candleRows)
}

function aggregateProfile(runtime, baseTime, price, trade) {
    const baseBucketSize = runtime.profileBaseBucketSize
    const bucketPrice = normalizePriceToBucket(price, baseBucketSize)
    const candleRows = runtime.profileSlices.get(baseTime) ?? new Map()
    const row = candleRows.get(bucketPrice) ?? {
        candleTime: baseTime,
        baseBucketSize,
        bucketPrice,
        bidVol: 0,
        askVol: 0,
        totalVol: 0,
        tradeCount: 0,
        orderCount: 0,
    }

    if (trade.isBuyerMaker) {
        row.bidVol += trade.quantity
    } else {
        row.askVol += trade.quantity
    }

    row.totalVol += trade.quantity
    row.tradeCount += 1
    row.orderCount += getAggregateTradeCount(trade) ?? 1
    candleRows.set(bucketPrice, row)
    runtime.profileSlices.set(baseTime, candleRows)
}

async function persistAllEligibleSlices(reason) {
    if (shuttingDown && reason !== 'shutdown') return

    await persistAggregateBubbleEvents(reason)

    for (const runtime of runtimes) {
        await persistRuntimeEligibleSlices(runtime, reason)
    }
}

async function requestPersist(reason) {
    if (persistPromise) return persistPromise

    persistPromise = persistAllEligibleSlices(reason)
        .catch((error) => {
            metrics.writeFailures += 1
            logger.error('flush failed', { reason, error: getErrorMessage(error) })
        })
        .finally(() => {
            persistPromise = null
        })

    return persistPromise
}

async function persistRuntimeEligibleSlices(runtime, reason) {
    const coverageStart = getCoverageStart(runtime)
    const closedBeforeTime = getClosedBeforeTime(runtime)
    const sliceTimes = getSortedSliceTimes(runtime)

    if (sliceTimes.length === 0) return

    if (coverageStart === null || closedBeforeTime === null) {
        return
    }

    for (const sliceTime of sliceTimes) {
        if (sliceTime < coverageStart) {
            deleteSlice(runtime, sliceTime)
            continue
        }

        if (sliceTime >= closedBeforeTime) {
            continue
        }

        const persistedKey = `${runtime.identity}:${sliceTime}`
        if (runtime.persistedSlices.has(persistedKey)) {
            deleteSlice(runtime, sliceTime)
            continue
        }

        const footprintRows = Array.from(runtime.footprintSlices.get(sliceTime)?.values() ?? [])
        const profileRows = Array.from(runtime.profileSlices.get(sliceTime)?.values() ?? [])

        if (footprintRows.length === 0 && profileRows.length === 0) {
            deleteSlice(runtime, sliceTime)
            continue
        }

        try {
            const result = await writeClosedSlice(runtime, sliceTime, footprintRows, profileRows)
            runtime.persistedSlices.add(persistedKey)
            deleteSlice(runtime, sliceTime)
            runtime.firstWriteFailureAtMs = null
            metrics.slicesPersisted += 1
            metrics.footprintRowsInserted += result.footprint.inserted
            metrics.footprintRowsSkipped += result.footprint.skipped
            metrics.profileRowsInserted += result.profile.inserted
            metrics.profileRowsSkipped += result.profile.skipped
            const log = sourceLoggers[runtime.contractType] || logger
            log.info('closed 1m slice persisted', {
                reason,
                identity: runtime.identity,
                candleTime: sliceTime,
                footprintRowsWritten: result.footprint.inserted,
                footprintRowsSkipped: result.footprint.skipped,
                profileRowsWritten: result.profile.inserted,
                profileRowsSkipped: result.profile.skipped,
                baseBucketSize: runtime.profileBaseBucketSize,
            })
        } catch (error) {
            if (runtime.firstWriteFailureAtMs === null) {
                runtime.firstWriteFailureAtMs = Date.now()
            }
            metrics.writeFailures += 1
            const pendingSlices = getSortedSliceTimes(runtime).length
            const log = sourceLoggers[runtime.contractType] || logger
            log.error('closed 1m slice persist failed', {
                identity: runtime.identity,
                candleTime: sliceTime,
                pendingSlices,
                error: getErrorMessage(error),
            })

            const currentSlices = getSortedSliceTimes(runtime)
            if (currentSlices.length > config.maxBufferedSlices) {
                const oldestSlice = currentSlices[0]
                deleteSlice(runtime, oldestSlice)
                log.error('DATA_LOSS slice buffer cap exceeded due to persistent write failures, dropped oldest slice', {
                    identity: runtime.identity,
                    droppedSliceTime: oldestSlice,
                    remainingSlices: getSortedSliceTimes(runtime).length,
                    maxCap: config.maxBufferedSlices,
                    failureDurationMs: Date.now() - runtime.firstWriteFailureAtMs,
                })
            }
            return
        }
    }
}


async function writeClosedSlice(runtime, sliceTime, footprintRows, profileRows) {
    const log = sourceLoggers[runtime.contractType] || logger
    const footprintDocuments = toFootprintDocuments(runtime, sliceTime, footprintRows)
    const profileDocuments = toProfileDocuments(runtime, sliceTime, profileRows)

    if (config.dryRun || !config.enableWrites) {
        log.info('dry-run closed slice write', {
            identity: runtime.identity,
            candleTime: sliceTime,
            footprintRows: footprintDocuments.length,
            profileRows: profileDocuments.length,
            writesEnabled: config.enableWrites,
            dryRun: config.dryRun,
        })
        return {
            footprint: { inserted: 0, skipped: footprintDocuments.length },
            profile: { inserted: 0, skipped: profileDocuments.length },
        }
    }

    // Try the write; if it fails, just throw so it can be retried on the next interval
    try {
        return await executeSliceWrite(runtime, sliceTime, footprintDocuments, profileDocuments)
    } catch (firstError) {
        log.warn('slice write failed, data remains in memory to retry on next interval', {
            identity: runtime.identity,
            candleTime: sliceTime,
            error: getErrorMessage(firstError),
        })
        throw firstError
    }
}

async function executeSliceWrite(runtime, sliceTime, footprintDocuments, profileDocuments) {
    const footprint = await insertMissingFootprintDocuments(footprintDocuments)
    const profile = await insertMissingProfileDocuments(profileDocuments)

    const now = new Date().toISOString()
    await updateCollectorMeta({
        last_collector_heartbeat: now,
        collector_symbol: SYMBOL,
        collector_profile_base_bucket_size: String(runtime.profileBaseBucketSize),
        last_footprint_stored: footprint.inserted > 0 ? now : undefined,
        last_profile_rows_stored: profile.inserted > 0 ? now : undefined,
    })

    return { footprint, profile }
}

async function persistAggregateBubbleEvents(reason) {
    if (queuedAggregateBubbleEvents.length === 0) return

    const batch = queuedAggregateBubbleEvents.slice(0, config.aggregateBubbleFlushSize)

    if (config.enableWrites && !config.dryRun && !config.aggregateBubbleWritesEnabled) {
        queuedAggregateBubbleEvents.splice(0, batch.length)
        metrics.aggregateBubbles.skippedPersistenceDisabled += batch.length
        logger.warn('aggregate bubble candidate write skipped; persistence disabled', {
            reason,
            rows: batch.length,
        })
        return
    }

    if (config.dryRun || !config.enableWrites) {
        queuedAggregateBubbleEvents.splice(0, batch.length)
        logger.info('dry-run aggregate bubble candidate write', {
            reason,
            rows: batch.length,
            writesEnabled: config.enableWrites,
            dryRun: config.dryRun,
            thresholds: {
                minVolume: config.aggregateBubbleMinVolume,
                minTradeCount: config.aggregateBubbleMinTradeCount,
                minTradeCountVolume: config.aggregateBubbleMinTradeCountVolume,
            },
        })
        return
    }

    try {
        const result = await insertAggregateBubbleDocuments(batch)
        queuedAggregateBubbleEvents.splice(0, batch.length)
        metrics.aggregateBubbles.inserted += result.inserted
        metrics.aggregateBubbles.duplicatesSkipped += result.duplicatesSkipped

        if (result.inserted > 0 || result.duplicatesSkipped > 0) {
            await updateCollectorMeta({
                last_aggregate_bubbles_stored: result.inserted > 0 ? new Date().toISOString() : undefined,
                aggregate_bubble_thresholds: JSON.stringify({
                    minVolume: config.aggregateBubbleMinVolume,
                    minTradeCount: config.aggregateBubbleMinTradeCount,
                    minTradeCountVolume: config.aggregateBubbleMinTradeCountVolume,
                }),
            })
        }

        logger.info('aggregate bubble candidates persisted', {
            reason,
            rowsSubmitted: batch.length,
            rowsInserted: result.inserted,
            duplicatesSkipped: result.duplicatesSkipped,
            pendingRows: queuedAggregateBubbleEvents.length,
        })
    } catch (error) {
        metrics.aggregateBubbles.insertFailed += batch.length
        metrics.writeFailures += 1
        logger.error('aggregate bubble candidate persist failed', {
            reason,
            rows: batch.length,
            pendingRows: queuedAggregateBubbleEvents.length,
            error: getErrorMessage(error),
        })
    }
}

function toFootprintDocuments(runtime, sliceTime, rows) {
    const time = new Date(sliceTime * 1000)
    const storedAt = new Date()

    return rows
        .filter((row) => (
            Number.isFinite(row.bucketPrice)
            && Number.isFinite(row.bidVol)
            && Number.isFinite(row.askVol)
            && row.bidVol + row.askVol > 0
        ))
        .map((row) => {
            const totalVol = row.bidVol + row.askVol
            const delta = row.askVol - row.bidVol

            return {
                time,
                meta: {
                    symbol: runtime.symbol,
                    contractType: runtime.contractType,
                    dataSourceMode: runtime.dataSourceMode,
                    timeframe: BASE_TIMEFRAME,
                    bucketSize: FOOTPRINT_BUCKET_SIZE,
                },
                candleTimeSec: sliceTime,
                bucketPrice: toStoredNumber(row.bucketPrice),
                bucketPriceKey: toNumberKey(row.bucketPrice),
                bidVol: toStoredNumber(row.bidVol),
                askVol: toStoredNumber(row.askVol),
                totalVol: toStoredNumber(totalVol),
                delta: toStoredNumber(delta),
                storedAt,
            }
        })
}

function toProfileDocuments(runtime, sliceTime, rows) {
    const time = new Date(sliceTime * 1000)
    const storedAt = new Date()
    const baseBucketSizeKey = toNumberKey(runtime.profileBaseBucketSize)

    return rows
        .filter((row) => (
            Number.isFinite(row.candleTime)
            && row.baseBucketSize > 0
            && Number.isFinite(row.bucketPrice)
            && row.totalVol > 0
            && row.tradeCount > 0
        ))
        .map((row) => ({
            time,
            meta: {
                symbol: runtime.symbol,
                contractType: runtime.contractType,
                dataSourceMode: runtime.dataSourceMode,
                timeframe: BASE_TIMEFRAME,
                baseBucketSizeKey,
            },
            candleTimeSec: sliceTime,
            baseBucketSize: toStoredNumber(row.baseBucketSize),
            bucketPrice: toStoredNumber(row.bucketPrice),
            bucketPriceKey: toNumberKey(row.bucketPrice),
            bidVol: toStoredNumber(row.bidVol),
            askVol: toStoredNumber(row.askVol),
            totalVol: toStoredNumber(row.totalVol),
            tradeCount: Math.max(0, Math.floor(row.tradeCount)),
            orderCount: Math.max(0, Math.floor(row.orderCount || row.tradeCount)),
            storedAt,
        }))
}

async function insertMissingFootprintDocuments(documents) {
    if (documents.length === 0) return { inserted: 0, skipped: 0 }

    const values = []
    const placeholders = []
    let paramIndex = 1

    for (const doc of documents) {
        placeholders.push(`($${paramIndex++}, $${paramIndex++}, $${paramIndex++}, $${paramIndex++}, $${paramIndex++}, $${paramIndex++}, $${paramIndex++}, $${paramIndex++}, $${paramIndex++}, $${paramIndex++}, $${paramIndex++}, $${paramIndex++})`)

        values.push(
            doc.time,
            doc.meta.symbol,
            doc.meta.contractType,
            doc.meta.dataSourceMode,
            doc.meta.timeframe,
            doc.meta.bucketSize,
            doc.candleTimeSec,
            Number(doc.bucketPrice),
            Number(doc.bidVol),
            Number(doc.askVol),
            Number(doc.totalVol),
            Number(doc.delta)
        )
    }

    const sql = `
    INSERT INTO footprint_cells (
      time, symbol, contract_type, data_source_mode, timeframe, bucket_size, 
      candle_time_sec, bucket_price, bid_vol, ask_vol, total_vol, delta
    ) VALUES ${placeholders.join(', ')}
    ON CONFLICT (symbol, contract_type, data_source_mode, timeframe, bucket_size, time, bucket_price) DO NOTHING
  `

    const result = await pgPool.query(sql, values)
    const inserted = result.rowCount ?? 0
    return { inserted, skipped: documents.length - inserted }
}

async function insertMissingProfileDocuments(documents) {
    if (documents.length === 0) return { inserted: 0, skipped: 0 }

    const values = []
    const placeholders = []
    let paramIndex = 1

    for (const doc of documents) {
        placeholders.push(`($${paramIndex++}, $${paramIndex++}, $${paramIndex++}, $${paramIndex++}, $${paramIndex++}, $${paramIndex++}, $${paramIndex++}, $${paramIndex++}, $${paramIndex++}, $${paramIndex++}, $${paramIndex++}, $${paramIndex++}, $${paramIndex++})`)

        values.push(
            doc.time,
            doc.meta.symbol,
            doc.meta.contractType,
            doc.meta.dataSourceMode,
            doc.meta.timeframe,
            doc.candleTimeSec,
            Number(doc.baseBucketSize),
            Number(doc.bucketPrice),
            Number(doc.bidVol),
            Number(doc.askVol),
            Number(doc.totalVol),
            doc.tradeCount,
            doc.orderCount
        )
    }

    const sql = `
    INSERT INTO profile_rows (
      time, symbol, contract_type, data_source_mode, timeframe, 
      candle_time_sec, base_bucket_size, bucket_price, bid_vol, ask_vol, total_vol, trade_count, order_count
    ) VALUES ${placeholders.join(', ')}
    ON CONFLICT (symbol, contract_type, data_source_mode, timeframe, base_bucket_size, time, bucket_price) DO NOTHING
  `

    const result = await pgPool.query(sql, values)
    const inserted = result.rowCount ?? 0
    return { inserted, skipped: documents.length - inserted }
}

async function insertAggregateBubbleDocuments(documents) {
    if (documents.length === 0) return { inserted: 0, duplicatesSkipped: 0 }

    const values = []
    const placeholders = []
    let paramIndex = 1

    for (const doc of documents) {
        placeholders.push(`($${paramIndex++}, $${paramIndex++}, $${paramIndex++}, $${paramIndex++}, $${paramIndex++}, $${paramIndex++}, $${paramIndex++}, $${paramIndex++}, $${paramIndex++}, $${paramIndex++}, $${paramIndex++}, $${paramIndex++}, $${paramIndex++}, $${paramIndex++})`)

        values.push(
            doc.eventTime,
            doc.symbol,
            doc.contractType,
            doc.aggregateTradeId,
            doc.eventTimeMs,
            Number(doc.price),
            doc.side,
            Number(doc.volume),
            doc.tradeCount,
            doc.firstTradeId,
            doc.lastTradeId,
            doc.qualifiedBy,
            Number(doc.minVolumeAtIngest),
            doc.minTradeCountAtIngest
        )
    }

    const sql = `
    INSERT INTO aggregate_bubble_events (
      event_time, symbol, contract_type, aggregate_trade_id, event_time_ms, price, side, 
      volume, trade_count, first_trade_id, last_trade_id, qualified_by, min_volume_at_ingest, min_trade_count_at_ingest
    ) VALUES ${placeholders.join(', ')}
    ON CONFLICT (symbol, contract_type, aggregate_trade_id, event_time) DO NOTHING
  `

    const result = await pgPool.query(sql, values)
    const inserted = result.rowCount ?? 0
    return { inserted, duplicatesSkipped: documents.length - inserted }
}

async function updateCollectorMeta(values) {
    const updates = Object.entries(values)
        .filter(([, value]) => value !== undefined)
        .map(async ([key, value]) => {
            await pgPool.query(
                `INSERT INTO collector_meta (key, value) VALUES ($1, $2)
         ON CONFLICT (key) DO UPDATE SET value = $2, updated_at = NOW()`,
                [key, String(value)]
            )
        })

    await Promise.all(updates)
}

function markSourceGap(source, start = null, end = null) {
    const log = sourceLoggers[source] || logger
    for (const runtime of runtimes) {
        if (!runtime.activeSources.includes(source)) continue

        const rangeStart = start ?? runtime.latestBaseTimeBySource[source] ?? Date.now()
        const rangeEnd = end ?? null
        runtime.taintedRangesBySource[source].push({ start: rangeStart, end: rangeEnd })
        runtime.latestBaseTimeBySource[source] = null
    }
    priceReferences[source] = null
    log.error('DATA_GAP backfill exhausted retries, range marked tainted', {
        source,
        rangeStart: start ?? 'unspecified',
        rangeEnd: end ?? 'open',
    })

    if (config.enableWrites && !config.dryRun && pgPool) {
        const rangeStart = start ?? null
        const rangeEnd = end ?? null
        // Write both the JSON blob (existing) and a structured row in collector_gaps (new)
        updateCollectorMeta({
            tainted_ranges: JSON.stringify(getAllTaintedRanges()),
        }).catch((err) => {
            logger.error('failed to record tainted ranges to collector_meta', { error: getErrorMessage(err) })
        })
        insertGapRecord(source, rangeStart, rangeEnd, 'backfill_exhausted').catch((err) => {
            logger.error('failed to insert gap record into collector_gaps', { error: getErrorMessage(err) })
        })
    }
}

/**
 * Insert a structured gap record into the collector_gaps audit table.
 * Requires migration: scripts/collector/migrations/001_collector_gaps.sql
 */
async function insertGapRecord(source, gapStart, gapEnd, reason) {
    if (!pgPool) return
    await pgPool.query(
        `INSERT INTO collector_gaps (source, gap_start, gap_end, reason)
     VALUES ($1, $2, $3, $4)`,
        [source, gapStart, gapEnd, reason]
    )
}

function hasActiveTaintedRanges() {
    for (const runtime of runtimes) {
        for (const source of runtime.activeSources) {
            if (runtime.taintedRangesBySource[source]?.length > 0) {
                return true
            }
        }
    }
    return false
}

function getAllTaintedRanges() {
    const ranges = {}
    for (const source of SOURCES) {
        ranges[source] = []
        for (const runtime of runtimes) {
            if (runtime.taintedRangesBySource[source]) {
                ranges[source].push(...runtime.taintedRangesBySource[source])
            }
        }
    }
    return ranges
}

function getCoverageStart(runtime) {
    const times = runtime.activeSources.map((source) => runtime.firstFullyCoveredBaseTimeBySource[source])
    if (times.some((time) => time === null)) return null
    return Math.max(...times)
}

function getClosedBeforeTime(runtime) {
    for (const source of runtime.activeSources) {
        if (!sourceState[source].connected || sourceState[source].isBackfilling) {
            return null
        }
    }

    const times = runtime.activeSources.map((source) => runtime.latestBaseTimeBySource[source])
    if (times.some((time) => time === null)) return null
    return Math.min(...times)
}

function getSortedSliceTimes(runtime) {
    return Array.from(new Set([
        ...runtime.footprintSlices.keys(),
        ...runtime.profileSlices.keys(),
    ])).sort((a, b) => a - b)
}

function deleteSlice(runtime, sliceTime) {
    runtime.footprintSlices.delete(sliceTime)
    runtime.profileSlices.delete(sliceTime)
}

let lastStatusMetrics = {
    tradesReceived: { spot: 0, futures: 0 },
    tradesAccepted: 0,
    tradesSkippedDuplicate: 0,
    tradesSkippedMissingReference: 0,
    slicesPersisted: 0,
    writeFailures: 0,
    bubblesQualified: 0,
    bubblesInserted: 0,
}

/**
 * Returns 'degraded' if any runtime has an active unresolved write failure,
 * or if any pending slice is older than PERSISTENCE_WARN_THRESHOLD_MS.
 * This is separate from 'health' (which reflects tainted/gap ranges).
 */
function getPersistenceHealth() {
    const nowMs = Date.now()
    for (const runtime of runtimes) {
        if (runtime.firstWriteFailureAtMs !== null) {
            return 'degraded'
        }
        const sliceTimes = getSortedSliceTimes(runtime)
        if (sliceTimes.length > 0) {
            const oldestSliceMs = sliceTimes[0] * 1000
            if (nowMs - oldestSliceMs > PERSISTENCE_WARN_THRESHOLD_MS) {
                return 'degraded'
            }
        }
    }
    return 'ok'
}

/**
 * Returns the age in ms of the oldest in-memory slice across all runtimes,
 * or null if there are no pending slices.
 */
function getOldestPendingSliceAgeMs() {
    const nowMs = Date.now()
    let oldest = null
    for (const runtime of runtimes) {
        const sliceTimes = getSortedSliceTimes(runtime)
        if (sliceTimes.length > 0) {
            const ageMs = nowMs - sliceTimes[0] * 1000
            if (oldest === null || ageMs > oldest) {
                oldest = ageMs
            }
        }
    }
    return oldest
}

async function logStatus() {
    const pendingSlices = runtimes.reduce((total, runtime) => total + getSortedSliceTimes(runtime).length, 0)
    const health = hasActiveTaintedRanges() ? 'DEGRADED' : 'ok'
    const persistenceHealth = getPersistenceHealth()
    const oldestPendingSliceAgeMs = getOldestPendingSliceAgeMs()

    // Warn or trigger watchdog if slices are aging dangerously
    const isAnyBackfilling = SOURCES.some((s) => sourceState[s].isBackfilling)
    if (persistenceHealth === 'degraded' && oldestPendingSliceAgeMs !== null) {
        if (!isAnyBackfilling && oldestPendingSliceAgeMs > PERSISTENCE_WARN_THRESHOLD_MS && pendingSlices > 0) {
            logger.error('PERSISTENCE_WATCHDOG_TRIPPED oldest pending slice exceeded threshold while not backfilling, triggering restart', {
                oldestPendingSliceAgeMs,
                thresholdMs: PERSISTENCE_WARN_THRESHOLD_MS,
                pendingSlices,
            })
            void safeExit(1, 'persistence_stall')
        } else {
            logger.warn('PERSISTENCE_DEGRADED oldest pending slice is aging, data loss risk increasing', {
                oldestPendingSliceAgeMs,
                warnThresholdMs: PERSISTENCE_WARN_THRESHOLD_MS,
                pendingSlices,
                isAnyBackfilling,
            })
        }
    }

    const sinceLastInterval = {
        tradesReceived: {
            spot: metrics.tradesReceived.spot - lastStatusMetrics.tradesReceived.spot,
            futures: metrics.tradesReceived.futures - lastStatusMetrics.tradesReceived.futures,
        },
        tradesAccepted: metrics.tradesAccepted - lastStatusMetrics.tradesAccepted,
        tradesSkippedDuplicate: metrics.tradesSkippedDuplicate - lastStatusMetrics.tradesSkippedDuplicate,
        tradesSkippedMissingReference: metrics.tradesSkippedMissingReference - lastStatusMetrics.tradesSkippedMissingReference,
        slicesPersisted: metrics.slicesPersisted - lastStatusMetrics.slicesPersisted,
        writeFailures: metrics.writeFailures - lastStatusMetrics.writeFailures,
        bubblesQualified: metrics.aggregateBubbles.qualified - lastStatusMetrics.bubblesQualified,
        bubblesInserted: metrics.aggregateBubbles.inserted - lastStatusMetrics.bubblesInserted,
    }

    lastStatusMetrics = {
        tradesReceived: { ...metrics.tradesReceived },
        tradesAccepted: metrics.tradesAccepted,
        tradesSkippedDuplicate: metrics.tradesSkippedDuplicate,
        tradesSkippedMissingReference: metrics.tradesSkippedMissingReference,
        slicesPersisted: metrics.slicesPersisted,
        writeFailures: metrics.writeFailures,
        bubblesQualified: metrics.aggregateBubbles.qualified,
        bubblesInserted: metrics.aggregateBubbles.inserted,
    }

    logger.info('collector status', {
        health,
        persistenceHealth,
        oldestPendingSliceAgeMs,
        pendingSlices,
        pendingBubbles: queuedAggregateBubbleEvents.length,
        sinceLastInterval,
        cumulative: {
            tradesReceived: metrics.tradesReceived,
            tradesAccepted: metrics.tradesAccepted,
            tradesSkippedDuplicate: metrics.tradesSkippedDuplicate,
            tradesSkippedMissingReference: metrics.tradesSkippedMissingReference,
            slicesPersisted: metrics.slicesPersisted,
            writeFailures: metrics.writeFailures,
            aggregateBubbles: metrics.aggregateBubbles,
        },
    })

    if (config.enableWrites && !config.dryRun && pgPool) {
        await updateCollectorMeta({
            last_collector_heartbeat: new Date().toISOString(),
            last_spot_trade_time_ms: sourceState.spot.lastTradeTimeMs ? String(sourceState.spot.lastTradeTimeMs) : undefined,
            last_futures_trade_time_ms: sourceState.futures.lastTradeTimeMs ? String(sourceState.futures.lastTradeTimeMs) : undefined,
            collector_status: JSON.stringify({
                symbol: SYMBOL,
                health,
                persistenceHealth,
                oldestPendingSliceAgeMs,
                pendingSlices,
                pendingAggregateBubbleEvents: queuedAggregateBubbleEvents.length,
                tradesReceived: metrics.tradesReceived,
                aggregateBubbles: metrics.aggregateBubbles,
                slicesPersisted: metrics.slicesPersisted,
                writeFailures: metrics.writeFailures,
            }),
        })

        // Informational size log.
        try {
            const stats = await pgPool.query('SELECT pg_database_size(current_database()) as size')
            const dataSizeMB = (parseInt(stats.rows[0].size || '0', 10) / 1024 / 1024).toFixed(2)
            logger.info('database size report', {
                dataSizeMB,
                retentionDays: DEFAULT_RETENTION_DAYS,
            })
        } catch (e) {
            logger.warn('could not read database size', { error: getErrorMessage(e) })
        }
    }
}


async function shutdown(signal, flushTimer, statusTimer) {
    if (shuttingDown) return
    shuttingDown = true
    logger.info('collector shutting down', { signal })
    clearInterval(flushTimer)
    clearInterval(statusTimer)

    try {
        if (persistPromise) {
            await persistPromise
        }
        await persistAllEligibleSlices('shutdown')
    } catch (error) {
        logger.error('shutdown flush failed', { error: getErrorMessage(error) })
    }

    streamClients.forEach((client) => client.close())
    if (pgPool) {
        await pgPool.end()
    }
    sourceLoggers.spot.flush?.()
    sourceLoggers.futures.flush?.()
    logger.flush?.()
    logger.info('collector stopped')
    process.exit(0)
}

function getTradeKey(trade) {
    if (Number.isFinite(trade.id)) return `${trade.source}:id:${trade.id}`
    return `${trade.source}:${trade.time}:${trade.price}:${trade.quantity}:${trade.isBuyerMaker ? 1 : 0}`
}

function getBaseTimeForTradeMs(timeMs) {
    return Math.floor((timeMs / 1000) / BASE_TIMEFRAME_SECONDS) * BASE_TIMEFRAME_SECONDS
}

function normalizePriceToBucket(price, bucketSize) {
    return Math.floor(price / bucketSize) * bucketSize
}

function toStoredNumber(value) {
    return Number.isFinite(value) ? String(value) : '0'
}

function toNumberKey(value) {
    if (!Number.isFinite(value)) return '0'
    const fixed = value.toFixed(12).replace(/\.?0+$/, '')
    return fixed === '-0' || fixed === '' ? '0' : fixed
}



function isValidTrade(trade) {
    return SOURCES.includes(trade.source)
        && Number.isFinite(trade.time)
        && Number.isFinite(trade.price)
        && Number.isFinite(trade.quantity)
        && trade.quantity > 0
}

// function getNumberEnv(name, fallback) {
//   const value = Number(process.env[name])
//   return Number.isFinite(value) && value > 0 ? value : fallback
// }

// function getIntegerEnv(name, fallback) {
//   return Math.floor(getNumberEnv(name, fallback))
// }

export function getRestBaseUrl(source) {
    const isSpot = source === 'spot'
    const proxyRoot = process.env.BINANCE_REST_PROXY_URL?.replace(/\/+$/, '')

    if (isSpot) {
        if (process.env.BINANCE_SPOT_REST_URL) {
            return process.env.BINANCE_SPOT_REST_URL.replace(/\/+$/, '')
        }
        if (proxyRoot) {
            return `${proxyRoot}/api/v3`
        }
        return 'https://data-api.binance.vision/api/v3'
    }

    // Futures
    if (process.env.BINANCE_FUTURES_REST_URL) {
        return process.env.BINANCE_FUTURES_REST_URL.replace(/\/+$/, '')
    }
    if (proxyRoot) {
        return `${proxyRoot}/fapi/v1`
    }
    return 'https://fapi.binance.com/fapi/v1'
}

let proxyAgentInstance = null
let lastConfiguredProxy = null

export function getProxyAgent() {
    const proxyUrl = process.env.BINANCE_PROXY_URL || process.env.HTTPS_PROXY || process.env.HTTP_PROXY
    if (!proxyUrl) return undefined
    if (proxyAgentInstance && lastConfiguredProxy === proxyUrl) {
        return proxyAgentInstance
    }
    lastConfiguredProxy = proxyUrl
    proxyAgentInstance = new ProxyAgent(proxyUrl)
    return proxyAgentInstance
}

async function runBackfill(source, startTime, endTime) {
    const log = sourceLoggers[source] || logger
    if (endTime - startTime < 1000) {
        return { ok: true, cursor: endTime }
    }

    log.info('starting auto-backfill for gap', { source, gapMs: endTime - startTime })

    const baseUrl = getRestBaseUrl(source)
    const isFutures = source === 'futures'
    const dispatcher = (isFutures || process.env.PROXY_ALL === 'true') ? getProxyAgent() : undefined
    let currentStartTime = startTime
    let totalFetched = 0

    while (currentStartTime < endTime) {
        if (shuttingDown) break
        const url = `${baseUrl}/aggTrades?symbol=${SYMBOL}&startTime=${currentStartTime}&endTime=${endTime}&limit=1000`

        try {
            const fetchFn = dispatcher ? undiciFetch : globalThis.fetch
            const response = await fetchFn(url, {
                signal: AbortSignal.timeout(10000),
                ...(dispatcher ? { dispatcher } : {}),
            })
            if (!response.ok) {
                log.error('backfill request failed', { source, status: response.status, statusText: response.statusText })
                break
            }

            const trades = await response.json()
            if (trades.length === 0) {
                currentStartTime = endTime
                break
            }

            for (const data of trades) {
                const trade = {
                    source,
                    id: Number.isFinite(Number(data.a)) ? Number(data.a) : undefined,
                    firstTradeId: Number.isFinite(Number(data.f)) ? Number(data.f) : undefined,
                    lastTradeId: Number.isFinite(Number(data.l)) ? Number(data.l) : undefined,
                    time: Number(data.T),
                    price: Number(data.p),
                    quantity: Number(data.q),
                    isBuyerMaker: Boolean(data.m),
                }

                if (isValidTrade(trade)) {
                    sourceState[source].lastTradeTimeMs = Math.max(sourceState[source].lastTradeTimeMs ?? 0, trade.time)
                    queueAggregateBubbleCandidate(trade)
                    for (const runtime of runtimes) {
                        if (!runtime.activeSources.includes(source)) continue
                        ingestTrade(runtime, trade)
                    }
                }
            }

            totalFetched += trades.length

            const lastTradeTime = trades[trades.length - 1].T
            if (trades.length < 1000) {
                currentStartTime = endTime
                break
            }

            currentStartTime = lastTradeTime + 1
            await new Promise(resolve => setTimeout(resolve, 100))
        } catch (error) {
            log.error('backfill network error', { source, error: getErrorMessage(error) })
            break
        }
    }

    const completed = currentStartTime >= endTime
    if (completed) {
        log.info('auto-backfill completed', { source, totalFetched })
    }
    return { ok: completed, cursor: currentStartTime }
}

async function runBackfillUntilComplete(source, startTime) {
    const log = sourceLoggers[source] || logger
    let cursor = startTime
    let attempt = 0
    const maxAttempts = 20

    while (!shuttingDown) {
        const endTime = Date.now()
        const result = await runBackfill(source, cursor, endTime)
        if (result.ok) {
            return { ok: true, cursor: endTime }
        }

        cursor = result.cursor
        attempt += 1
        if (attempt > maxAttempts) {
            return { ok: false, cursor }
        }

        const delay = Math.min(
            config.reconnectMaxMs,
            config.reconnectMinMs * 2 ** Math.min(attempt - 1, 10),
        )
        log.warn('backfill incomplete, retrying', {
            source,
            attempt,
            delayMs: delay,
            cursor,
        })
        await new Promise((r) => setTimeout(r, delay))
    }
    return { ok: false, cursor }
}



function describeWebSocketEvent(event) {
    if (!event) return 'unknown websocket error'
    if ('message' in event && event.message) return String(event.message)
    if ('type' in event && event.type) return String(event.type)
    return String(event)
}

function getErrorMessage(error) {
    return error instanceof Error ? error.message : String(error)
}

function handlePipeError(error) {
    if (error?.code === 'EPIPE') {
        process.exit(0)
    }
}

function rememberBoundedSet(set, value, limit) {
    set.add(value)
    while (set.size > limit) {
        const oldest = set.values().next().value
        if (oldest === undefined) break
        set.delete(oldest)
    }
}

class BoundedSet {
    constructor(limit) {
        this.limit = limit
        this.values = new Set()
    }

    has(value) {
        return this.values.has(value)
    }

    add(value) {
        this.values.add(value)
        while (this.values.size > this.limit) {
            const oldest = this.values.values().next().value
            if (oldest === undefined) break
            this.values.delete(oldest)
        }
    }
}

export const _test = {
    config,
    runtimes,
    metrics,
    sourceState,
    sourceLoggers,
    logger,
    createBurstCollapsingLogger,
    createSourceLogger,
    queuedAggregateBubbleEvents,
    queuedAggregateBubbleKeys,
    priceReferences,
    createRuntime,
    markSourceGap,
    hasActiveTaintedRanges,
    getAllTaintedRanges,
    getCoverageStart,
    getClosedBeforeTime,
    getSortedSliceTimes,
    deleteSlice,
    ingestTrade,
    handleStreamMessage,
    queueAggregateBubbleCandidate,
    runBackfill,
    runBackfillUntilComplete,
    persistRuntimeEligibleSlices,
    logStatus,
    getPersistenceHealth,
    getOldestPendingSliceAgeMs,
    insertGapRecord,
    setPgPool: (p) => { pgPool = p },
    setShuttingDown: (val) => { shuttingDown = val },
    setPersistPromise: (val) => { persistPromise = val },
    getRuntimes: () => runtimes,
    setRuntimes: (val) => { runtimes.length = 0; runtimes.push(...val) },
    setExitFn: (fn) => { exitFn = fn },
    getExitFn: () => exitFn,
    safeExit,
    seedWatermarksFromDatabase,
    getRestBaseUrl,
    getProxyAgent,
    MAX_BACKFILL_GAP_MS,
}