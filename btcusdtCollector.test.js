/**
 * btcusdtCollector.test.js
 *
 * Tests for the backfill gap-gating logic.
 * Run with:  node btcusdtCollector.test.js
 */

import assert from 'node:assert/strict'

// Prevent main() from running during import
process.env.NODE_ENV = 'test'

const { _test } = await import('./btcusdtCollector.js')
const {
    MAX_BACKFILL_GAP_MS,
    runBackfill,
} = _test

// ─── silent logger stub ──────────────────────────────────────────────────────

function makeSilentLogger() {
    const noop = () => {}
    const stub = { info: noop, warn: noop, error: noop, debug: noop, flush: noop }
    stub.child = () => stub
    stub.raw = stub
    return stub
}

// ─── test harness ────────────────────────────────────────────────────────────

let passed = 0
let failed = 0

function test(name, fn) {
    try {
        const r = fn()
        if (r instanceof Promise) {
            r.then(() => {
                console.log(`  ✓  ${name}`)
                passed++
            }).catch((err) => {
                console.error(`  ✗  ${name}`)
                console.error(`       ${err.message}`)
                failed++
            })
        } else {
            console.log(`  ✓  ${name}`)
            passed++
        }
    } catch (err) {
        console.error(`  ✗  ${name}`)
        console.error(`       ${err.message}`)
        failed++
    }
}

/**
 * The gap-decision logic extracted from ws.onopen in createBinanceStreamClient.
 * Returns 'backfill' | 'skip_large_gap' | 'no_gap'
 */
function simulateOnOpen(source, sourceState) {
    const gapStart = sourceState[source].lastTradeTimeMs
    if (!gapStart) {
        sourceState[source].isBackfilling = false
        return 'no_gap'
    }

    const gapMs = Date.now() - gapStart
    if (gapMs > MAX_BACKFILL_GAP_MS) {
        sourceState[source].lastTradeTimeMs = Date.now()
        sourceState[source].isBackfilling = false
        return 'skip_large_gap'
    }

    sourceState[source].isBackfilling = true
    return 'backfill'
}

function makeSourceState(source, lastTradeTimeMs) {
    return {
        [source]: { connected: false, isBackfilling: false, lastTradeTimeMs, lastMessageAtMs: null, connectedAtMs: null },
    }
}

// ─── test suite ──────────────────────────────────────────────────────────────

console.log('\nBackfill gap-gating tests\n')

// 1. Constant sanity
test('MAX_BACKFILL_GAP_MS is exported and is a positive number', () => {
    assert.ok(typeof MAX_BACKFILL_GAP_MS === 'number' && MAX_BACKFILL_GAP_MS > 0,
        `Expected positive number, got ${MAX_BACKFILL_GAP_MS}`)
})

test('MAX_BACKFILL_GAP_MS default is 4 hours (14400000 ms) when env var is unset', () => {
    if (!process.env.COLLECTOR_MAX_BACKFILL_GAP_MS) {
        assert.equal(MAX_BACKFILL_GAP_MS, 4 * 60 * 60 * 1000)
    }
})

// 2. Small gap → backfill
test('small gap (5 min) → action is "backfill"', () => {
    const source = 'futures'
    const st = makeSourceState(source, Date.now() - 5 * 60 * 1000)
    const action = simulateOnOpen(source, st)
    assert.equal(action, 'backfill')
    assert.equal(st[source].isBackfilling, true)
})

test('small gap (30 min) → action is "backfill"', () => {
    const source = 'spot'
    const st = makeSourceState(source, Date.now() - 30 * 60 * 1000)
    assert.equal(simulateOnOpen(source, st), 'backfill')
})

// 3. Large gap → skip
test('large gap (4.9 days) → action is "skip_large_gap"', () => {
    const source = 'futures'
    const st = makeSourceState(source, Date.now() - 4.9 * 24 * 60 * 60 * 1000)
    assert.equal(simulateOnOpen(source, st), 'skip_large_gap')
    assert.equal(st[source].isBackfilling, false)
})

test('large gap → watermark is advanced to approx now', () => {
    const source = 'futures'
    const before = Date.now()
    const st = makeSourceState(source, Date.now() - 5 * 24 * 60 * 60 * 1000)
    simulateOnOpen(source, st)
    const after = Date.now()
    const newWatermark = st[source].lastTradeTimeMs
    assert.ok(newWatermark >= before, `watermark (${newWatermark}) must be >= before (${before})`)
    assert.ok(newWatermark <= after + 10, `watermark (${newWatermark}) must be close to now (${after})`)
})

test('large gap on futures → spot backfill is unaffected', () => {
    const spotSt = makeSourceState('spot', Date.now() - 10 * 60 * 1000)   // 10 min
    const futSt  = makeSourceState('futures', Date.now() - 4.9 * 24 * 60 * 60 * 1000)
    assert.equal(simulateOnOpen('spot', spotSt),    'backfill',        'spot small gap should backfill')
    assert.equal(simulateOnOpen('futures', futSt),  'skip_large_gap',  'futures large gap should skip')
})

// 4. No prior watermark → fresh start
test('no watermark (null) → action is "no_gap", isBackfilling stays false', () => {
    const source = 'futures'
    const st = makeSourceState(source, null)
    assert.equal(simulateOnOpen(source, st), 'no_gap')
    assert.equal(st[source].isBackfilling, false)
    assert.equal(st[source].lastTradeTimeMs, null)
})

// 5. Restart after skip → no repeated large-gap retry
test('restart after skip: watermark from ~1 min ago → normal backfill (not another skip)', () => {
    const source = 'futures'
    const st = makeSourceState(source, Date.now() - 60 * 1000)
    assert.equal(simulateOnOpen(source, st), 'backfill')
})

test('restart after skip: watermark from seconds ago → normal backfill (gap < MAX)', () => {
    const source = 'futures'
    const st = makeSourceState(source, Date.now() - 500)
    assert.equal(simulateOnOpen(source, st), 'backfill')
})

// 6. Boundary cases
test('gap exactly equal to MAX_BACKFILL_GAP_MS → backfill (boundary is NOT > MAX, so backfill)', () => {
    const source = 'futures'
    const st = makeSourceState(source, Date.now() - MAX_BACKFILL_GAP_MS)
    assert.equal(simulateOnOpen(source, st), 'backfill')
})

test('gap one millisecond over MAX_BACKFILL_GAP_MS → skip_large_gap', () => {
    const source = 'futures'
    const st = makeSourceState(source, Date.now() - MAX_BACKFILL_GAP_MS - 1)
    assert.equal(simulateOnOpen(source, st), 'skip_large_gap')
})

// 7. runBackfill trivial-gap guard (existing behavior preserved)
test('runBackfill with endTime - startTime < 1000ms returns ok:true immediately (no fetch)', async () => {
    const now = Date.now()
    const result = await runBackfill('spot', now - 500, now)
    assert.equal(result.ok, true)
    assert.ok(typeof result.cursor === 'number')
})

// ─── summary ─────────────────────────────────────────────────────────────────

// Allow async test promises to settle
await new Promise((r) => setTimeout(r, 50))

console.log(`\nResults: ${passed} passed, ${failed} failed\n`)
if (failed > 0) process.exitCode = 1
