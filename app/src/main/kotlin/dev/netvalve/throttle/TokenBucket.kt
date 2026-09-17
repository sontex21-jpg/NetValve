package dev.netvalve.throttle

import kotlin.math.ceil
import kotlin.math.min

/**
 * A lazy-refill token bucket used to shape one direction of one app's traffic.
 *
 * ### Why a token bucket
 * A token bucket meters a sustained rate while tolerating short bursts up to
 * [capacityBytes]. Tokens accrue continuously at [rate][updateRate] bytes/sec
 * and are spent as data passes.
 *
 * Lazy refill means there is no timer or background refill loop. We calculate
 * the accrued allowance only when traffic arrives. This keeps the shaper
 * allocation-light and avoids periodic CPU wakeups.
 *
 * ### Pacing, not dropping
 * Callers do not poll or spin. [reserveNanos] subtracts the requested bytes
 * and returns how long the caller should wait for the resulting debt to be
 * repaid. [ThrottleManager.pace] performs the actual coroutine suspension.
 *
 * Thread-safe: all mutable state is guarded by [lock].
 *
 * @param clock monotonic nanosecond source, injected for deterministic tests.
 */
class TokenBucket(
    capacityBytes: Long,
    rateBytesPerSec: Long,
    private val clock: () -> Long = System::nanoTime,
) {
    private val lock = Any()

    /**
     * Upper bound on accrued allowance (burst size), in bytes.
     */
    private var capacity: Double =
        capacityBytes
            .coerceAtLeast(1)
            .toDouble()

    /**
     * Sustained rate in bytes/sec.
     * <= 0 means unlimited.
     */
    private var rate: Double =
        rateBytesPerSec.toDouble()

    /**
     * Current allowance.
     *
     * A negative value represents reserved-but-not-yet-earned debt.
     */
    private var tokens: Double =
        capacity

    private var lastNanos: Long =
        clock()

    /**
     * Bytes added by the most recent refill.
     * Exposed only for debug instrumentation.
     */
    @Volatile
    private var lastRefillBytes: Double =
        0.0

    val ratePerSecond: Long
        get() =
            synchronized(lock) {
                rate.toLong()
            }

    // ---- Introspection for instrumentation ----

    val configuredRate: Long
        get() =
            synchronized(lock) {
                rate.toLong()
            }

    val configuredBurst: Long
        get() =
            synchronized(lock) {
                capacity.toLong()
            }

    val lastRefillAmount: Long
        get() =
            lastRefillBytes.toLong()

    /**
     * Reserve [bytes] of allowance.
     *
     * @return nanoseconds to wait before sending.
     *         Returns 0 if the allowance was sufficient.
     *         Unlimited buckets always return 0.
     */
    fun reserveNanos(
        bytes: Long,
    ): Long =
        synchronized(lock) {
            if (rate <= 0.0) {
                return 0L
            }

            refillLocked()

            tokens -= bytes

            if (tokens >= 0.0) {
                return 0L
            }

            val deficit =
                -tokens

            /*
             * seconds = deficit / rate
             * nanos   = seconds * 1,000,000,000
             *
             * Round up so the caller never sends early because of fractional
             * nanoseconds.
             */
            return ceil(
                deficit /
                    rate *
                    1_000_000_000.0,
            )
                .toLong()
                .coerceAtLeast(0L)
        }

    /**
     * Change the sustained rate and optionally its burst capacity.
     *
     * Existing debt/allowance is preserved. Only the positive balance is
     * clamped to the new capacity ceiling.
     */
    fun updateRate(
        newRateBytesPerSec: Long,
        newCapacityBytes: Long = -1,
    ) =
        synchronized(lock) {
            refillLocked()

            rate =
                newRateBytesPerSec.toDouble()

            if (newCapacityBytes > 0) {
                capacity =
                    newCapacityBytes.toDouble()

                if (tokens > capacity) {
                    tokens = capacity
                }
            }
        }

    /**
     * Returns current token balance.
     * Intended for tests and instrumentation.
     */
    fun availableTokens(): Long =
        synchronized(lock) {
            refillLocked()
            tokens.toLong()
        }

    private fun refillLocked() {
        val now =
            clock()

        val dt =
            now - lastNanos

        if (dt <= 0) {
            lastRefillBytes = 0.0
            return
        }

        lastNanos =
            now

        if (rate <= 0.0) {
            lastRefillBytes = 0.0
            return
        }

        val accrued =
            dt /
                1_000_000_000.0 *
                rate

        val before =
            tokens

        tokens =
            min(
                capacity,
                tokens + accrued,
            )

        lastRefillBytes =
            tokens - before
    }
}
