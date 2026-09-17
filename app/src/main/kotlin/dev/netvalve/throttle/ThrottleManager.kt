package dev.netvalve.throttle

import dev.netvalve.data.model.Direction
import dev.netvalve.log.LogCategory
import dev.netvalve.log.LogLevel
import dev.netvalve.log.Logger
import dev.netvalve.rules.EffectivePolicy
import dev.netvalve.rules.RuleEngine
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.launchIn
import kotlinx.coroutines.flow.onEach
import java.util.concurrent.ConcurrentHashMap
import kotlin.math.min

/**
 * Owns the per-app, per-direction [TokenBucket]s and exposes the single hot-path
 * primitive the relay uses: [pace].
 *
 * Upload/download shaping is implemented with a lazy-refill token bucket.
 *
 * The burst size is intentionally kept small because the underlying link can
 * be a variable-rate LTE/5G cellular connection. A burst measured in bytes
 * represents a much larger latency spike when the cell temporarily delivers
 * lower throughput.
 */
class ThrottleManager(
    private val ruleEngine: RuleEngine,
    scope: CoroutineScope,
    private val logger: Logger? = null,
) {
    private val buckets = ConcurrentHashMap<Long, TokenBucket>()

    init {
        // Refresh live buckets whenever the resolved rule set changes.
        ruleEngine.revision
            .onEach { refreshAll() }
            .launchIn(scope)
    }

    private fun key(uid: Int, direction: Direction): Long =
        (uid.toLong() shl 1) or
            (if (direction == Direction.DOWNLOAD) 1L else 0L)

    private fun capFor(
        policy: EffectivePolicy,
        direction: Direction,
    ): Long? =
        if (direction == Direction.DOWNLOAD) {
            policy.downloadBytesPerSec
        } else {
            policy.uploadBytesPerSec
        }

    /**
     * @return the bucket to use for this flow's [direction], or null if that
     * direction is unlimited under the current policy.
     */
    fun bucketFor(
        uid: Int,
        direction: Direction,
    ): TokenBucket? {
        val cap =
            capFor(
                ruleEngine.policyForUid(uid),
                direction,
            ) ?: run {
                buckets.remove(
                    key(uid, direction),
                )
                return null
            }

        val k = key(uid, direction)
        val created = !buckets.containsKey(k)

        val bucket =
            buckets.getOrPut(k) {
                TokenBucket(
                    capacityBytes = burstFor(cap),
                    rateBytesPerSec = cap,
                )
            }

        bucket.updateRate(
            cap,
            burstFor(cap),
        )

        if (created) {
            logger?.i(
                LogCategory.THROTTLE,
                "bucket created dir=$direction " +
                    "rate=$cap B/s " +
                    "burst=${burstFor(cap)} B",
                uid = uid,
            )
        }

        return bucket
    }

    /**
     * Suspend until [bytes] of allowance are available on [bucket], then return.
     *
     * A null bucket returns immediately (unlimited).
     * Never drops TCP data here.
     *
     * The wait is clamped so an individual large application write cannot
     * create an excessively long suspension.
     */
    suspend fun pace(
        bucket: TokenBucket?,
        bytes: Long,
    ) {
        if (bucket == null || bytes <= 0) {
            return
        }

        val rawWait =
            bucket.reserveNanos(bytes)

        val waitNanos =
            min(
                rawWait,
                MAX_PACE_NANOS,
            )

        if (logger?.isEnabled(LogLevel.DEBUG) == true) {
            logger.d(
                LogCategory.THROTTLE,
                "pace requested=$bytes " +
                    "rate=${bucket.configuredRate} " +
                    "burst=${bucket.configuredBurst} " +
                    "tokens=${bucket.availableTokens()} " +
                    "refill=${bucket.lastRefillAmount} " +
                    "waitNs=$rawWait" +
                    if (rawWait > MAX_PACE_NANOS) {
                        " (clamped→$waitNanos)"
                    } else {
                        ""
                    },
            )
        }

        if (rawWait > MAX_PACE_NANOS) {
            logger?.w(
                LogCategory.THROTTLE,
                "pace wait ${rawWait / 1_000_000} ms clamped to " +
                    "${MAX_PACE_NANOS / 1_000_000} ms " +
                    "(cap too low for ${bytes}B chunk; " +
                    "throughput will exceed the cap)",
            )
        }

        if (waitNanos > 0) {
            // Round up to the next millisecond.
            // This avoids busy waiting and keeps CPU usage low.
            delay(
                (waitNanos + 999_999) / 1_000_000,
            )
        }
    }

    /**
     * Buckets are shared per UID and direction across flows.
     * They remain alive until a policy refresh removes an unlimited bucket.
     */
    fun releaseFlow(uid: Int) {
        // Intentionally no-op.
    }

    /**
     * Recompute the rate of every live bucket from the current policy.
     */
    private fun refreshAll() {
        buckets.forEach { (k, bucket) ->
            val uid =
                (k ushr 1).toInt()

            val direction =
                if (k and 1L == 1L) {
                    Direction.DOWNLOAD
                } else {
                    Direction.UPLOAD
                }

            val cap =
                capFor(
                    ruleEngine.policyForUid(uid),
                    direction,
                )

            if (cap == null) {
                buckets.remove(k)
            } else {
                bucket.updateRate(
                    cap,
                    burstFor(cap),
                )
            }
        }
    }

    companion object {

        /**
         * Cellular latency-oriented burst sizing.
         *
         * The previous implementation allowed up to 64 KiB.
         * On a variable-rate LTE/5G link, the same byte burst can represent
         * very different amounts of airtime when radio conditions change.
         *
         * We therefore target only a few milliseconds of burst and cap it at
         * one relay-sized chunk. At the current 18 Mbps configuration this
         * produces a burst around 13.5 KiB, rather than 64 KiB.
         *
         * This changes only the burst allowance. The sustained configured
         * rate remains unchanged.
         */
        fun burstFor(rateBytesPerSec: Long): Long {
            val bytesForSixMs =
                (
                    rateBytesPerSec
                        .coerceAtLeast(1L)
                    * BURST_TARGET_MILLIS
                ) / 1000L

            return min(
                bytesForSixMs,
                RELAY_BURST_MAX_BYTES,
            ).coerceAtLeast(
                RELAY_BURST_MIN_BYTES,
            )
        }

        /**
         * Upper bound on a single pace sleep.
         */
        const val MAX_PACE_NANOS: Long =
            2_000_000_000L

        private const val BURST_TARGET_MILLIS = 6L

        private const val RELAY_BURST_MIN_BYTES =
            8 * 1024L

        private const val RELAY_BURST_MAX_BYTES =
            16 * 1024L
    }
}
