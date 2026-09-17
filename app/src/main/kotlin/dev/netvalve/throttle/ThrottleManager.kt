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
import kotlin.math.max
import kotlin.math.min

/**
 * Owns the per-app, per-direction TokenBuckets and exposes the hot-path
 * pacing primitive used by FlowSupervisor.
 *
 * Upload shaping also contains a conservative adaptive controller for
 * variable-rate LTE/5G links.
 *
 * The adaptive controller does NOT generate probe traffic. It observes the
 * real upstream socket write/flush duration. Sustained blocking is treated
 * as a congestion/queue-pressure signal.
 */
class ThrottleManager(
    private val ruleEngine: RuleEngine,
    scope: CoroutineScope,
    private val logger: Logger? = null,
) {
    private val buckets =
        ConcurrentHashMap<Long, TokenBucket>()

    private data class UploadAdaptiveState(
        var rateBytesPerSec: Long,
        var ewmaWriteNanos: Double = 0.0,
        var initialized: Boolean = false,
        var badSamples: Int = 0,
        var goodSamples: Int = 0,
        var lastChangeNanos: Long = 0L,
    )

    /**
     * State is per UID so one noisy app does not change another app's
     * configured upload rate.
     */
    private val adaptiveUpload =
        ConcurrentHashMap<Int, UploadAdaptiveState>()

    init {
        ruleEngine.revision
            .onEach {
                refreshAll()
            }
            .launchIn(scope)
    }

    private fun key(
        uid: Int,
        direction: Direction,
    ): Long =
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
     * Returns the currently effective rate for a UID upload.
     *
     * The user's configured rate remains the hard ceiling.
     * The adaptive controller can only reduce it temporarily.
     */
    private fun effectiveUploadRate(
        uid: Int,
        configuredRate: Long,
    ): Long {
        val cap =
            configuredRate.coerceAtLeast(
                MIN_UPLOAD_RATE_BYTES_PER_SEC,
            )

        val state =
            adaptiveUpload.compute(
                uid,
            ) { _, old ->
                val current =
                    old ?: UploadAdaptiveState(
                        rateBytesPerSec = cap,
                    )

                current.rateBytesPerSec =
                    current.rateBytesPerSec.coerceIn(
                        MIN_UPLOAD_RATE_BYTES_PER_SEC,
                        cap,
                    )

                current
            }

        return state
            ?.rateBytesPerSec
            ?.coerceIn(
                MIN_UPLOAD_RATE_BYTES_PER_SEC,
                cap,
            )
            ?: cap
    }

    /**
     * @return the bucket to use for this flow's direction, or null when
     * that direction is unlimited under the current policy.
     */
    fun bucketFor(
        uid: Int,
        direction: Direction,
    ): TokenBucket? {
        val configuredCap =
            capFor(
                ruleEngine.policyForUid(uid),
                direction,
            ) ?: run {
                if (direction == Direction.UPLOAD) {
                    adaptiveUpload.remove(uid)
                }

                buckets.remove(
                    key(uid, direction),
                )

                return null
            }

        val cap =
            if (direction == Direction.UPLOAD) {
                effectiveUploadRate(
                    uid,
                    configuredCap,
                )
            } else {
                configuredCap
            }

        val k =
            key(
                uid,
                direction,
            )

        val created =
            !buckets.containsKey(k)

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
     * Feed a real upstream upload write measurement into the adaptive
     * controller.
     *
     * This must be called after upstream.write(), which includes flush().
     *
     * The controller is deliberately conservative:
     *
     *  - EWMA write time >= 25 ms -> congestion signal
     *  - EWMA write time <= 8 ms  -> healthy signal
     *  - 8 bad samples            -> reduce by 10%
     *  - 30 good samples          -> increase by 5%
     *  - rate changes no faster than once every 2 seconds
     *
     * This is intended for variable LTE/5G capacity, where a fixed
     * B8/B20/n28/n78 profile would be brittle.
     */
    fun observeUploadWrite(
        uid: Int,
        configuredRateBytesPerSec: Long,
        writeNanos: Long,
    ) {
        if (
            configuredRateBytesPerSec <= 0L ||
            writeNanos <= 0L
        ) {
            return
        }

        val cap =
            configuredRateBytesPerSec.coerceAtLeast(
                MIN_UPLOAD_RATE_BYTES_PER_SEC,
            )

        val now =
            System.nanoTime()

        val state =
            adaptiveUpload.compute(
                uid,
            ) { _, old ->
                val current =
                    old ?: UploadAdaptiveState(
                        rateBytesPerSec = cap,
                    )

                current.rateBytesPerSec =
                    current.rateBytesPerSec.coerceIn(
                        MIN_UPLOAD_RATE_BYTES_PER_SEC,
                        cap,
                    )

                val sample =
                    writeNanos.toDouble()

                if (!current.initialized) {
                    current.ewmaWriteNanos =
                        sample

                    current.initialized =
                        true
                } else {
                    current.ewmaWriteNanos =
                        (
                            current.ewmaWriteNanos *
                                (1.0 - EWMA_ALPHA)
                        ) +
                            (
                                sample *
                                    EWMA_ALPHA
                            )
                }

                when {
                    current.ewmaWriteNanos >=
                        CONGESTION_WRITE_NANOS -> {
                        current.badSamples++
                        current.goodSamples = 0
                    }

                    current.ewmaWriteNanos <=
                        HEALTHY_WRITE_NANOS -> {
                        current.goodSamples++
                        current.badSamples = 0
                    }

                    else -> {
                        current.badSamples = 0
                        current.goodSamples = 0
                    }
                }

                val canChange =
                    now -
                        current.lastChangeNanos >=
                        MIN_CHANGE_INTERVAL_NANOS

                if (
                    canChange &&
                    current.badSamples >=
                        BAD_SAMPLES_TO_DOWNSTEP
                ) {
                    val oldRate =
                        current.rateBytesPerSec

                    current.rateBytesPerSec =
                        max(
                            MIN_UPLOAD_RATE_BYTES_PER_SEC,
                            (
                                oldRate *
                                    DOWNSTEP_NUMERATOR
                            ) /
                                DOWNSTEP_DENOMINATOR,
                        )

                    current.badSamples = 0
                    current.goodSamples = 0
                    current.lastChangeNanos = now

                    logger?.i(
                        LogCategory.THROTTLE,
                        "adaptive upload downstep " +
                            "${oldRate / 125_000L} Mbps -> " +
                            "${current.rateBytesPerSec / 125_000L} Mbps " +
                            "ewmaWrite=${current.ewmaWriteNanos / 1_000_000.0} ms",
                        uid = uid,
                    )
                } else if (
                    canChange &&
                    current.goodSamples >=
                        GOOD_SAMPLES_TO_UPSTEP
                ) {
                    val oldRate =
                        current.rateBytesPerSec

                    current.rateBytesPerSec =
                        min(
                            cap,
                            (
                                oldRate *
                                    UPSTEP_NUMERATOR
                            ) /
                                UPSTEP_DENOMINATOR,
                        )

                    current.goodSamples = 0
                    current.badSamples = 0
                    current.lastChangeNanos = now

                    logger?.i(
                        LogCategory.THROTTLE,
                        "adaptive upload upstep " +
                            "${oldRate / 125_000L} Mbps -> " +
                            "${current.rateBytesPerSec / 125_000L} Mbps " +
                            "ewmaWrite=${current.ewmaWriteNanos / 1_000_000.0} ms",
                        uid = uid,
                    )
                }

                current
            }

        /*
         * Apply the new rate immediately to an already-existing bucket.
         * Otherwise the new rate would only take effect on the next
         * bucketFor() call.
         */
        if (state != null) {
            val effective =
                state.rateBytesPerSec.coerceIn(
                    MIN_UPLOAD_RATE_BYTES_PER_SEC,
                    cap,
                )

            buckets[
                key(
                    uid,
                    Direction.UPLOAD,
                )
            ]?.updateRate(
                effective,
                burstFor(effective),
            )
        }
    }

    /**
     * Suspend until [bytes] of allowance are available.
     *
     * No packet dropping occurs here.
     */
    suspend fun pace(
        bucket: TokenBucket?,
        bytes: Long,
    ) {
        if (
            bucket == null ||
            bytes <= 0
        ) {
            return
        }

        val rawWait =
            bucket.reserveNanos(bytes)

        val waitNanos =
            min(
                rawWait,
                MAX_PACE_NANOS,
            )

        if (
            logger?.isEnabled(
                LogLevel.DEBUG,
            ) == true
        ) {
            logger.d(
                LogCategory.THROTTLE,
                "pace requested=$bytes " +
                    "rate=${bucket.configuredRate} " +
                    "burst=${bucket.configuredBurst} " +
                    "tokens=${bucket.availableTokens()} " +
                    "refill=${bucket.lastRefillAmount} " +
                    "waitNs=$rawWait" +
                    if (
                        rawWait >
                            MAX_PACE_NANOS
                    ) {
                        " (clamped→$waitNanos)"
                    } else {
                        ""
                    },
            )
        }

        if (
            rawWait >
                MAX_PACE_NANOS
        ) {
            logger?.w(
                LogCategory.THROTTLE,
                "pace wait " +
                    "${rawWait / 1_000_000} ms " +
                    "clamped to " +
                    "${MAX_PACE_NANOS / 1_000_000} ms " +
                    "(cap too low for ${bytes}B chunk; " +
                    "throughput will exceed the cap)",
            )
        }

        if (waitNanos > 0) {
            delay(
                (
                    waitNanos +
                        999_999
                ) /
                    1_000_000,
            )
        }
    }

    /**
     * Buckets are shared per UID and direction.
     */
    fun releaseFlow(
        uid: Int,
    ) {
        // Intentionally no-op.
    }

    /**
     * Recompute all live buckets from the current policy.
     */
    private fun refreshAll() {
        buckets.forEach { (k, bucket) ->
            val uid =
                (k ushr 1).toInt()

            val direction =
                if (
                    k and 1L == 1L
                ) {
                    Direction.DOWNLOAD
                } else {
                    Direction.UPLOAD
                }

            val configuredCap =
                capFor(
                    ruleEngine.policyForUid(
                        uid,
                    ),
                    direction,
                )

            if (configuredCap == null) {
                buckets.remove(k)

                if (
                    direction ==
                        Direction.UPLOAD
                ) {
                    adaptiveUpload.remove(uid)
                }
            } else {
                val cap =
                    if (
                        direction ==
                            Direction.UPLOAD
                    ) {
                        effectiveUploadRate(
                            uid,
                            configuredCap,
                        )
                    } else {
                        configuredCap
                    }

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
         * At 18 Mbps this is about 13.5 KiB, capped at 16 KiB.
         * The sustained rate is controlled separately.
         */
        fun burstFor(
            rateBytesPerSec: Long,
        ): Long {
            val bytesForSixMs =
                (
                    rateBytesPerSec
                        .coerceAtLeast(1L) *
                        BURST_TARGET_MILLIS
                ) /
                    1000L

            return min(
                bytesForSixMs,
                RELAY_BURST_MAX_BYTES,
            ).coerceAtLeast(
                RELAY_BURST_MIN_BYTES,
            )
        }

        const val MAX_PACE_NANOS: Long =
            2_000_000_000L

        private const val BURST_TARGET_MILLIS =
            6L

        private const val RELAY_BURST_MIN_BYTES =
            8 * 1024L

        private const val RELAY_BURST_MAX_BYTES =
            16 * 1024L

        private const val MIN_UPLOAD_RATE_BYTES_PER_SEC =
            1_000_000L

        private const val EWMA_ALPHA =
            0.10

        private const val CONGESTION_WRITE_NANOS =
            25_000_000L

        private const val HEALTHY_WRITE_NANOS =
            8_000_000L

        private const val BAD_SAMPLES_TO_DOWNSTEP =
            8

        private const val GOOD_SAMPLES_TO_UPSTEP =
            30

        private const val MIN_CHANGE_INTERVAL_NANOS =
            2_000_000_000L

        private const val DOWNSTEP_NUMERATOR =
            9L

        private const val DOWNSTEP_DENOMINATOR =
            10L

        private const val UPSTEP_NUMERATOR =
            21L

        private const val UPSTEP_DENOMINATOR =
            20L
    }
}
