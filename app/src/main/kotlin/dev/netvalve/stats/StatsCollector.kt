package dev.netvalve.stats

import dev.netvalve.repository.AppInfoLookup
import dev.netvalve.repository.AppUsageRecord
import dev.netvalve.repository.StatsRepository
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong

/**
 * Thread-safe, low-overhead traffic accounting.
 *
 * Packet-path methods remain lock-free and use atomics.
 *
 * Sampling is intentionally cheap while the service is idle:
 * if no traffic/connection/stat counter changed since the previous sample,
 * the existing snapshot is returned without rebuilding per-app statistics.
 *
 * When traffic or connection statistics change, the normal live/avg/peak
 * calculations and per-app snapshot are performed.
 *
 * Cumulative per-app byte totals are checkpointed to [StatsRepository].
 */
class StatsCollector(
    private val statsRepository: StatsRepository,
    private val appInfo: AppInfoLookup,
    private val clock: () -> Long = System::currentTimeMillis,
) {

    private class Counters {
        val up = AtomicLong(0)
        val down = AtomicLong(0)

        @Volatile
        var lastActiveMillis = 0L

        val upMeter = ThroughputMeter()
        val downMeter = ThroughputMeter()

        @Volatile
        var liveUp = 0L

        @Volatile
        var liveDown = 0L
    }

    private val perUid = ConcurrentHashMap<Int, Counters>()

    private val totalUp = AtomicLong(0)
    private val totalDown = AtomicLong(0)

    private val activeConnections = AtomicInteger(0)
    private val throttledConnections = AtomicLong(0)
    private val blockedConnections = AtomicLong(0)
    private val dnsQueries = AtomicLong(0)
    private val connectLatencySum = AtomicLong(0)
    private val connectLatencyCount = AtomicLong(0)

    @Volatile
    private var sessionStart = 0L

    @Volatile
    private var lastReset = clock()

    private val globalUpMeter = ThroughputMeter()
    private val globalDownMeter = ThroughputMeter()

    private val _snapshot = MutableStateFlow(StatsSnapshot())
    val snapshot: StateFlow<StatsSnapshot> = _snapshot.asStateFlow()

    /*
     * Last values observed by sample().
     *
     * These allow the sampler to return immediately when absolutely nothing
     * relevant changed since the previous sample.
     *
     * This is important because sample() is called periodically even when
     * there is no traffic.
     */
    @Volatile
    private var lastSampleTotalUp = Long.MIN_VALUE

    @Volatile
    private var lastSampleTotalDown = Long.MIN_VALUE

    @Volatile
    private var lastSampleActiveConnections = Int.MIN_VALUE

    @Volatile
    private var lastSampleThrottledConnections = Long.MIN_VALUE

    @Volatile
    private var lastSampleBlockedConnections = Long.MIN_VALUE

    @Volatile
    private var lastSampleDnsQueries = Long.MIN_VALUE

    @Volatile
    private var lastSampleLatencySum = Long.MIN_VALUE

    @Volatile
    private var lastSampleLatencyCount = Long.MIN_VALUE

    // ---- lifecycle ----------------------------------------------------------

    /**
     * Load persisted per-app baselines and mark the session start.
     */
    suspend fun startSession() {
        sessionStart = clock()

        statsRepository.current().forEach { rec ->
            val c = perUid.getOrPut(rec.uid) { Counters() }

            c.up.set(rec.totalUpload)
            c.down.set(rec.totalDownload)
            c.lastActiveMillis = rec.lastActiveMillis

            totalUp.addAndGet(rec.totalUpload)
            totalDown.addAndGet(rec.totalDownload)
        }

        /*
         * Force the first sample after startSession() to build a complete
         * snapshot even if there is no traffic yet.
         */
        invalidateSampleCache()
    }

    // ---- hot path (lock-free) ----------------------------------------------

    fun recordUpload(uid: Int, bytes: Long) {
        if (bytes <= 0) return

        counters(uid).let {
            it.up.addAndGet(bytes)
            it.lastActiveMillis = clock()
        }

        totalUp.addAndGet(bytes)
    }

    fun recordDownload(uid: Int, bytes: Long) {
        if (bytes <= 0) return

        counters(uid).let {
            it.down.addAndGet(bytes)
            it.lastActiveMillis = clock()
        }

        totalDown.addAndGet(bytes)
    }

    fun onFlowOpened(
        uid: Int,
        throttled: Boolean,
        blocked: Boolean,
    ) {
        if (blocked) {
            blockedConnections.incrementAndGet()
            return
        }

        activeConnections.incrementAndGet()

        if (throttled) {
            throttledConnections.incrementAndGet()
        }
    }

    fun onFlowClosed(uid: Int) {
        activeConnections.updateAndGet {
            if (it > 0) it - 1 else 0
        }
    }

    fun onDnsQuery() {
        dnsQueries.incrementAndGet()
    }

    fun recordConnectLatency(millis: Long) {
        if (millis < 0) return

        connectLatencySum.addAndGet(millis)
        connectLatencyCount.incrementAndGet()
    }

    private fun counters(uid: Int): Counters =
        perUid.getOrPut(uid) { Counters() }

    // ---- sampling -----------------------------------------------------------

    /**
     * Build the current statistics snapshot.
     *
     * The sampler can be called periodically (normally ~1 Hz).
     *
     * When every relevant counter is identical to the previous sample,
     * there is nothing new to calculate. In that case we return the existing
     * snapshot immediately instead of:
     *
     *  - iterating over perUid
     *  - creating AppStat objects
     *  - resolving package names
     *  - sorting the app list
     *  - updating MutableStateFlow
     *
     * This substantially reduces idle/background work.
     */
    fun sample(): StatsSnapshot {
        val tUp = totalUp.get()
        val tDown = totalDown.get()

        val active = activeConnections.get()
        val throttled = throttledConnections.get()
        val blocked = blockedConnections.get()
        val dns = dnsQueries.get()
        val latencySum = connectLatencySum.get()
        val latencyCount = connectLatencyCount.get()

        /*
         * Fast idle path.
         *
         * No traffic and no connection/statistics changes means the previous
         * snapshot is still valid. Avoid rebuilding all per-app statistics.
         */
        if (
            tUp == lastSampleTotalUp &&
            tDown == lastSampleTotalDown &&
            active == lastSampleActiveConnections &&
            throttled == lastSampleThrottledConnections &&
            blocked == lastSampleBlockedConnections &&
            dns == lastSampleDnsQueries &&
            latencySum == lastSampleLatencySum &&
            latencyCount == lastSampleLatencyCount
        ) {
            return _snapshot.value
        }

        val liveUp = globalUpMeter.sample(tUp)
        val liveDown = globalDownMeter.sample(tDown)
        val now = clock()

        val apps = perUid.entries
            .map { (uid, c) ->
                val up = c.up.get()
                val down = c.down.get()

                c.liveUp = c.upMeter.sample(up)
                c.liveDown = c.downMeter.sample(down)

                AppStat(
                    uid = uid,
                    packageName = appInfo.packagesForUid(uid).firstOrNull(),
                    uploadBytes = up,
                    downloadBytes = down,
                    liveUploadBps = c.liveUp,
                    liveDownloadBps = c.liveDown,
                    active = now - c.lastActiveMillis < ACTIVE_WINDOW_MILLIS,
                )
            }
            .sortedByDescending {
                it.uploadBytes + it.downloadBytes
            }

        val snap = StatsSnapshot(
            totalUpload = tUp,
            totalDownload = tDown,

            liveUploadBps = liveUp,
            liveDownloadBps = liveDown,

            avgUploadBps = globalUpMeter.average(tUp),
            avgDownloadBps = globalDownMeter.average(tDown),

            peakUploadBps = globalUpMeter.peakBps,
            peakDownloadBps = globalDownMeter.peakBps,

            activeConnections = active,
            throttledConnections = throttled,
            blockedConnections = blocked,
            dnsQueries = dns,

            avgConnectLatencyMillis =
                if (latencyCount > 0) {
                    latencySum / latencyCount
                } else {
                    0
                },

            sessionStartMillis = sessionStart,
            lastResetMillis = lastReset,

            perApp = apps,
        )

        /*
         * Store the values used to create this snapshot.
         */
        lastSampleTotalUp = tUp
        lastSampleTotalDown = tDown
        lastSampleActiveConnections = active
        lastSampleThrottledConnections = throttled
        lastSampleBlockedConnections = blocked
        lastSampleDnsQueries = dns
        lastSampleLatencySum = latencySum
        lastSampleLatencyCount = latencyCount

        _snapshot.value = snap

        return snap
    }

    /**
     * Snapshot the cumulative per-app totals for durable persistence.
     */
    fun checkpoint(): List<AppUsageRecord> =
        perUid.entries.map { (uid, c) ->
            AppUsageRecord(
                uid = uid,
                packageName =
                    appInfo.packagesForUid(uid).firstOrNull().orEmpty(),
                totalUpload = c.up.get(),
                totalDownload = c.down.get(),
                lastActiveMillis = c.lastActiveMillis,
            )
        }

    suspend fun reset() {
        perUid.clear()

        totalUp.set(0)
        totalDown.set(0)

        activeConnections.set(0)
        throttledConnections.set(0)
        blockedConnections.set(0)
        dnsQueries.set(0)

        connectLatencySum.set(0)
        connectLatencyCount.set(0)

        globalUpMeter.reset()
        globalDownMeter.reset()

        lastReset = clock()
        sessionStart = clock()

        statsRepository.reset()

        invalidateSampleCache()

        _snapshot.value = StatsSnapshot(
            sessionStartMillis = sessionStart,
            lastResetMillis = lastReset,
        )
    }

    /**
     * Force the next sample() call to perform a full snapshot calculation.
     */
    private fun invalidateSampleCache() {
        lastSampleTotalUp = Long.MIN_VALUE
        lastSampleTotalDown = Long.MIN_VALUE
        lastSampleActiveConnections = Int.MIN_VALUE
        lastSampleThrottledConnections = Long.MIN_VALUE
        lastSampleBlockedConnections = Long.MIN_VALUE
        lastSampleDnsQueries = Long.MIN_VALUE
        lastSampleLatencySum = Long.MIN_VALUE
        lastSampleLatencyCount = Long.MIN_VALUE
    }

    companion object {
        private const val ACTIVE_WINDOW_MILLIS = 5_000L
    }
}
