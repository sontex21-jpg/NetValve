package dev.netvalve.throttle

import kotlinx.coroutines.channels.Channel

/**
 * Bounded byte-accounted FIFO for paced UDP datagrams.
 *
 * The queue uses a conflated wake-up signal instead of timer polling.
 * When empty, the consumer suspends until a producer offers a packet.
 */
class PacingQueue<T>(
    val capacityBytes: Long,
    val dropPolicy: DropPolicy = DropPolicy.DROP_NEWEST,
) {
    enum class DropPolicy { DROP_NEWEST, DROP_OLDEST }

    private data class Node<T>(val item: T, val size: Long)

    private val deque = ArrayDeque<Node<T>>()
    private var bytesQueued = 0L

    private val availableSignal = Channel<Unit>(Channel.CONFLATED)

    var droppedCount = 0L
        private set

    var droppedBytes = 0L
        private set

    val queuedBytes: Long
        get() = synchronized(this) { bytesQueued }

    val size: Int
        get() = synchronized(this) { deque.size }

    fun offer(item: T, size: Long): Boolean {
        val accepted = synchronized(this) {
            if (size > capacityBytes) {
                droppedCount++
                droppedBytes += size
                return@synchronized false
            }

            when (dropPolicy) {
                DropPolicy.DROP_NEWEST -> {
                    if (bytesQueued + size > capacityBytes) {
                        droppedCount++
                        droppedBytes += size
                        return@synchronized false
                    }
                }

                DropPolicy.DROP_OLDEST -> {
                    while (bytesQueued + size > capacityBytes && deque.isNotEmpty()) {
                        val old = deque.removeFirst()
                        bytesQueued -= old.size
                        droppedCount++
                        droppedBytes += old.size
                    }
                }
            }

            deque.addLast(Node(item, size))
            bytesQueued += size
            true
        }

        if (accepted) {
            availableSignal.trySend(Unit)
        }

        return accepted
    }

    fun poll(): T? = synchronized(this) {
        val node = deque.removeFirstOrNull() ?: return null
        bytesQueued -= node.size
        node.item
    }

    /**
     * Suspends without periodic polling until an item is available.
     */
    suspend fun awaitPoll(): T {
        while (true) {
            poll()?.let { return it }
            availableSignal.receive()
        }
    }

    fun clear() = synchronized(this) {
        deque.clear()
        bytesQueued = 0
    }
}
