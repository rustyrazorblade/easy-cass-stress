package com.thelastpickle.tlpstress.schedulers

import com.google.common.util.concurrent.RateLimiter
import com.thelastpickle.tlpstress.Metrics
import com.thelastpickle.tlpstress.PartitionKey
import com.thelastpickle.tlpstress.PartitionKeyGenerator
import java.util.NoSuchElementException
import java.util.concurrent.ArrayBlockingQueue
import kotlin.concurrent.thread

/**
 * Generates PartitionKeys at a fixed rate for internal queuing.  Does not suffer from coordination omission.
 */
class FixedRateScheduler(var rate: RateLimiter,
                         var total: Long,
                         private var maxId: Long,
                         queueDepth: Int,
                         var metrics: Metrics,
                         private var partitionKeyGenerator: PartitionKeyGenerator) : QueryScheduler {

    private var queue = ArrayBlockingQueue<PartitionKey>(queueDepth)

    var thread = thread(start = false, name = "Fixed Rate Scheduler") {
        for (pk in partitionKeyGenerator.generateKey(total, maxId)) {
            if (isComplete) {
                break
            }
            try {
                queue.add(pk)
            } catch (e : IllegalStateException) {
                metrics.errors.mark()
            }
            rate.acquire()
        }
        isComplete = true
    }

    private var isComplete = false

    /*
    start a background thread that populates a concurrent queue
     */
    override fun start() {
        thread.start()
    }

    override fun generateKey() = sequence {
        while (!isComplete) {
            try {
                // todo not sure if this is the optimal approach, revisit
                // might want to use poll instead, needs a benchmark
                yield(queue.remove())
            } catch (e : NoSuchElementException) {
                // queue is empty, it's ok
            }
        }
    }

    override fun stop() {
        isComplete = true
    }
}
