package com.thelastpickle.tlpstress.schedulers

import com.google.common.util.concurrent.RateLimiter
import com.thelastpickle.tlpstress.Metrics
import com.thelastpickle.tlpstress.PartitionKey
import com.thelastpickle.tlpstress.PartitionKeyGenerator
import java.util.concurrent.ArrayBlockingQueue
import kotlin.concurrent.thread

/**
 * Generates PartitionKeys at a fixed rate for internal queuing.  Does not suffer from coordination omission.
 */
class FixedRateScheduler(var rate: RateLimiter,
                         var total: Long,
                         var queueDepth: Int,
                         var metrics: Metrics,
                         var partitionKeyGenerator: PartitionKeyGenerator) : QueryScheduler {

    var queue = ArrayBlockingQueue<PartitionKey>(queueDepth)
    lateinit var thread : Thread

    override fun start() {
        // start a background thread that populates a concurrent queue
        thread = thread {
            do {

            } while (true)
        }
    }

    override fun generateKey() = sequence {

        yield(PartitionKey("test", 1L))
    }
    // Thread generates
}