package com.thelastpickle.tlpstress.schedulers

import com.thelastpickle.tlpstress.PartitionKey
import com.thelastpickle.tlpstress.PartitionKeyGenerator
import kotlin.math.max

/**
 * Generates PartitionKeys on demand.  Suffers from coordinated omission as the query rate is determined by server
 * availability, but can be useful for determining maximum throughput to a cluster with no regard for latency.
 */
class OnDemandScheduler(private val total: Long,
                        private val maxId: Long,
                        private val partitionKeyGenerator: PartitionKeyGenerator) : QueryScheduler {
    private var isComplete = false
    override fun start() {
    }

    override fun stop() {
        isComplete = true
    }

    override fun generateKey() = sequence {
        for (pk in partitionKeyGenerator.generateKey(total, maxId)) {
            yield(pk)
            if (isComplete) {
                break
            }
        }
    }
}
