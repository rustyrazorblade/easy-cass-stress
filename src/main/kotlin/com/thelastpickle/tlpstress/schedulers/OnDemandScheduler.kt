package com.thelastpickle.tlpstress.schedulers

import com.thelastpickle.tlpstress.PartitionKey
import com.thelastpickle.tlpstress.PartitionKeyGenerator

/**
 * Generates PartitionKeys on demand.  Suffers from coordinated omission as the query rate is determined by server
 * availability, but can be useful for determining maximum throughput to a cluster with no regard for latency.
 */
class OnDemandScheduler(var total: Long, var partitionKeyGenerator: PartitionKeyGenerator) : QueryScheduler {
    override fun start() {
        TODO("Not yet implemented")
    }

    override fun generateKey(): Sequence<PartitionKey> {
        TODO("Not yet implemented")
    }
}