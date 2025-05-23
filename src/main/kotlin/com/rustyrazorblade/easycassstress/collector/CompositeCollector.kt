package com.rustyrazorblade.easycassstress.collector

import com.datastax.oss.driver.api.core.cql.AsyncResultSet
import com.rustyrazorblade.easycassstress.Context
import com.rustyrazorblade.easycassstress.Either
import com.rustyrazorblade.easycassstress.StressContext
import com.rustyrazorblade.easycassstress.workloads.Operation

class CompositeCollector(private vararg val collectors: Collector) : Collector {
    override fun collect(
        ctx: StressContext,
        op: Operation,
        result: Either<AsyncResultSet, Throwable>,
        startTimeMs: Long,
        durationNs: Long,
    ) {
        for (c in collectors)
            c.collect(ctx, op, result, startTimeMs, durationNs)
    }

    override fun close(context: Context) {
        for (c in collectors)
            c.close(context)
    }
}
