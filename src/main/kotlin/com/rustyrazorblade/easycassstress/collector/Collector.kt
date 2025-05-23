package com.rustyrazorblade.easycassstress.collector

import com.datastax.oss.driver.api.core.cql.AsyncResultSet
import com.rustyrazorblade.easycassstress.Context
import com.rustyrazorblade.easycassstress.Either
import com.rustyrazorblade.easycassstress.StressContext
import com.rustyrazorblade.easycassstress.workloads.Operation

/**
 * When an operation completes (success or failure) this interface is called showing the state at that moment.  This
 * interface is part of the "hot" path and as such implementations should respect that and should push expensive work
 * outside the thread calling this.
 */
interface Collector {
    fun collect(
        ctx: StressContext,
        op: Operation,
        result: Either<AsyncResultSet, Throwable>,
        startNanos: Long,
        endNanos: Long,
    )

    fun close(context: Context) {
    }
}
