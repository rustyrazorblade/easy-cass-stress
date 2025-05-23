package com.rustyrazorblade.easycassstress.collector

import com.datastax.oss.driver.api.core.cql.AsyncResultSet
import com.rustyrazorblade.easycassstress.Context
import com.rustyrazorblade.easycassstress.Either
import com.rustyrazorblade.easycassstress.StressContext
import com.rustyrazorblade.easycassstress.workloads.Operation

interface Collector {
    fun collect(ctx: StressContext,
                op: Operation,
                result: Either<AsyncResultSet, Throwable>,
                startTimeMs: Long,
                durationNs: Long)


    fun close(context: Context) {

    }
}