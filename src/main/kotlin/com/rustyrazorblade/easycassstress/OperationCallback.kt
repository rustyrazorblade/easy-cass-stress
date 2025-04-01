package com.rustyrazorblade.easycassstress

import com.datastax.oss.driver.api.core.cql.ResultSet
import com.datastax.oss.driver.api.core.cql.AsyncResultSet
import com.rustyrazorblade.easycassstress.workloads.IStressRunner
import com.rustyrazorblade.easycassstress.workloads.Operation
import org.apache.logging.log4j.kotlin.logger
import java.util.concurrent.CompletionStage
import java.util.function.BiConsumer

/**
 * Callback after a mutation or select
 * This was moved out of the inline ProfileRunner to make populate mode easier
 * as well as reduce clutter
 */
class OperationCallback(
    val context: StressContext,
    val runner: IStressRunner,
    val op: Operation,
    val paginate: Boolean = false,
    val writeHdr: Boolean = true,
) : BiConsumer<ResultSet?, Throwable?> {
    companion object {
        val log = logger()
    }

    override fun accept(result: ResultSet?, t: Throwable?) {
        if (t != null) {
            context.metrics.errors.mark()
            log.error { t }
            return
        }
        
        if (result == null) {
            return
        }

        // maybe paginate - in driver v4, we need to implement differently
        if (paginate && result is AsyncResultSet) {
            // Paginate through all available pages
            // This would need a new implementation
            // fetchNextPage().toCompletableFuture().get()
        }

        val time = op.startTime.stop()

        // we log to the HDR histogram and do the callback for mutations
        // might extend this to select, but I can't see a reason for it now
        when (op) {
            is Operation.Mutation -> {
                if (writeHdr) {
                    context.metrics.mutationHistogram.recordValue(time)
                }
                runner.onSuccess(op, result)
            }

            is Operation.Deletion -> {
                if (writeHdr) {
                    context.metrics.deleteHistogram.recordValue(time)
                }
            }

            is Operation.SelectStatement -> {
                if (writeHdr) {
                    context.metrics.selectHistogram.recordValue(time)
                }
            }
            is Operation.DDL -> {
                if (writeHdr) {
                    context.metrics.mutationHistogram.recordValue(time)
                }
                runner.onSuccess(op, result)
            }
            is Operation.Stop -> {
                throw OperationStopException()
            }
        }
    }
}
