package com.rustyrazorblade.easycassstress

import com.datastax.oss.driver.api.core.cql.AsyncResultSet
import com.rustyrazorblade.easycassstress.workloads.IStressRunner
import com.rustyrazorblade.easycassstress.workloads.Operation
import org.apache.logging.log4j.kotlin.logger
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
) : BiConsumer<AsyncResultSet?, Throwable?> {
    companion object {
        val log = logger()
    }

    override fun accept(
        result: AsyncResultSet?,
        t: Throwable?,
    ) {
        if (t != null) {
            context.metrics.errors.mark()
            context.metrics.recordError() // Record error to OTel
            log.error { t }
            return
        }

        // Handle pagination in driver v4
        if (paginate && result != null) {
            // Fetch next page - this could be made async but we'll keep it simple for now
            while (result.hasMorePages()) {
                result.fetchNextPage()
            }
        }

        val time = op.startTime.stop()

        // we log to the HDR histogram and do the callback for mutations
        // might extend this to select, but I can't see a reason for it now
        when (op) {
            is Operation.Mutation -> {
                if (writeHdr) {
                    context.metrics.mutationHistogram.recordValue(time)
                }
                context.metrics.recordMutation() // Record mutation to OTel
                runner.onSuccess(op, result)
            }

            is Operation.Deletion -> {
                if (writeHdr) {
                    context.metrics.deleteHistogram.recordValue(time)
                }
                context.metrics.recordDeletion() // Record deletion to OTel
            }

            is Operation.SelectStatement -> {
                if (writeHdr) {
                    context.metrics.selectHistogram.recordValue(time)
                }
                context.metrics.recordSelect() // Record select to OTel
            }
            is Operation.DDL -> {
                if (writeHdr) {
                    context.metrics.mutationHistogram.recordValue(time)
                }
                context.metrics.recordMutation() // Record DDL as mutation to OTel
                runner.onSuccess(op, result)
            }
            is Operation.Stop -> {
                throw OperationStopException()
            }
        }
    }
}
