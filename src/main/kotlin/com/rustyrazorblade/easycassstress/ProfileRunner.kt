package com.rustyrazorblade.easycassstress

import com.datastax.oss.driver.api.core.cql.Statement
import com.datastax.oss.driver.api.core.cql.SimpleStatement
import com.datastax.oss.driver.api.core.cql.AsyncResultSet
import com.rustyrazorblade.easycassstress.workloads.IStressProfile
import com.rustyrazorblade.easycassstress.workloads.Operation
import org.apache.logging.log4j.kotlin.logger
import java.util.concurrent.CompletableFuture
import java.util.function.BiConsumer
import java.time.Duration
import java.time.LocalTime
import java.time.format.DateTimeFormatter
import java.time.format.FormatStyle

class PartitionKeyGeneratorException : Exception()

/**
 * Single threaded profile runner.
 * One profile runner should be created per thread
 * Logs all errors along the way
 * Keeps track of useful metrics, per thread
 */
class ProfileRunner(
    val context: StressContext,
    val profile: IStressProfile,
    val partitionKeyGenerator: PartitionKeyGenerator,
) {
    companion object {
        fun create(
            context: StressContext,
            profile: IStressProfile,
        ): ProfileRunner {
            val partitionKeyGenerator = getGenerator(context, context.mainArguments.partitionKeyGenerator)

            return ProfileRunner(context, profile, partitionKeyGenerator)
        }

        fun getGenerator(
            context: StressContext,
            name: String,
        ): PartitionKeyGenerator {
            val prefix = context.mainArguments.id + "." + context.thread + "."
            println("Creating generator $name")
            val partitionKeyGenerator =
                when (name) {
                    "normal" -> PartitionKeyGenerator.normal(prefix)
                    "random" -> PartitionKeyGenerator.random(prefix)
                    "sequence" -> PartitionKeyGenerator.sequence(prefix)
                    else -> throw PartitionKeyGeneratorException()
                }
            return partitionKeyGenerator
        }

        val log = logger()
    }

    val readRate: Double

    init {
        val tmp = context.mainArguments.readRate

        if (tmp != null) {
            readRate = tmp
        } else {
            readRate = profile.getDefaultReadRate()
        }

        // check unsupported operations
    }

    val deleteRate: Double

    init {
        val tmp = context.mainArguments.deleteRate

        if (tmp != null) {
            deleteRate = tmp
        } else {
            deleteRate = 0.0
        }
    }

    fun print(message: String) {
        println("[Thread ${context.thread}]: $message")
    }

    /**

     */
    fun run() {
        if (context.mainArguments.duration == 0L) {
            print("Running the profile for ${context.mainArguments.iterations} iterations...")
        } else {
            val startTime = LocalTime.now()
            val endTime = startTime.plus(Duration.ofMinutes(context.mainArguments.duration))
            val formatter = DateTimeFormatter.ofLocalizedTime(FormatStyle.SHORT)

            print(
                "Running the profile for ${context.mainArguments.duration}min (start: ${formatter.format(
                    startTime,
                )} end: ${formatter.format(endTime)})",
            )
        }
        executeOperations(context.mainArguments.iterations, context.mainArguments.duration)
    }

    /**
     * Used for both pre-populating data and for performing the actual runner
     */
    private fun executeOperations(
        iterations: Long,
        duration: Long,
    ) {
        // create a semaphore local to the thread to limit the query concurrency
        val runner = profile.getRunner(context)

        // we use MAX_VALUE since it's essentially infinite if we give a duration
        val totalValues = if (duration > 0) Long.MAX_VALUE else iterations

        // if we have a custom generator for the populate phase we'll use that

        val queue = RequestQueue(partitionKeyGenerator, context, totalValues, duration, runner, readRate, deleteRate)

        queue.start()

        // pull requests off the queue instead of using generateKey
        // move the getNextOperation into the queue thing
        var paginate = context.mainArguments.paginate
        for (op in queue.getNextOperation()) {
            // Start timing before executing the query
            op.startTime = context.metrics.startTimer()
            
            // In driver v4, async execution returns a CompletionStage
            val future = when (op) {
                is Operation.DDL -> {
                    paginate = false
                    // Create a SimpleStatement for DDL operations
                    context.session.executeAsync(SimpleStatement.newInstance(op.statement!!))
                }
                else -> {
                    // Ensure bound statement is not null
                    context.session.executeAsync(op.bound!!)
                }
            }
            
            // Create callback to handle the result
            val callback = OperationCallback(
                context,
                runner,
                op,
                paginate = paginate,
                writeHdr = context.mainArguments.hdrHistogramPrefix != "",
            )
            
            // Use whenComplete for CompletionStage with explicit BiConsumer creation
            // to avoid unchecked cast warning
            future.whenComplete(BiConsumer<AsyncResultSet?, Throwable?> { result, error ->
                callback.accept(result, error)
            })

        }
    }

    /**
     * Prepopulates the database with numRows
     * Mutations only, does not count towards the normal metrics
     * Records all timers in the populateMutations metrics
     * Can (and should) be graphed separately
     */
    fun populate(
        numRows: Long,
        deletes: Boolean = true,
    ) {
        val runner = profile.getRunner(context)

        val populatePartitionKeyGenerator = profile.getPopulatePartitionKeyGenerator().orElse(partitionKeyGenerator)

        val queue =
            RequestQueue(
                populatePartitionKeyGenerator,
                context,
                numRows,
                0,
                runner,
                0.0,
                if (deletes) deleteRate else 0.0,
                populatePhase = true,
            )
        queue.start()

        try {
            for (op in queue.getNextOperation()) {
                // Start timing before executing the query
                op.startTime = context.metrics.startTimer()
                
                // In driver v4, async execution returns a CompletionStage
                val future = context.session.executeAsync(op.bound!!)
                
                // Create callback to handle the result
                val callback = OperationCallback(
                    context,
                    runner,
                    op,
                    paginate = false,
                    writeHdr = context.mainArguments.hdrHistogramPrefix != ""
                )
                
                // Use whenComplete for CompletionStage with explicit BiConsumer creation
                // to avoid unchecked cast warning
                future.whenComplete(BiConsumer<AsyncResultSet?, Throwable?> { result, error ->
                    callback.accept(result, error)
                })
            }
        } catch (_: OperationStopException) {
            log.info("Received Stop signal")
            Thread.sleep(3000)
        } catch (e: Exception) {
            log.warn("Received unknown exception ${e.message}")
            throw e
        }
    }

    fun prepare() {
        profile.prepare(context.session)
    }
}
