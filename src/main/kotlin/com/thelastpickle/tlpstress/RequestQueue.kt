package com.thelastpickle.tlpstress

import com.thelastpickle.tlpstress.profiles.IStressRunner
import com.thelastpickle.tlpstress.profiles.Operation
import com.codahale.metrics.Timer
import org.apache.logging.log4j.kotlin.logger
import java.time.LocalDateTime
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.ThreadLocalRandom
import kotlin.concurrent.thread

/**
 * Request adds ability to set a proper rate limiter and addresses
 * the issue of coordinated omission.
 */
class RequestQueue(

    private val partitionKeyGenerator: PartitionKeyGenerator,
    context: StressContext,
    totalValues: Long,
    duration: Long,
    runner: IStressRunner,
    readRate: Double,
    deleteRate: Double,
    populatePhase: Boolean = false,
    ) {

    val queue = ArrayBlockingQueue<Operation>(context.mainArguments.queueDepth.toInt(), true);
    var generatorThread : Thread

    companion object {
        val log = logger()
    }

    init {

        generatorThread = thread(start=false) {
            val desiredEndTime = LocalDateTime.now().plusMinutes(duration)
            var executed = 0L
            log.info("populate=$populatePhase total values: $totalValues, duration: $duration")

            // we're using a separate timer for populate phase
            // regardless of the operation performed
            fun getTimer(operation: Operation) : Timer {
                return if (populatePhase)
                    context.metrics.populate
                else when (operation) {
                    is Operation.SelectStatement -> context.metrics.selects
                    is Operation.Mutation -> context.metrics.mutations
                    is Operation.Deletion -> context.metrics.deletions
                    is Operation.Stop -> throw OperationStopException()
                }
            }

            for (key in partitionKeyGenerator.generateKey(totalValues, context.mainArguments.partitionValues)) {
                if (duration > 0 && desiredEndTime.isBefore(LocalDateTime.now())) {
                    log.info("Reached duration, ending")
                    break
                }

                if (totalValues > 0 && executed == totalValues) {
                    log.info("Reached total values $totalValues")
                    break
                }

                // check if we hit our limit
                // get next thing from the profile
                // thing could be a statement, or it could be a failure command
                // certain profiles will want to deterministically inject failures
                // others can be randomly injected by the runner
                // I should be able to just tell the runner to inject gossip failures in any test
                // without having to write that code in the profile

                val nextOp = ThreadLocalRandom.current().nextInt(0, 100)

                context.rateLimiter?.run {
                    acquire(1)
                }

                // we only do the mutations in non-populate run
                val op = if ( !populatePhase && readRate * 100 > nextOp) {
                    runner.getNextSelect(key)
                } else if ((readRate * 100) + (deleteRate * 100) > nextOp) {
                    // we might be in a populate phase but only if the user specifically requested it
                    runner.getNextDelete(key)
                } else if (populatePhase) {
                    // at this point we're either populating or we're in a normal run
                    runner.getNextPopulate(key)
                } else {
                    runner.getNextMutation(key)
                }



                op.startTime=getTimer(op).time()

                if(!queue.offer(op)) {
                    context.metrics.errors.mark()
                }
                executed++
            }

            // wait for the queue to drain

            log.info("Finished queuing requests, waiting for queue to empty. $executed executed")
            Thread.sleep(1000)
            while (queue.size > 0) {
                Thread.sleep(1000)
            }
            queue.add(Operation.Stop())
        }
    }

    fun getNextOperation() = sequence<Operation> {
        while (generatorThread.isAlive) {
            when (val tmp = queue.take()) {
                is Operation.Stop -> break
                else -> yield(tmp)
            }
        }
    }

    fun start() {
        generatorThread.start()
    }
}