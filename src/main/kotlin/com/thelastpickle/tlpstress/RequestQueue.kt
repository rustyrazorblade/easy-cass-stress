package com.thelastpickle.tlpstress

import com.thelastpickle.tlpstress.profiles.IStressRunner
import com.thelastpickle.tlpstress.profiles.Operation
import com.codahale.metrics.Timer
import java.lang.Exception
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
    totalValues: Long, // total number of operations
    duration: Long,
    runner: IStressRunner,
    readRate: Double,
    deleteRate: Double,
    populatePhase: Boolean = false

    ) {

    val queue = ArrayBlockingQueue<Operation>(context.mainArguments.queueDepth.toInt(), true);
    var generatorThread : Thread

    init {

        generatorThread = thread(start=false) {
            val desiredEndTime = LocalDateTime.now().plusMinutes(duration)
            var executed = 0L
            for (key in partitionKeyGenerator.generateKey(totalValues, context.mainArguments.partitionValues)) {
                if (duration > 0 && desiredEndTime.isBefore(LocalDateTime.now())) {
                    break
                }

                if (executed == totalValues) {
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

                // we only do the mutations in non-populate run
                val op = if ( !populatePhase && readRate * 100 > nextOp) {
                    runner.getNextSelect(key).apply { startTime=getTimer(this).time() }
                } else if ((readRate * 100) + (deleteRate * 100) > nextOp) {
                    runner.getNextDelete(key).apply { startTime=getTimer(this).time()}
                } else {
                    runner.getNextMutation(key).apply { startTime=getTimer(this).time()}
                }

                if(!queue.offer(op)) {
                    context.metrics.errors.mark()
                }
                executed++
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

    fun stop() {
        generatorThread.interrupt()
        generatorThread.stop()
    }

}