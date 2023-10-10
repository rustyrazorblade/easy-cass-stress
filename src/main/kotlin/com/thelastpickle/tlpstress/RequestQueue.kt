package com.thelastpickle.tlpstress

import com.thelastpickle.tlpstress.profiles.IStressRunner
import com.thelastpickle.tlpstress.profiles.Operation
import java.time.LocalDateTime
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.ThreadLocalRandom
import kotlin.concurrent.thread

/**
 * Need to accept the rate limiter
 */
class RequestQueue(val partitionKeyGenerator: PartitionKeyGenerator,
                   context: StressContext,
                   totalValues: Long,
                   duration: Long,
                   runner: IStressRunner,
                   readRate: Double,
                   deleteRate: Double

    ) {

    val queue = LinkedBlockingQueue<Operation>(context.mainArguments.queueDepth);
    var generatorThread : Thread

    init {
        generatorThread = thread(start=false) {
            val desiredEndTime = LocalDateTime.now().plusMinutes(duration)
            for (key in partitionKeyGenerator.generateKey(totalValues, context.mainArguments.partitionValues)) {
                if (duration > 0 && desiredEndTime.isBefore(LocalDateTime.now())) {
                    break
                }
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

                val op = if (readRate * 100 > nextOp) {
                    runner.getNextSelect(key).apply { startTime=context.metrics.selects.time() }
                } else if ((readRate * 100) + (deleteRate * 100) > nextOp) {

                    runner.getNextDelete(key).apply { startTime=context.metrics.deletions.time() }
                } else {
                    runner.getNextMutation(key).apply { startTime=context.metrics.mutations.time() }
                }

                if(!queue.offer(op)) {
                    context.metrics.errors.mark()
                }

            }
        }
    }

    fun start() {
        generatorThread.start()
    }

    fun add() {

    }

    fun stop() {

    }

}