package com.rustyrazorblade.easycassstress

import com.google.common.util.concurrent.RateLimiter
import org.apache.logging.log4j.kotlin.logger
import java.lang.Math.cbrt
import java.lang.Math.min
import java.util.Optional
import java.util.concurrent.TimeUnit
import kotlin.math.sqrt

/**
 *
 */
class RateLimiterOptimizer(
    val rateLimiter: RateLimiter,
    val metrics: Metrics,
    val maxReadLatency: Long?,
    val maxWriteLatency: Long?,
    var isStepPhase: Boolean = true
) {
    companion object {
        val log = logger()
    }

    val durationFactor = 1.0 / TimeUnit.MILLISECONDS.toNanos(1)

    val initial: Double = rateLimiter.rate
    val stepValue = initial / 10.0

    init {
        println("Stepping rate limiter by $stepValue to $initial")
    }

    /**
     * Updates the rate limiter to use the new value and returns the new rate limit
     */
    fun execute(): Double {
        if (isStepPhase) {
            log.info("Stepping rate limiter by $stepValue")
            val newValue = min(rateLimiter.rate + stepValue, initial)
            if (newValue == initial) {
                log.info("Moving to optimization phase")
                isStepPhase = false
            }
            rateLimiter.rate = newValue
            log.info("New rate limiter value: ${rateLimiter.rate}")
            return rateLimiter.rate
        }

        // determine the latency number that's closest to it's limit
        val result = getCurrentAndMaxLatency().map { pair ->
            val newLimit = getNextValue(rateLimiter.rate, pair.first, pair.second)
            if (newLimit == rateLimiter.rate) {
                log.info("Optimizer has nothing to do")
                newLimit
            } else {
                // don't increase the rate limiter if we're not within 20% of the target
                val currentThroughput = getCurrentTotalThroughput()
                if (newLimit > rateLimiter.rate && currentThroughput < rateLimiter.rate * .9) {
                    log.info(
                        "Not increasing rate limiter, not within 10% of the current limit " +
                            "(current: $currentThroughput vs actual:${rateLimiter.rate})",
                    )
                    rateLimiter.rate
                } else {
                    // if we're decreasing the limit, we want to make sure we don't lower it too quickly,
                    // overwise we oscillate between too high and too low
                    if (newLimit < rateLimiter.rate && currentThroughput < rateLimiter.rate * .9) {
                        log.info(
                            "Not decreasing rate limiter, current throughput is above current limit " +
                                "(current: $currentThroughput vs actual: ${rateLimiter.rate})",
                        )
                    }
                    log.info("Updating rate limiter from ${rateLimiter.rate} to $newLimit")
                    rateLimiter.rate = newLimit
                    rateLimiter.rate
                }
            }
        }.orElse(rateLimiter.rate)
        
        return result
    }

    /**
     * Added to prevent the rate limiter from acting when queries aren't running, generally during populate phase
     */
    fun getTotalOperations(): Long {
        return metrics.mutations.count + metrics.selects.count
    }

    fun getCurrentTotalThroughput(): Double {
        return metrics.mutations.oneMinuteRate +
            metrics.selects.oneMinuteRate +
            metrics.deletions.oneMinuteRate +
            metrics.populate.oneMinuteRate
    }

    /**
     * Returns current, target Pair
     */
    fun getCurrentAndMaxLatency(): Optional<Pair<Double, Long>> {
        if (maxWriteLatency == null && maxReadLatency == null) {
            return Optional.empty()
        }
        // if we're in the populate phase
        if (metrics.mutations.count == 0L &&
            metrics.selects.count == 0L &&
            metrics.deletions.count == 0L &&
            metrics.populate.count > 0L
        ) {
            if (maxWriteLatency != null) {
                log.info("In populate phase, using populate latency")
                return Optional.of(Pair(getPopulateLatency(), maxWriteLatency))
            } else {
                return Optional.empty<Pair<Double, Long>>()
            }
        }
        if (maxReadLatency == null) {
            return Optional.of(Pair(getWriteLatency(), maxWriteLatency!!))
        }
        if (maxWriteLatency == null) {
            return Optional.of(Pair(getReadLatency(), maxReadLatency))
        }

        val rLatP = getReadLatency() / maxReadLatency
        val wLatP = getWriteLatency() / maxWriteLatency

        // if either is over, return the one that's the most % over
        return if (rLatP > wLatP) {
            Optional.of(Pair(getReadLatency(), maxReadLatency))
        } else {
            Optional.of(Pair(getWriteLatency(), maxWriteLatency))
        }
    }

    /**
     * Provide the new value for the rate limiter
     * If we're over, we significantly reduce traffic
     * If we're within 95% of our target we increase by 1 (just to do something)
     * if we're under, we increase by up to 2x
     */
    fun getNextValue(
        currentRateLimiterValue: Double,
        currentLatency: Double,
        maxLatency: Long,
    ): Double {
        // we set our max increase relative to the total latency we can tolerate, at most increasing by 5%
        // small latency requirements (< 10ms) should barely adjust the throughput b/c it's so sensitive
        var maxIncrease = (1.0 + sqrt(maxLatency.toDouble()) / 100.0).coerceAtMost(1.05)

        if (currentLatency > maxLatency) {
            log.info("Current Latency ($currentLatency) over Max Latency ($maxLatency) reducing throughput by 10%")
            return currentRateLimiterValue * .90
        } else if (currentLatency / maxLatency.toDouble() > .90) {
            log.info("Current latency ($currentLatency) within 95% of max ($maxLatency), not adjusting")
            return currentRateLimiterValue
        } else {
            // increase a reasonable amount
            // should provide a very gentle increase when we get close to the right number
            val adjustmentFactor = (1 + cbrt(maxLatency.toDouble() - currentLatency) / maxLatency.toDouble()).coerceAtMost(maxIncrease)
            val newLimit = currentRateLimiterValue * adjustmentFactor
            log.info(
                "Current limiter: $currentRateLimiterValue latency $currentLatency, " +
                    "max: $maxLatency adjustment factor: $adjustmentFactor, new limit: $newLimit",
            )
            return newLimit
        }
    }

    fun getReadLatency() = metrics.selects.snapshot.get99thPercentile() * durationFactor

    fun getWriteLatency() = metrics.mutations.snapshot.get99thPercentile() * durationFactor

    fun getPopulateLatency() = metrics.populate.snapshot.get99thPercentile() * durationFactor

    fun reset() {
        isStepPhase = true
        rateLimiter.rate = stepValue
    }
}
