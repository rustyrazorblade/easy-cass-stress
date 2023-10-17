package com.thelastpickle.tlpstress

import com.google.common.util.concurrent.RateLimiter
import org.apache.logging.log4j.kotlin.logger
import java.util.*
import java.util.concurrent.TimeUnit
import kotlin.math.sqrt

/**
 *
 */
class RateLimiterOptimizer(val rateLimiter: RateLimiter,
                           val metrics: Metrics,
                           val maxReadLatency: Long?,
                           val maxWriteLatency: Long?) {
    companion object {
        val log = logger()
    }
    val durationFactor = 1.0 / TimeUnit.MILLISECONDS.toNanos(1)

    /**
     * Updates the rate limiter to use the new value and returns the new rate limit
     */
    fun execute(): Double {
        // determine the latency number that's closest to it's limit
        getCurrentAndMaxLatency().map {
            val newLimit = getNextValue(rateLimiter.rate, it.first, it.second)
            return@map if (newLimit == rateLimiter.rate) {
                log.info("Optimizer has nothing to do")
                newLimit
            } else {
                log.info("Updating rate limiter from ${rateLimiter.rate} to ${newLimit}")
                rateLimiter.rate = newLimit
                rateLimiter.rate
            }
        }.orElse(
            return rateLimiter.rate
        )
    }

    /**
     * Added to prevent the rate limiter from acting when queries aren't running, generally during populate phase
     */
    fun getTotalOperations() : Long {
        return metrics.mutations.count + metrics.selects.count
    }

    /**
     * Returns current, target Pair
     */
    fun getCurrentAndMaxLatency() : Optional<Pair<Double, Long>> {
        if (maxWriteLatency == null && maxReadLatency == null) {
            return Optional.empty()
        }
        // if we're in the populate phase
        if (metrics.mutations.count == 0L &&
            metrics.selects.count == 0L &&
            metrics.deletions.count == 0L &&
            metrics.populate.count > 0L )
            if (maxWriteLatency != null) {
                log.info("In populate phase, using populate latency")
                return Optional.of(Pair(getPopulateLatency(), maxWriteLatency))
            } else {
                return Optional.empty<Pair<Double, Long>>()
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
        } else Optional.of(Pair(getWriteLatency(), maxWriteLatency))
    }

    /**
     * Provide the new value for the rate limiter
     * If we're over, we significantly reduce traffic
     * If we're within 95% of our target we increase by 1 (just to do something)
     * if we're under, we increase by up to 2x
     */
    fun getNextValue(currentRateLimiterValue: Double, currentLatency: Double, maxLatency: Long): Double {
        // if we're at 99% of max we sit sight
        if (currentLatency > maxLatency) {
            log.info("Current Latency ($currentLatency) over Max Latency ($maxLatency) reducing throughput by 25%")
            return currentRateLimiterValue * .75
        }
        else if (currentLatency / maxLatency.toDouble() > .95) {
            log.info("Current latency ($currentLatency) within 95% of max ($maxLatency), not adjusting")
            return currentRateLimiterValue
        }
        else {
            // increase a reasonable amount
            // should provide a very gentle increase when we get close to the right number
            val adjustmentFactor = (1 + sqrt(maxLatency.toDouble() - currentLatency) / maxLatency.toDouble()).coerceAtMost(1.25)
            val newLimit = currentRateLimiterValue * adjustmentFactor
            log.info("Current limiter: $currentRateLimiterValue latency $currentLatency, max: $maxLatency adjustment factor: $adjustmentFactor, new limit: $newLimit")
            return newLimit
        }
    }

    fun getReadLatency() =
        metrics.selects.snapshot.get99thPercentile() * durationFactor


    fun getWriteLatency() =
        metrics.mutations.snapshot.get99thPercentile() * durationFactor

    fun getPopulateLatency() =
        metrics.populate.snapshot.get99thPercentile() * durationFactor

}