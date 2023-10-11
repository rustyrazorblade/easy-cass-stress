package com.thelastpickle.tlpstress

import com.google.common.util.concurrent.RateLimiter
import org.apache.logging.log4j.kotlin.logger
import java.util.concurrent.TimeUnit
import kotlin.math.max
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
    val durationFactor = 1.0 / TimeUnit.MILLISECONDS.toNanos(1);

    /**
     * Updates the rate limiter to use the new value and returns the new rate limit
     */
    fun execute(): Double {
        // determine the latency number that's closest to it's limit
        val currentLatency = getCurrentAndMaxLatency()
        val newLimit = getNextValue(rateLimiter.rate, currentLatency.first, currentLatency.second)
        if (newLimit == rateLimiter.rate) {
            log.info("Optimizer has nothing to do")
            return newLimit
        }

        log.info("Updating rate limiter from ${rateLimiter.rate} to ${newLimit}")
        rateLimiter.rate = newLimit
        return rateLimiter.rate
    }

    /**
     * Returns current, target Pair
     */
    fun getCurrentAndMaxLatency() : Pair<Double, Long> {
        if (maxReadLatency == null) {
            return Pair(getWriteLatency(), maxWriteLatency!!)
        }
        if (maxWriteLatency == null) {
            return Pair(getReadLatency(), maxReadLatency)
        }

        val rLatP = getReadLatency() / maxReadLatency
        val wLatP = getWriteLatency() / maxWriteLatency

        // if either is over, return the one that's the most % over
        return if (rLatP > wLatP) {
            Pair(getReadLatency(), maxReadLatency)
        } else Pair(getWriteLatency(), maxWriteLatency)
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



}