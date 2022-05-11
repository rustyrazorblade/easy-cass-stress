package com.thelastpickle.tlpstress.schedulers

import com.google.common.util.concurrent.RateLimiter
import com.thelastpickle.tlpstress.Metrics
import com.thelastpickle.tlpstress.PartitionKeyGenerator
import io.mockk.mockk
import org.junit.jupiter.api.Test
import org.assertj.core.api.Assertions.assertThat

internal class FixedRateSchedulerTest {
    @Test
    fun testBasicUsage() {
        val limiter = RateLimiter.create(10.0)
        val metrics = mockk<Metrics>()
        val pkGenerator = PartitionKeyGenerator.normal("test")
        val scheduler = FixedRateScheduler(limiter, 30, 100, 10, metrics, pkGenerator)
        scheduler.start()

        for (x in scheduler.generateKey()) {

        }

        scheduler.stop()
    }

    /**
     * Internally we should be throwing errors
     */
    @Test
    fun testSlowUsage() {
        val limiter = RateLimiter.create(10.0)
        val metrics = mockk<Metrics>()
        val pkGenerator = PartitionKeyGenerator.normal("test")
        val scheduler = FixedRateScheduler(limiter, 30, 100, 10, metrics, pkGenerator)
        scheduler.start()

        for (x in scheduler.generateKey()) {
            Thread.sleep(1000)
        }

        scheduler.stop()

        metrics.errors.count
    }
}