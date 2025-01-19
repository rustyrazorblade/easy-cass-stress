package com.rustyrazorblade.easycassstress

import com.google.common.util.concurrent.RateLimiter
import io.mockk.every
import io.mockk.junit5.MockKExtension
import io.mockk.mockk
import io.mockk.spyk
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import java.util.Optional

@ExtendWith(MockKExtension::class)
class RateLimiterOptimizerTest {
    var rateLimiter: RateLimiter = RateLimiter.create(1000.0)
    val metrics = mockk<Metrics>()

    fun pair(
        current: Double,
        max: Long,
    ): Optional<Pair<Double, Long>> {
        return Optional.of(Pair(current, max))
    }

    @Test
    fun testSimpleReadLimitRaise() {
        val optimizer = spyk(RateLimiterOptimizer(rateLimiter, metrics, 100, 100, isStepPhase = false))
        every { optimizer.getCurrentAndMaxLatency() } returns pair(10.0, 50)
        every { optimizer.getTotalOperations() } returns 100
        every { optimizer.getCurrentTotalThroughput() } returns 1000.0

        val newRate = optimizer.execute()
        assertThat(newRate).isGreaterThan(1000.0)
    }

    @Test
    fun testSimpleLimitLower() {
        val maxLatency = 100L
        // the original rate limit is 1K, so the test here is that we're over our max latency
        // and we should see the rate limiter be < 1K.
        val optimizer = spyk(RateLimiterOptimizer(rateLimiter, metrics, maxLatency, maxLatency, isStepPhase = false))
        every { optimizer.getCurrentAndMaxLatency() } returns pair(110.0, maxLatency)
        every { optimizer.getTotalOperations() } returns 100
        every { optimizer.getCurrentTotalThroughput() } returns 1000.0

        val newRate = optimizer.execute()
        assertThat(newRate).isLessThan(1000.0)
    }

    // Current limiter: 10.0 latency 1.4934458E7, max: 50 adjustment factor: 2.5109716067365823E-6
    @Test
    fun testLowInitialRate() {
        val maxLatency = 50L
        rateLimiter = RateLimiter.create(10.0)

        val optimizer = spyk(RateLimiterOptimizer(rateLimiter, metrics, maxLatency, maxLatency, isStepPhase = false))
        every { optimizer.getCurrentAndMaxLatency() } returns pair(1.0, maxLatency)
        every { optimizer.getTotalOperations() } returns 100
        every { optimizer.getCurrentTotalThroughput() } returns 10.0

        val newRate = optimizer.execute()
        assertThat(newRate).isGreaterThan(10.0)
    }
}
