package com.thelastpickle.tlpstress

import com.google.common.util.concurrent.RateLimiter
import io.mockk.every
import io.mockk.junit5.MockKExtension
import org.junit.jupiter.api.Test
import io.mockk.mockk
import io.mockk.spyk
import org.assertj.core.api.Assertions.*
import org.junit.jupiter.api.extension.ExtendWith

@ExtendWith(MockKExtension::class)
class RateLimiterOptimizerTest {

    var rateLimiter: RateLimiter = RateLimiter.create(1000.0)
    val metrics = mockk<Metrics>()


    @Test
    fun testSimpleReadLimitRaise() {


        val optimizer = spyk(RateLimiterOptimizer(rateLimiter, metrics, 100, 100))
        every { optimizer.getCurrentAndMaxLatency() } returns Pair(10.0, 50)

        val newRate = optimizer.execute()
        assertThat(newRate).isGreaterThan(1000.0)
    }

    @Test
    fun testSimpleLimitLower() {

        val maxLatency = 100L
        val optimizer = spyk(RateLimiterOptimizer(rateLimiter, metrics, maxLatency, maxLatency))
        every { optimizer.getCurrentAndMaxLatency() } returns Pair(110.0, maxLatency)

        val newRate = optimizer.execute()
        assertThat(newRate).isLessThan(1000.0)
    }

    // Current limiter: 10.0 latency 1.4934458E7, max: 50 adjustment factor: 2.5109716067365823E-6
    @Test
    fun testLowInitialRate() {
        val maxLatency = 50L
        rateLimiter = RateLimiter.create(10.0)

        val optimizer = spyk(RateLimiterOptimizer(rateLimiter, metrics, maxLatency, maxLatency))
        every { optimizer.getCurrentAndMaxLatency() } returns Pair(1.0, maxLatency)

        val newRate = optimizer.execute()
        assertThat(newRate).isGreaterThan(20.0)
    }
}