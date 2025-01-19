package com.rustyrazorblade.easycassstress

import com.datastax.driver.core.Session
import com.google.common.util.concurrent.RateLimiter
import com.rustyrazorblade.easycassstress.commands.Run
import com.rustyrazorblade.easycassstress.generators.Registry

data class StressContext(
    val session: Session,
    val mainArguments: Run,
    val thread: Int,
    val metrics: Metrics,
    val registry: Registry,
    val rateLimiter: RateLimiter?,
)
