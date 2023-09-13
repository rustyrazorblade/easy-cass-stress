package com.thelastpickle.tlpstress

import java.util.concurrent.TimeUnit

import com.codahale.metrics.MetricRegistry
import com.codahale.metrics.ScheduledReporter

import io.prometheus.client.CollectorRegistry
import io.prometheus.client.dropwizard.DropwizardExports
import io.prometheus.client.exporter.HTTPServer

import org.HdrHistogram.SynchronizedHistogram

class Metrics(metricRegistry: MetricRegistry, val reporters: List<ScheduledReporter>, val httpPort : Int) {

    val server: HTTPServer


    fun startReporting() {
        for(reporter in reporters)
            reporter.start(3, TimeUnit.SECONDS)
    }

    fun shutdown() {
        server.close()
        
        for(reporter in reporters) {
            reporter.stop()
        }
    }

    init {
        CollectorRegistry.defaultRegistry.register(DropwizardExports(metricRegistry))
        server = HTTPServer(httpPort)

    }


    val errors = metricRegistry.meter("errors")
    val mutations = metricRegistry.timer("mutations")
    val selects = metricRegistry.timer("selects")
    val deletions = metricRegistry.timer("deletions")

    val populate = metricRegistry.timer("populateMutations")

    // Using a synchronized histogram for now, we may need to change this later if it's a perf bottleneck
    val mutationHistogram = SynchronizedHistogram(2)
    val selectHistogram = SynchronizedHistogram(2)
    val deleteHistogram = SynchronizedHistogram(2)



}