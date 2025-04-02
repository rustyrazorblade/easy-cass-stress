package com.rustyrazorblade.easycassstress

import com.codahale.metrics.MetricRegistry
import com.codahale.metrics.ScheduledReporter
import io.prometheus.client.CollectorRegistry
import io.prometheus.client.dropwizard.DropwizardExports
import io.prometheus.client.exporter.HTTPServer
import org.HdrHistogram.SynchronizedHistogram
import java.util.Optional
import java.util.concurrent.TimeUnit

class Metrics(val metricRegistry: MetricRegistry, val reporters: List<ScheduledReporter>, httpPort: Int) {
    val server: Optional<HTTPServer>

    fun startReporting() {
        for (reporter in reporters)
            reporter.start(3, TimeUnit.SECONDS)
    }

    fun shutdown() {
        server.map { it.close() }

        for (reporter in reporters) {
            reporter.stop()
        }
    }

    fun resetErrors() {
        metricRegistry.remove("errors")
        errors = metricRegistry.meter("errors")
    }

    init {
        server =
            if (httpPort > 0) {
                CollectorRegistry.defaultRegistry.register(DropwizardExports(metricRegistry))
                Optional.of(HTTPServer(httpPort))
            } else {
                println("Not setting up prometheus endpoint.")
                Optional.empty()
            }
    }

    var errors = metricRegistry.meter("errors")
    val mutations = metricRegistry.timer("mutations")
    val selects = metricRegistry.timer("selects")
    val deletions = metricRegistry.timer("deletions")

    val populate = metricRegistry.timer("populateMutations")

    // Throughput trackers for metrics
    val selectThroughputTracker = getTracker { selects.count }.start()
    val mutationThroughputTracker = getTracker { mutations.count }.start()
    val deletionThroughputTracker = getTracker { deletions.count }.start()
    val populateThroughputTracker = getTracker { populate.count }.start()

    // Using a synchronized histogram for now, we may need to change this later if it's a perf bottleneck
    val mutationHistogram = SynchronizedHistogram(2)
    val selectHistogram = SynchronizedHistogram(2)
    val deleteHistogram = SynchronizedHistogram(2)

    /**
     * We track throughput using separate structures than Dropwizard
     */
    fun resetThroughputTrackers() {
        selectThroughputTracker.reset()
        mutationThroughputTracker.reset()
        deletionThroughputTracker.reset()
        populateThroughputTracker.reset()
    }

    fun getTracker(countSupplier: () -> Long): ThroughputTracker {
        return ThroughputTracker(
            windowSize = 10,
            countSupplier = countSupplier,
        )
    }

    fun getSelectThroughput() = selectThroughputTracker.getCurrentThroughput()
    fun getMutationThroughput() = mutationThroughputTracker.getCurrentThroughput()
    fun getDeletionThroughput() = deletionThroughputTracker.getCurrentThroughput()
    fun getPopulateThroughput() = populateThroughputTracker.getCurrentThroughput()
}
