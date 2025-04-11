package com.rustyrazorblade.easycassstress

import com.codahale.metrics.MetricRegistry
import com.codahale.metrics.ScheduledReporter
import io.opentelemetry.api.OpenTelemetry
import io.opentelemetry.api.common.Attributes
import io.opentelemetry.api.metrics.LongCounter
import io.opentelemetry.api.metrics.Meter
import io.opentelemetry.api.metrics.ObservableDoubleGauge
import io.opentelemetry.exporter.otlp.metrics.OtlpGrpcMetricExporter
import io.opentelemetry.sdk.OpenTelemetrySdk
import io.opentelemetry.sdk.metrics.SdkMeterProvider
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader
import io.opentelemetry.sdk.resources.Resource
import io.opentelemetry.semconv.resource.attributes.ResourceAttributes
import io.prometheus.client.CollectorRegistry
import io.prometheus.client.dropwizard.DropwizardExports
import io.prometheus.client.exporter.HTTPServer
import org.HdrHistogram.SynchronizedHistogram
import java.time.Duration
import java.util.Optional
import java.util.concurrent.TimeUnit

class Metrics(
    val metricRegistry: MetricRegistry,
    val reporters: List<ScheduledReporter>,
    httpPort: Int,
    otelEndpoint: String,
) {
    val server: Optional<HTTPServer>
    val openTelemetry: OpenTelemetry
    private var meter: Meter? = null

    // OTel metrics
    private var mutationCounter: LongCounter? = null
    private var selectCounter: LongCounter? = null
    private var deleteCounter: LongCounter? = null
    private var errorCounter: LongCounter? = null
    private var mutationThroughputGauge: ObservableDoubleGauge? = null
    private var selectThroughputGauge: ObservableDoubleGauge? = null
    private var deleteThroughputGauge: ObservableDoubleGauge? = null
    private var populateThroughputGauge: ObservableDoubleGauge? = null

    fun startReporting() {
        for (reporter in reporters)
            reporter.start(3, TimeUnit.SECONDS)
    }

    fun shutdown() {
        server.map { it.close() }

        for (reporter in reporters) {
            reporter.stop()
        }

        if (openTelemetry is OpenTelemetrySdk) {
            (openTelemetry as OpenTelemetrySdk).close()
        }
    }

    fun resetErrors() {
        metricRegistry.remove("errors")
        errors = metricRegistry.meter("errors")
    }

    init {
        // Set up Prometheus HTTP server if needed
        server =
            if (httpPort > 0) {
                CollectorRegistry.defaultRegistry.register(DropwizardExports(metricRegistry))
                Optional.of(HTTPServer(httpPort))
            } else {
                println("Not setting up prometheus endpoint.")
                Optional.empty()
            }

        // Initialize OpenTelemetry
        openTelemetry =
            if (otelEndpoint.isNotEmpty()) {
                val resource =
                    Resource.getDefault()
                        .merge(
                            Resource.create(
                                Attributes.of(
                                    ResourceAttributes.SERVICE_NAME, "easy-cass-stress",
                                    ResourceAttributes.SERVICE_VERSION, "1.0.0",
                                ),
                            ),
                        )

                // Ensure endpoint starts with http:// or https://
                val fullEndpoint =
                    if (otelEndpoint.startsWith("http://") || otelEndpoint.startsWith("https://")) {
                        otelEndpoint
                    } else {
                        "http://$otelEndpoint"
                    }

                val metricExporter =
                    OtlpGrpcMetricExporter.builder()
                        .setEndpoint(fullEndpoint)
                        .build()

                val meterProvider =
                    SdkMeterProvider.builder()
                        .setResource(resource)
                        .registerMetricReader(
                            PeriodicMetricReader.builder(metricExporter)
                                .setInterval(Duration.ofSeconds(5))
                                .build(),
                        )
                        .build()

                val sdk =
                    OpenTelemetrySdk.builder()
                        .setMeterProvider(meterProvider)
                        .build()

                // Create OpenTelemetry meter and metrics
                meter = sdk.getMeter("com.rustyrazorblade.easycassstress")
                setupOtelMetrics()

                println("OpenTelemetry metrics export enabled to endpoint: $fullEndpoint")
                sdk
            } else {
                println("Not setting up OpenTelemetry metrics export (no endpoint configured).")
                OpenTelemetry.noop()
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

    // Record a mutation to both Dropwizard and OTel
    fun recordMutation() {
        mutationCounter?.add(1)
    }

    // Record a selection to both Dropwizard and OTel
    fun recordSelect() {
        selectCounter?.add(1)
    }

    // Record a deletion to both Dropwizard and OTel
    fun recordDeletion() {
        deleteCounter?.add(1)
    }

    // Record an error to both Dropwizard and OTel
    fun recordError() {
        errorCounter?.add(1)
    }

    private fun setupOtelMetrics() {
        val m = meter ?: return

        // Create counters for operation counts
        mutationCounter =
            m.counterBuilder("cassandra.mutations")
                .setDescription("Number of Cassandra mutation operations")
                .setUnit("operations")
                .build()

        selectCounter =
            m.counterBuilder("cassandra.selects")
                .setDescription("Number of Cassandra select operations")
                .setUnit("operations")
                .build()

        deleteCounter =
            m.counterBuilder("cassandra.deletions")
                .setDescription("Number of Cassandra delete operations")
                .setUnit("operations")
                .build()

        errorCounter =
            m.counterBuilder("cassandra.errors")
                .setDescription("Number of Cassandra operation errors")
                .setUnit("errors")
                .build()

        // Set up observable gauges for throughput metrics
        mutationThroughputGauge =
            m.gaugeBuilder("cassandra.mutations.throughput")
                .setDescription("Cassandra mutation operations throughput")
                .setUnit("operations/s")
                .buildWithCallback { result ->
                    result.record(getMutationThroughput().toDouble())
                }

        selectThroughputGauge =
            m.gaugeBuilder("cassandra.selects.throughput")
                .setDescription("Cassandra select operations throughput")
                .setUnit("operations/s")
                .buildWithCallback { result ->
                    result.record(getSelectThroughput().toDouble())
                }

        deleteThroughputGauge =
            m.gaugeBuilder("cassandra.deletions.throughput")
                .setDescription("Cassandra delete operations throughput")
                .setUnit("operations/s")
                .buildWithCallback { result ->
                    result.record(getDeletionThroughput().toDouble())
                }

        populateThroughputGauge =
            m.gaugeBuilder("cassandra.populate.throughput")
                .setDescription("Cassandra populate operations throughput")
                .setUnit("operations/s")
                .buildWithCallback { result ->
                    result.record(getPopulateThroughput().toDouble())
                }
    }
}
