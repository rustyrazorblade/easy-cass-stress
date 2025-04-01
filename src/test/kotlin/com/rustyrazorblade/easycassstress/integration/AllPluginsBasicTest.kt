package com.rustyrazorblade.easycassstress.integration

import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.config.DefaultDriverOption
import com.datastax.oss.driver.api.core.config.DriverConfigLoader
import com.rustyrazorblade.easycassstress.Plugin
import com.rustyrazorblade.easycassstress.commands.Run
import java.net.InetSocketAddress
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource

@Retention(AnnotationRetention.RUNTIME)
@MethodSource("getPlugins")
annotation class AllPlugins

/**
 * This test grabs every plugin and ensures it can run against localhost
 * Next step is to start up a docker container with Cassandra
 * Baby steps.
 */
class AllPluginsBasicTest {
    val ip = System.getenv("EASY_CASS_STRESS_CASSANDRA_IP") ?: "127.0.0.1"

    var configLoaderBuilder = DriverConfigLoader.programmaticBuilder()
        .withString(DefaultDriverOption.REQUEST_TIMEOUT, "30s")
        .withString(DefaultDriverOption.CONTROL_CONNECTION_TIMEOUT, "10s")
        .withString(DefaultDriverOption.METADATA_SCHEMA_REQUEST_TIMEOUT, "30s")

    private val connection =
        CqlSession.builder()
        .addContactPoint(InetSocketAddress(ip, 9042))
        .withLocalDatacenter("datacenter1")
        .withConfigLoader(configLoaderBuilder.build())
        .build()

    lateinit var run: Run

    var prometheusPort = 9600

    /**
     * Annotate a test with @AllPlugins
     */
    companion object {
        @JvmStatic
        fun getPlugins() =
            Plugin.getPlugins().values.filter {
                it.name != "Demo"
            }
    }

    init {
        println("Running tests against $ip")
    }

    @BeforeEach
    fun cleanup() {
        connection.execute("DROP KEYSPACE IF EXISTS easy_cass_stress")
        run = Run("placeholder")
    }

    @AfterEach
    fun shutdownMetrics() {
    }

    /**
     * This test is configured to run against a local instance on a laptop, using the default DC name "datacenter1"
     * To run against a real cluster, we'd need to un-hardcode the test.
     */
    @AllPlugins
    @ParameterizedTest(name = "run test {0}")
    fun runEachTest(plugin: Plugin) {
        run.apply {
            host = ip
            profile = plugin.name
            iterations = 1000
            rate = 100L
            partitionValues = 1000
            prometheusPort = prometheusPort++
            threads = 2
            replication = "{'class': 'SimpleStrategy', 'replication_factor':1 }"
            dc = "datacenter1"
        }.execute()
    }
}
