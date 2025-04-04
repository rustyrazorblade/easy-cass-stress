package com.rustyrazorblade.easycassstress.integration

import com.rustyrazorblade.easycassstress.Plugin
import com.rustyrazorblade.easycassstress.commands.Run
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
class AllPluginsBasicTest : CassandraTestBase() {
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

    @BeforeEach
    fun setupTest() {
        cleanupKeyspace()
        run = Run("placeholder")
    }

    /**
     * This test is configured to run against a local instance
     * using the datacenter name from our base class.
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
            dc = localDc // Use the datacenter from the base class
        }.execute()
    }
}
