package com.rustyrazorblade.easycassstress.integration

import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.config.DefaultDriverOption
import com.datastax.oss.driver.api.core.config.DriverConfigLoader
import com.rustyrazorblade.easycassstress.commands.Run
import java.net.InetSocketAddress
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

/**
 * Simple tests for various flags that don't required dedicated testing
 */
class FlagsTest {
    val ip = System.getenv("EASY_CASS_STRESS_CASSANDRA_IP") ?: "127.0.0.1"
    var configLoaderBuilder = DriverConfigLoader.programmaticBuilder()
        .withString(DefaultDriverOption.REQUEST_TIMEOUT, "30s")
        .withString(DefaultDriverOption.CONTROL_CONNECTION_TIMEOUT, "10s")
        .withString(DefaultDriverOption.METADATA_SCHEMA_REQUEST_TIMEOUT, "30s")

    val connection = 
        CqlSession.builder()
        .addContactPoint(InetSocketAddress(ip, 9042))
        .withLocalDatacenter("datacenter1")
        .withConfigLoader(configLoaderBuilder.build())
        .build()

    var keyvalue = Run("placeholder")

    @BeforeEach
    fun resetRunners() {
        keyvalue =
            keyvalue.apply {
                profile = "KeyValue"
                iterations = 100
            }
    }

    @Test
    fun csvTest() {
        keyvalue.apply {
            csvFile = "test.csv"
        }.execute()
    }
}
