package com.rustyrazorblade.easycassstress.integration

import com.rustyrazorblade.easycassstress.commands.Run
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

/**
 * Simple tests for various flags that don't required dedicated testing
 */
class FlagsTest : CassandraTestBase() {
    var keyvalue = Run("placeholder")

    @BeforeEach
    fun resetRunners() {
        cleanupKeyspace()
        keyvalue = Run("placeholder").apply {
            profile = "KeyValue"
            iterations = 100
            host = ip
            dc = localDc
            replication = "{'class': 'SimpleStrategy', 'replication_factor':1 }"
        }
    }

    @Test
    fun csvTest() {
        keyvalue.apply {
            csvFile = "test.csv"
        }.execute()
    }
}
