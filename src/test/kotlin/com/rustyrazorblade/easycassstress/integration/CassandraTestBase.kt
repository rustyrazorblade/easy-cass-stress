package com.rustyrazorblade.easycassstress.integration

import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.config.DefaultDriverOption
import com.datastax.oss.driver.api.core.config.DriverConfigLoader
import java.net.InetSocketAddress
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll

/**
 * Base class for Cassandra integration tests
 * Provides common setup logic for connection handling
 */
abstract class CassandraTestBase {
    companion object {
        // Connection parameters with environment variable fallbacks
        val ip = System.getenv("EASY_CASS_STRESS_CASSANDRA_IP") ?: "127.0.0.1"
        val localDc = System.getenv("EASY_CASS_STRESS_DATACENTER") ?: "datacenter1"
        
        // Configure driver with reasonable timeouts for tests
        val configLoader = DriverConfigLoader.programmaticBuilder()
            .withString(DefaultDriverOption.REQUEST_TIMEOUT, "30s")
            .withString(DefaultDriverOption.CONTROL_CONNECTION_TIMEOUT, "10s")
            .withString(DefaultDriverOption.METADATA_SCHEMA_REQUEST_TIMEOUT, "30s")
            .withString(DefaultDriverOption.CONNECTION_INIT_QUERY_TIMEOUT, "30s")
            .withString(DefaultDriverOption.CONNECTION_SET_KEYSPACE_TIMEOUT, "10s")
            .build()
            
        // Shared session for all tests
        val connection = createSession()
        
        // Initialize the session
        private fun createSession(): CqlSession {
            println("Connecting to Cassandra at $ip using datacenter $localDc")
            return CqlSession.builder()
                .addContactPoint(InetSocketAddress(ip, 9042))
                .withLocalDatacenter(localDc)
                .withConfigLoader(configLoader)
                .build()
        }
        
        @JvmStatic
        @BeforeAll
        fun setupClass() {
            // Ensure keyspace doesn't exist before tests
            connection.execute("DROP KEYSPACE IF EXISTS easy_cass_stress")
        }
        
        @JvmStatic
        @AfterAll
        fun teardownClass() {
            // Clean up resources after tests
            connection.close()
        }
    }
    
    // Cleanup before each test case
    fun cleanupKeyspace() {
        connection.execute("DROP KEYSPACE IF EXISTS easy_cass_stress")
    }
}