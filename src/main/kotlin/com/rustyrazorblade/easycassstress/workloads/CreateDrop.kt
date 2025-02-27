package com.rustyrazorblade.easycassstress.workloads

import com.datastax.driver.core.ResultSet
import com.datastax.driver.core.Session
import com.rustyrazorblade.easycassstress.PartitionKey
import com.rustyrazorblade.easycassstress.StressContext
import com.rustyrazorblade.easycassstress.WorkloadParameter
import org.apache.logging.log4j.kotlin.logger
import java.util.concurrent.atomic.AtomicInteger
import java.util.stream.IntStream
import kotlin.collections.ArrayDeque

/**
 * Creates and drops unique tables.
 * Most useful when paired with another workload, to determine if
 * impact of schema operations on running workloads.
 */
class CreateDrop : IStressProfile {
    @WorkloadParameter("Number of fields in each table.")
    var fields = 1

    @WorkloadParameter("Number of tables to keep active")
    var activeTables = 10

    lateinit var currentTables : ArrayDeque<String>

    var tableCount = AtomicInteger(0)

    var logger = logger()

    override fun prepare(session: Session) {
    }

    override fun schema(): List<String> {
        return listOf()
    }

    override fun getDefaultReadRate() = 0.0


    override fun getRunner(context: StressContext): IStressRunner {
        currentTables = ArrayDeque(activeTables * 2)

        return object : IStressRunner {
            override fun getNextMutation(partitionKey: PartitionKey): Operation {
                // consider removing this to be able to test concurrent schema modifications

                synchronized(this) {
                    if (currentTables.size > activeTables) {
                        val name = currentTables.removeFirst()
                        val drop = "DROP TABLE IF EXISTS $name"
                        return Operation.DDL(statement = drop)
                    } else {
                        var next = tableCount.addAndGet(1)
                        var name = "create_drop_$next"
                        // create `fields` fields in the table
                        var fieldsStr = (0 until fields).map { "f${it} text" }.joinToString(", ")

                        var create = """CREATE TABLE $name ( id text, $fieldsStr, primary key (id) )"""
                        logger.debug(create)
                        currentTables.addLast(name)

                        return Operation.DDL(statement = create)
                    }
                }
            }


            override fun getNextSelect(partitionKey: PartitionKey): Operation {
                TODO("Not yet implemented")
            }

            override fun getNextDelete(partitionKey: PartitionKey): Operation {
                TODO("Not yet implemented")
            }

        }
    }


}