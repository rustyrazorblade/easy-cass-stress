package com.rustyrazorblade.easycassstress.workloads

import com.datastax.driver.core.PreparedStatement
import com.datastax.driver.core.Session
import com.rustyrazorblade.easycassstress.PartitionKey
import com.rustyrazorblade.easycassstress.StressContext
import com.rustyrazorblade.easycassstress.WorkloadParameter
import org.apache.logging.log4j.kotlin.logger
import java.util.Timer
import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicInteger
import kotlin.collections.ArrayDeque
import kotlin.concurrent.schedule

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

    @WorkloadParameter("Seconds between DDL operations")
    var seconds = 60.0

    var logger = logger()

    override fun prepare(session: Session) {
    }

    override fun schema(): List<String> {
        return listOf()
    }

    override fun getDefaultReadRate() = 0.0


    override fun getRunner(context: StressContext): IStressRunner {
        /**
         * We're going to be managing a bunch of tables here, and each one needs it's own prepared statements
         */
        data class Table(val name: String,
                         var insert: PreparedStatement,
                         var select: PreparedStatement,
                         var delete: PreparedStatement)

        var tableCount = AtomicInteger(0)

        var currentTables = ArrayDeque<Table>(activeTables * 2)

        // query fragments
        var fieldList = (0 until fields).map { "f${it}" }
        var createFieldsWithType = fieldList.map { "$it text" }.joinToString(",")
        val fieldStr = fieldList.joinToString(", " )
        val placeholders = (0 until fields).map { "?" }.joinToString(", ")

        // whether or not to execute a DDL statement in the next iteration
        val timer = Timer()
        var latch = CountDownLatch(1)

        logger.info("Scheduling DDL every $seconds seconds")
        timer.schedule(0L,
            seconds.toLong() * 1000) {

            if (currentTables.size < activeTables) {
                val next = tableCount.addAndGet(1)
                val name = "create_drop_${context.thread}_${context.mainArguments.id}_$next"
                val query = """CREATE TABLE IF NOT EXISTS $name ( id text, $createFieldsWithType, primary key (id) )"""
                context.session.execute(query)

                val insert = "INSERT INTO $name (id, $fieldStr) VALUES (?, $placeholders)"

                val insertPrepared = context.session.prepare(insert)
                val selectPrepared = context.session.prepare("SELECT * from $name WHERE id = ?")
                val deletePrepared = context.session.prepare("DELETE from $name WHERE id = ?")

                currentTables.addLast(
                    Table(name,
                        insert=insertPrepared,
                        select = selectPrepared,
                        delete=deletePrepared))
            }
            else {
                val name = currentTables.removeFirst()
                context.session.execute("DROP TABLE IF EXISTS ${name.name}")
            }
            latch.countDown()

        }
        latch.await()

        return object : IStressRunner {

            override fun getNextMutation(partitionKey: PartitionKey): Operation {
                // consider removing this to be able to test concurrent schema modifications
                val table = currentTables.last()

                val boundValues = mutableListOf(partitionKey.getText())

                // placeholder for now
                for (i in 0 until fields) {
                    boundValues.add("test")
                }

                return Operation.Mutation(table.insert.bind(*boundValues.toTypedArray()))

            }

            override fun getNextSelect(partitionKey: PartitionKey): Operation {
                return Operation.SelectStatement(currentTables.last().select.bind(partitionKey.getText()))
            }

            override fun getNextDelete(partitionKey: PartitionKey): Operation {
                return Operation.Deletion(currentTables.last().delete.bind(partitionKey.getText()))
            }

        }
    }


}