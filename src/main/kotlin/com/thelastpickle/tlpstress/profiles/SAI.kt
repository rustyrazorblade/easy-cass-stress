package com.thelastpickle.tlpstress.profiles

import com.datastax.driver.core.PreparedStatement
import com.datastax.driver.core.Session
import com.thelastpickle.tlpstress.PartitionKey
import com.thelastpickle.tlpstress.StressContext
import com.thelastpickle.tlpstress.WorkloadParameter
import com.thelastpickle.tlpstress.generators.Field
import com.thelastpickle.tlpstress.generators.FieldGenerator
import com.thelastpickle.tlpstress.generators.functions.Book
import com.thelastpickle.tlpstress.generators.functions.Random
import org.apache.logging.log4j.kotlin.logger
import java.util.concurrent.ThreadLocalRandom

/**
 * Executes a SAI workload with queries restricted to a single partition,
 * which is the primary workload targeted by SAI indexes.
 */

const val TABLE = "sai"
class SAI : IStressProfile {

    // TODO allow for all operators, may be useful for simulating more workloads
    // uncover some bugs.
    @WorkloadParameter(description = "Operator to use for SAI queries, defaults to equality = search.")
    var operator = "="

    @WorkloadParameter(description = "Rows per partition")
    var rows = 10000

    @WorkloadParameter(description = "Enable global queries with true to query the entire cluster.")
    var global = false

    @WorkloadParameter(description = "Min size in words of value field.")
    var minSize = 5

    @WorkloadParameter(description = "Max size in words of value field.")
    var maxSize = 10

    lateinit var insert: PreparedStatement
    lateinit var select: PreparedStatement
    lateinit var delete: PreparedStatement

    val log = logger()
    override fun prepare(session: Session) {
        println("Preparing workload with global=$global")
        insert = session.prepare("INSERT INTO $TABLE (partition_id, c_id, value) VALUES (?, ?, ?)")
        // todo make the operator configurable with a workload parameter

        select = when(global) {
            // global queries are not partition restricted
            true -> session.prepare("SELECT * from $TABLE WHERE value $operator ?")
            false -> session.prepare("SELECT * from $TABLE WHERE partition_id = ? AND value $operator ?")
        }

        delete = session.prepare("DELETE from $TABLE WHERE partition_id = ? AND c_id = ?")
    }

    override fun schema(): List<String> =
        listOf(
            """
                CREATE TABLE IF NOT EXISTS $TABLE (
                    partition_id text,
                    c_id int,
                    value text,
                    primary key (partition_id, c_id)
                )
            """.trimIndent(),
            """
                CREATE INDEX IF NOT EXISTS ON $TABLE (value) USING 'sai';
            """.trimIndent()
        )


    override fun getRunner(context: StressContext): IStressRunner {
        return object : IStressRunner {

            val nextRow : Int get() = c_id.nextInt(0, rows)

            val c_id = ThreadLocalRandom.current()
            // generator for the value field
            val value = context.registry.getGenerator(TABLE, "value")

            override fun getNextMutation(partitionKey: PartitionKey): Operation {
                val bound = insert.bind(partitionKey.getText(), nextRow, value.getText())
                return Operation.Mutation(bound)
            }

            override fun getNextSelect(partitionKey: PartitionKey): Operation {
                val bound = when (global) {
                    true -> select.bind(value.getText())
                    false -> select.bind(partitionKey.getText(), value.getText())
                }
                return Operation.SelectStatement(bound)
            }

            override fun getNextDelete(partitionKey: PartitionKey) =
                Operation.Deletion(delete.bind(partitionKey.getText(), nextRow))

        }
    }

    override fun getFieldGenerators(): Map<Field, FieldGenerator> {
        return mapOf(Field(TABLE, "value") to Book().apply{ min=minSize; max=maxSize})
    }
}