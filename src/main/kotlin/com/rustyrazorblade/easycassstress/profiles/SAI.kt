package com.rustyrazorblade.easycassstress.profiles

import com.datastax.driver.core.PreparedStatement
import com.datastax.driver.core.Session
import  com.rustyrazorblade.easycassstress.PartitionKey
import  com.rustyrazorblade.easycassstress.StressContext
import  com.rustyrazorblade.easycassstress.WorkloadParameter
import  com.rustyrazorblade.easycassstress.generators.Field
import  com.rustyrazorblade.easycassstress.generators.FieldGenerator
import  com.rustyrazorblade.easycassstress.generators.functions.Book
import com.rustyrazorblade.easycassstress.generators.functions.LastName
import  com.rustyrazorblade.easycassstress.generators.functions.Random
import org.apache.logging.log4j.kotlin.logger
import java.util.concurrent.Executors
import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.ThreadPoolExecutor

/**
 * Executes a SAI workload with queries restricted to a single partition,
 * which is the primary workload targeted by SAI indexes.
 */

const val TABLE : String = "sai"
const val MIN_VALUE_TEXT_SIZE=1
const val MAX_VALUE_TEXT_SIZE=2

class SAI : IStressProfile {

    @WorkloadParameter(description = "Operator to use for SAI queries, defaults to equality = search.")
    var intCompare = "="

    @WorkloadParameter(description = "Logic operator combining multiple predicates.  Not yet supported." )
    var operator = "AND"

    @WorkloadParameter(description = "Max rows per partition")
    var rows = 10000

    @WorkloadParameter(description = "Enable global queries with true to query the entire cluster.")
    var global = false

    @WorkloadParameter(description = "Fields to index, comma separated")
    var indexFields = "value_int,value_text"

    @WorkloadParameter(description = "Fields to search, comma separated")
    var searchFields = "value_text"

    lateinit var insert: PreparedStatement
    lateinit var select: PreparedStatement
    lateinit var delete: PreparedStatement

    // mutable sets are backed by a LinkedHashSet, so we can preserve order
    lateinit var indexFieldsSet : Set<String>
    lateinit var searchFieldsSet : Set<String>

    val log = logger()
    override fun prepare(session: Session) {
        println("Preparing workload with global=$global")

        indexFieldsSet = indexFields.split("\\s*,\\s*".toRegex()).toSet()
        searchFieldsSet = searchFields.split("\\s*,\\s*".toRegex()).toSet()

        insert = session.prepare("INSERT INTO $TABLE (partition_id, c_id, value_text, value_int) VALUES (?, ?, ?, ?)")
        // todo make the operator configurable with a workload parameter

        val parts = mutableListOf<String>()

        // if we're doing a global query we skip the partition key
        if (!global) {
            parts.add("partition_id = ?")
        }

        if (searchFieldsSet.contains("value_text")) {
            parts.add("value_text = ?")
        }
        if (searchFieldsSet.contains("value_int")) {
            parts.add("value_int $intCompare ?")
        }

        val selectQuery = "SELECT * from $TABLE WHERE " + parts.joinToString(" $operator ")
        println("Preparing $selectQuery")
        select = session.prepare(selectQuery)

        delete = session.prepare("DELETE from $TABLE WHERE partition_id = ? AND c_id = ?")
    }

    override fun schema(): List<String> {
        val result = mutableListOf(
            """
                CREATE TABLE IF NOT EXISTS $TABLE (
                    partition_id text,
                    c_id int,
                    value_text text,
                    value_int int,
                    primary key (partition_id, c_id)
                )
            """.trimIndent()
        )
        if (indexFields.contains("value_text") ) {
            result.add("CREATE INDEX IF NOT EXISTS ON $TABLE (value_text) USING 'sai'")
        }
        if (indexFields.contains("value_int")) {
            result.add("CREATE INDEX IF NOT EXISTS ON $TABLE (value_int) USING 'sai'")
        }
        return result
    }

    override fun getRunner(context: StressContext): IStressRunner {
        return object : IStressRunner {

            val c_id = ThreadLocalRandom.current()
            // use nextRowId
            val nextRowId : Int get() = c_id.nextInt(0, rows)

            // generator for the value field
            val value_text = context.registry.getGenerator(TABLE, "value_text")
            val value_int = context.registry.getGenerator(TABLE, "value_int")

            override fun getNextMutation(partitionKey: PartitionKey): Operation {
                val bound = insert.bind(partitionKey.getText(), nextRowId, value_text.getText(), value_int.getInt())
                return Operation.Mutation(bound)
            }

            override fun getNextSelect(partitionKey: PartitionKey): Operation {
                // first bind the partition key
                val boundValues = mutableListOf<Any>()

                if (!global) {
                    boundValues.add(partitionKey.getText())
                }

                if (searchFieldsSet.contains("value_text")) {
                    boundValues.add(value_text.getText())
                }

                if (searchFieldsSet.contains("value_int")) {
                    boundValues.add(value_int.getInt())
                }

                val boundStatement = select.bind(*boundValues.toTypedArray())
                return Operation.SelectStatement(boundStatement)
            }

            override fun getNextDelete(partitionKey: PartitionKey) =
                Operation.Deletion(delete.bind(partitionKey.getText(), nextRowId))

        }
    }

    override fun getFieldGenerators(): Map<Field, FieldGenerator> {
        return mapOf(Field(TABLE, "value_text") to LastName(),
            Field(TABLE, "value_int") to Random().apply{ min=0; max=10000})
    }
}