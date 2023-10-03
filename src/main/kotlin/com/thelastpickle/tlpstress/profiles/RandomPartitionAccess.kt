package com.thelastpickle.tlpstress.profiles

import com.datastax.driver.core.PreparedStatement
import com.datastax.driver.core.Session
import com.thelastpickle.tlpstress.PartitionKey
import com.thelastpickle.tlpstress.StressContext
import com.thelastpickle.tlpstress.WorkloadParameter
import com.thelastpickle.tlpstress.generators.Field
import com.thelastpickle.tlpstress.generators.FieldFactory
import com.thelastpickle.tlpstress.generators.FieldGenerator
import com.thelastpickle.tlpstress.generators.functions.Random
import java.util.concurrent.ThreadLocalRandom

class RandomPartitionAccess : IStressProfile {

    @WorkloadParameter(description = "Number of rows per partition, defaults to 100")
    var rows = 100

    @WorkloadParameter("Select and delete random row or the entire partition.  Acceptable values: row, partition")
    var select = "row"

    @WorkloadParameter("Select and delete random row or the entire partition.  Acceptable values: row, partition")
    var delete = "row"

    lateinit var insert_query : PreparedStatement
    lateinit var select_query : PreparedStatement
    lateinit var delete_query : PreparedStatement

    override fun prepare(session: Session) {
        insert_query = session.prepare("INSERT INTO random_access (partition_id, row_id, value) values (?, ?, ?)")

        select_query = when(select) {

            "partition" -> {
                println("Preparing full partition reads")
                session.prepare("SELECT * from random_access WHERE partition_id = ?")
            }
            "row" -> {
                println("Preparing single row reads")
                session.prepare("SELECT * from random_access WHERE partition_id = ? and row_id = ?")
            }
            else ->
                throw RuntimeException("select must be row or partition.")
        }

        delete_query = when(delete) {

            "partition" -> {
                println("Preparing full partition deletes")
                session.prepare("DELETE FROM random_access WHERE partition_id = ?")
            }
            "row" -> {
                println("Preparing single row deletes")
                session.prepare("DELETE FROM random_access WHERE partition_id = ? and row_id = ?")
            }
            else ->
                throw RuntimeException("select must be row or partition.")
        }
    }

    override fun schema(): List<String> {
        return listOf("""CREATE TABLE IF NOT EXISTS random_access (
                           partition_id text,
                           row_id int,
                           value text,
                           primary key (partition_id, row_id)
                        )""")
    }

    override fun getRunner(context: StressContext) : IStressRunner {

        println("Using $rows rows per partition")

        return object : IStressRunner {

            val value = context.registry.getGenerator("random_access", "value")
            val random = ThreadLocalRandom.current()


            override fun getNextMutation(partitionKey: PartitionKey): Operation {
                val rowId = random.nextInt(0, rows)
                val bound = insert_query.bind(partitionKey.getText(),
                    rowId, value.getText())
                return Operation.Mutation(bound)
            }

            override fun getNextSelect(partitionKey: PartitionKey): Operation {
                val bound = when(select) {

                    "partition" ->
                        select_query.bind(partitionKey.getText())
                    "row" -> {
                        val rowId = random.nextInt(0, rows)
                        select_query.bind(partitionKey.getText(), rowId)
                    }
                    else -> throw RuntimeException("not even sure how you got here")

                }
                return Operation.SelectStatement(bound)

            }

            override fun getNextDelete(partitionKey: PartitionKey): Operation {
                val bound = when(delete) {
                    "partition" ->
                        delete_query.bind(partitionKey.getText())
                    "row" -> {
                        val rowId = random.nextInt(0, rows)
                        delete_query.bind(partitionKey.getText(), rowId)
                    }
                    else -> throw RuntimeException("not even sure how you got here")

                }
                return Operation.Deletion(bound)
            }
        }
    }

    override fun getFieldGenerators(): Map<Field, FieldGenerator> {
        val ra = FieldFactory("random_access")
        return mapOf(ra.getField("value") to Random().apply{ min=100; max=200})
    }

}