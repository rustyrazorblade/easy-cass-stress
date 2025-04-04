package com.rustyrazorblade.easycassstress.workloads

import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.cql.PreparedStatement
import com.rustyrazorblade.easycassstress.PartitionKey
import com.rustyrazorblade.easycassstress.StressContext
import com.rustyrazorblade.easycassstress.WorkloadParameter
import com.rustyrazorblade.easycassstress.generators.Field
import com.rustyrazorblade.easycassstress.generators.FieldFactory
import com.rustyrazorblade.easycassstress.generators.FieldGenerator
import com.rustyrazorblade.easycassstress.generators.functions.Random
import java.util.concurrent.ThreadLocalRandom

class AllowFiltering : IStressProfile {
    @WorkloadParameter(description = "Number of rows per partition")
    var rows = 100

    @WorkloadParameter(description = "Max Value of the value field.  Lower values will return more results.")
    var maxValue = 100

    lateinit var insert: PreparedStatement
    lateinit var select: PreparedStatement
    lateinit var delete: PreparedStatement

    override fun prepare(session: CqlSession) {
        insert = session.prepare("INSERT INTO allow_filtering (partition_id, row_id, value, payload) values (?, ?, ?, ?)")
        select = session.prepare("SELECT * from allow_filtering WHERE partition_id = ? and value = ? ALLOW FILTERING")
        delete = session.prepare("DELETE from allow_filtering WHERE partition_id = ? and row_id = ?")
    }

    override fun schema(): List<String> {
        return listOf(
            """CREATE TABLE IF NOT EXISTS allow_filtering (
            |partition_id text,
            |row_id int,
            |value int,
            |payload text,
            |primary key (partition_id, row_id)
            |) 
            """.trimMargin(),
        )
    }

    override fun getRunner(context: StressContext): IStressRunner {
        val payload = context.registry.getGenerator("allow_filtering", "payload")
        val random = ThreadLocalRandom.current()

        return object : IStressRunner {
            override fun getNextMutation(partitionKey: PartitionKey): Operation {
                val rowId = random.nextInt(0, rows)
                val value = random.nextInt(0, maxValue)

                val bound =
                    insert.bind()
                        .setString(0, partitionKey.getText())
                        .setInt(1, rowId)
                        .setInt(2, value)
                        .setString(3, payload.getText())
                return Operation.Mutation(bound)
            }

            override fun getNextSelect(partitionKey: PartitionKey): Operation {
                val value = random.nextInt(0, maxValue)
                val bound =
                    select.bind()
                        .setString(0, partitionKey.getText())
                        .setInt(1, value)
                return Operation.SelectStatement(bound)
            }

            override fun getNextDelete(partitionKey: PartitionKey): Operation {
                val rowId = random.nextInt(0, rows)
                val bound =
                    delete.bind()
                        .setString(0, partitionKey.getText())
                        .setInt(1, rowId)
                return Operation.Deletion(bound)
            }
        }
    }

    override fun getFieldGenerators(): Map<Field, FieldGenerator> {
        val af = FieldFactory("allow_filtering")
        return mapOf(
            af.getField("payload") to
                Random().apply {
                    min = 0
                    max = 1
                },
        )
    }
}
