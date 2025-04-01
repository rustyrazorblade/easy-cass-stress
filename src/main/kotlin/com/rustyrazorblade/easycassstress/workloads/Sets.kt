package com.rustyrazorblade.easycassstress.workloads

import com.datastax.oss.driver.api.core.cql.PreparedStatement
import com.datastax.oss.driver.api.core.CqlSession
import com.rustyrazorblade.easycassstress.PartitionKey
import com.rustyrazorblade.easycassstress.StressContext
import com.rustyrazorblade.easycassstress.generators.Field
import com.rustyrazorblade.easycassstress.generators.FieldGenerator
import com.rustyrazorblade.easycassstress.generators.functions.Random

class Sets : IStressProfile {
    lateinit var insert: PreparedStatement
    lateinit var update: PreparedStatement
    lateinit var select: PreparedStatement
    lateinit var deleteElement: PreparedStatement

    override fun prepare(session: CqlSession) {
        insert = session.prepare("INSERT INTO sets (key, values) VALUES (?, ?)")
        update = session.prepare("UPDATE sets SET values = values + ? WHERE key = ?")
        select = session.prepare("SELECT * from sets WHERE key = ?")
        deleteElement = session.prepare("UPDATE sets SET values = values - ? WHERE key = ?")
    }

    override fun schema(): List<String> {
        return listOf(
            """
            CREATE TABLE IF NOT EXISTS sets (
            |key text primary key,
            |values set<text>
            |)
            """.trimMargin(),
        )
    }

    override fun getRunner(context: StressContext): IStressRunner {
        val payload = context.registry.getGenerator("sets", "values")

        return object : IStressRunner {
            override fun getNextMutation(partitionKey: PartitionKey): Operation {
                val value = payload.getText()
                val valueSet = java.util.HashSet<String>()
                valueSet.add(value)
                
                // Create a simple statement for now - we'll use direct binding
                // The driver v4 has different ways of setting collections
                val bound = update.bind()
                bound.set(0, valueSet, java.util.HashSet::class.java)
                bound.setString(1, partitionKey.getText())

                return Operation.Mutation(bound)
            }

            override fun getNextSelect(partitionKey: PartitionKey): Operation {
                val bound = select.bind()
                    .setString(0, partitionKey.getText())
                return Operation.SelectStatement(bound)
            }

            override fun getNextDelete(partitionKey: PartitionKey): Operation {
                val valueSet = java.util.HashSet<String>()
                valueSet.add(partitionKey.getText())
                
                // Create a simple statement for now - we'll use direct binding
                val bound = deleteElement.bind()
                bound.set(0, valueSet, java.util.HashSet::class.java)
                bound.setString(1, partitionKey.getText())
                return Operation.Deletion(bound)
            }
        }
    }

    override fun getFieldGenerators(): Map<Field, FieldGenerator> {
        return mapOf(
            Field("sets", "values") to
                Random().apply {
                    min = 6
                    max = 16
                },
        )
    }
}
