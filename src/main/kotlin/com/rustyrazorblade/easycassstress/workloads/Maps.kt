package com.rustyrazorblade.easycassstress.workloads

import com.datastax.oss.driver.api.core.cql.PreparedStatement
import com.datastax.oss.driver.api.core.CqlSession
import com.rustyrazorblade.easycassstress.PartitionKey
import com.rustyrazorblade.easycassstress.StressContext

class Maps : IStressProfile {
    lateinit var insert: PreparedStatement
    lateinit var select: PreparedStatement
    lateinit var delete: PreparedStatement

    override fun prepare(session: CqlSession) {
        insert = session.prepare("UPDATE map_stress SET data[?] = ? WHERE id = ?")
        select = session.prepare("SELECT * from map_stress WHERE id = ?")
        delete = session.prepare("DELETE from map_stress WHERE id = ?")
    }

    override fun schema(): List<String> {
        val query = """ CREATE TABLE IF NOT EXISTS map_stress (id text, data map<text, text>, primary key (id)) """
        return listOf(query)
    }

    override fun getRunner(context: StressContext): IStressRunner {
        return object : IStressRunner {
            override fun getNextMutation(partitionKey: PartitionKey): Operation {
                return Operation.Mutation(
                    insert.bind()
                        .setString(0, "key")
                        .setString(1, "value")
                        .setString(2, partitionKey.getText())
                )
            }

            override fun getNextSelect(partitionKey: PartitionKey): Operation {
                val b = select.bind()
                    .setString(0, partitionKey.getText())
                return Operation.SelectStatement(b)
            }

            override fun getNextDelete(partitionKey: PartitionKey): Operation {
                val b = delete.bind()
                    .setString(0, partitionKey.getText())
                return Operation.Deletion(b)
            }
        }
    }
}
