package com.rustyrazorblade.easycassstress.workloads

import com.datastax.oss.driver.api.core.cql.PreparedStatement
import com.datastax.oss.driver.api.core.cql.AsyncResultSet
import com.datastax.oss.driver.api.core.CqlSession
import com.rustyrazorblade.easycassstress.PartitionKey
import com.rustyrazorblade.easycassstress.StressContext

class LWT : IStressProfile {
    lateinit var insert: PreparedStatement
    lateinit var update: PreparedStatement
    lateinit var select: PreparedStatement
    lateinit var delete: PreparedStatement
    lateinit var deletePartition: PreparedStatement

    override fun schema(): List<String> {
        return arrayListOf("""CREATE TABLE IF NOT EXISTS lwt (id text primary key, value int) """)
    }

    override fun prepare(session: CqlSession) {
        insert = session.prepare("INSERT INTO lwt (id, value) VALUES (?, ?) IF NOT EXISTS")
        update = session.prepare("UPDATE lwt SET value = ? WHERE id = ? IF value = ?")
        select = session.prepare("SELECT * from lwt WHERE id = ?")
        delete = session.prepare("DELETE from lwt WHERE id = ? IF value = ?")
        deletePartition = session.prepare("DELETE from lwt WHERE id = ? IF EXISTS")
    }

    override fun getRunner(context: StressContext): IStressRunner {
        data class CallbackPayload(val id: String, val value: Int)

        return object : IStressRunner {
            val state = mutableMapOf<String, Int>()

            override fun getNextMutation(partitionKey: PartitionKey): Operation {
                val currentValue = state[partitionKey.getText()]
                val newValue: Int

                val mutation =
                    if (currentValue != null) {
                        newValue = currentValue + 1
                        update.bind()
                            .setInt(0, newValue)
                            .setString(1, partitionKey.getText())
                            .setInt(2, currentValue)
                    } else {
                        newValue = 0
                        insert.bind()
                            .setString(0, partitionKey.getText())
                            .setInt(1, newValue)
                    }
                val payload = CallbackPayload(partitionKey.getText(), newValue)
                return Operation.Mutation(mutation, payload)
            }

            override fun getNextSelect(partitionKey: PartitionKey): Operation {
                return Operation.SelectStatement(
                    select.bind()
                        .setString(0, partitionKey.getText())
                )
            }

            override fun getNextDelete(partitionKey: PartitionKey): Operation {
                val currentValue = state[partitionKey.getText()]
                
                val deletion =
                    if (currentValue != null) {
                        delete.bind()
                            .setString(0, partitionKey.getText())
                            .setInt(1, currentValue)
                    } else {
                        deletePartition.bind()
                            .setString(0, partitionKey.getText())
                    }
                return Operation.Deletion(deletion)
            }

            override fun onSuccess(
                op: Operation.Mutation,
                result: AsyncResultSet?,
            ) {
                val payload = op.callbackPayload!! as CallbackPayload
                state[payload.id] = payload.value
            }
        }
    }
}
