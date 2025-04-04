package com.rustyrazorblade.easycassstress.workloads

import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.cql.PreparedStatement
import com.rustyrazorblade.easycassstress.PartitionKey
import com.rustyrazorblade.easycassstress.PartitionKeyGenerator
import com.rustyrazorblade.easycassstress.PopulateOption
import com.rustyrazorblade.easycassstress.StressContext
import com.rustyrazorblade.easycassstress.commands.Run
import org.apache.logging.log4j.kotlin.logger
import java.util.Optional
import java.util.concurrent.ConcurrentHashMap

/**
 * Note: currently broken :(
 * Warning: this workload is under development and should not be used as a reference across multiple easy-cass-stress runs with
 * different versions of easy-cass-stress as the implementation may change!
 *
 * Load test for a case where we have a dataset that requires LWT for a status update type workload
 * This could be a lock on status or a state machine in the real world
 *
 * For this test, we'll use the following states
 *
 * 0: normal
 * 1: temporarily locked
 */
class Locking : IStressProfile {
    lateinit var insert: PreparedStatement
    lateinit var update: PreparedStatement
    lateinit var select: PreparedStatement
    lateinit var delete: PreparedStatement

    var log = logger()

    override fun prepare(session: CqlSession) {
        insert = session.prepare("INSERT INTO lwtupdates (item_id, name, status) VALUES (?, ?, 0)")
        update = session.prepare("UPDATE lwtupdates set status = ? WHERE item_id = ? IF status = ?")
        select = session.prepare("SELECT * from lwtupdates where item_id = ?")
        delete = session.prepare("DELETE from lwtupdates where item_id = ? IF EXISTS")
    }

    override fun schema(): List<String> {
        val query =
            """
            CREATE TABLE IF NOT EXISTS lwtupdates (
                item_id text primary key,
                name text,
                status int
            )
            """.trimIndent()
        return listOf(query)
    }

    override fun getPopulateOption(args: Run): PopulateOption = PopulateOption.Custom(args.partitionValues, deletes = false)

    override fun getPopulatePartitionKeyGenerator(): Optional<PartitionKeyGenerator> {
        return Optional.of(PartitionKeyGenerator.sequence("test"))
    }

    override fun getRunner(context: StressContext): IStressRunner {
        return object : IStressRunner {
            // this test can't do more than 2 billion partition keys

            val state: ConcurrentHashMap<String, Int> = ConcurrentHashMap(context.mainArguments.partitionValues.toInt())

            override fun getNextMutation(partitionKey: PartitionKey): Operation {
                val currentState = state.getOrDefault(partitionKey.getText(), 0)

                val newState =
                    when (currentState) {
                        0 -> 1
                        else -> 0
                    }

                log.trace { "Updating ${partitionKey.getText()} to $newState" }

                val bound =
                    update.bind()
                        .setInt(0, newState)
                        .setString(1, partitionKey.getText())
                        .setInt(2, newState)
                state[partitionKey.getText()] = newState
                return Operation.Mutation(bound)
            }

            override fun getNextSelect(partitionKey: PartitionKey): Operation {
                val bound =
                    select.bind()
                        .setString(0, partitionKey.getText())
                return Operation.SelectStatement(bound)
            }

            override fun getNextDelete(partitionKey: PartitionKey): Operation {
                val bound =
                    delete.bind()
                        .setString(0, partitionKey.getText())
                return Operation.Deletion(bound)
            }

            override fun getNextPopulate(partitionKey: PartitionKey): Operation {
                val bound =
                    insert.bind()
                        .setString(0, partitionKey.getText())
                        .setString(1, "test")
                return Operation.Mutation(bound)
            }
        }
    }
}
