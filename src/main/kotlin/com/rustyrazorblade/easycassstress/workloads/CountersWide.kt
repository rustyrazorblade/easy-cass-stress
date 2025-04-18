package com.rustyrazorblade.easycassstress.workloads

import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.cql.PreparedStatement
import com.rustyrazorblade.easycassstress.PartitionKey
import com.rustyrazorblade.easycassstress.StressContext
import com.rustyrazorblade.easycassstress.WorkloadParameter
import java.util.concurrent.ThreadLocalRandom
import kotlin.math.roundToLong

class CountersWide : IStressProfile {
    lateinit var increment: PreparedStatement
    lateinit var selectOne: PreparedStatement
    lateinit var selectAll: PreparedStatement
    lateinit var deleteOne: PreparedStatement

    @WorkloadParameter("Total rows per partition.")
    var rowsPerPartition = 10000

    override fun prepare(session: CqlSession) {
        increment = session.prepare("UPDATE counter_wide SET value = value + 1 WHERE key = ? and cluster = ?")
        selectOne = session.prepare("SELECT * from counter_wide WHERE key = ? AND cluster = ?")
        selectAll = session.prepare("SELECT * from counter_wide WHERE key = ?")
        deleteOne = session.prepare("DELETE from counter_wide WHERE key = ? AND cluster = ?")
    }

    override fun schema(): List<String> {
        return listOf(
            """CREATE TABLE IF NOT EXISTS counter_wide (
            | key text,
            | cluster bigint,
            | value counter,
            | primary key(key, cluster))
            """.trimMargin(),
        )
    }

    override fun getRunner(context: StressContext): IStressRunner {
        // for now i'm just going to hardcode this at 10K items
        // later when a profile can accept dynamic parameters i'll make it configurable

        return object : IStressRunner {
            var iterations = 0L

            override fun getNextMutation(partitionKey: PartitionKey): Operation {
                val clusteringKey = (ThreadLocalRandom.current().nextGaussian() * rowsPerPartition.toDouble()).roundToLong()
                val tmp =
                    increment.bind()
                        .setString(0, partitionKey.getText())
                        .setLong(1, clusteringKey)
                return Operation.Mutation(tmp)
            }

            override fun getNextSelect(partitionKey: PartitionKey): Operation {
                iterations++

                if (iterations % 2 == 0L) {
                    val clusteringKey = (ThreadLocalRandom.current().nextGaussian() * rowsPerPartition.toDouble()).roundToLong()
                    return Operation.SelectStatement(
                        selectOne.bind()
                            .setString(0, partitionKey.getText())
                            .setLong(1, clusteringKey),
                    )
                }

                return Operation.SelectStatement(
                    selectAll.bind()
                        .setString(0, partitionKey.getText()),
                )
            }

            override fun getNextDelete(partitionKey: PartitionKey): Operation {
                val clusteringKey = (ThreadLocalRandom.current().nextGaussian() * rowsPerPartition.toDouble()).roundToLong()
                return Operation.Deletion(
                    deleteOne.bind()
                        .setString(0, partitionKey.getText())
                        .setLong(1, clusteringKey),
                )
            }
        }
    }
}
