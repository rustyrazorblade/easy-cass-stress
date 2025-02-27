package com.rustyrazorblade.easycassstress.workloads

import com.datastax.driver.core.PreparedStatement
import com.datastax.driver.core.Session
import com.datastax.driver.core.TokenRange
import com.rustyrazorblade.easycassstress.PartitionKey
import com.rustyrazorblade.easycassstress.StressContext
import com.rustyrazorblade.easycassstress.WorkloadParameter
import org.apache.logging.log4j.kotlin.logger

/**
 * this is a bit of an oddball workout because it doesn't support writes.
 */

class RangeScan : IStressProfile {
    private lateinit var ranges: List<TokenRange>

    @WorkloadParameter("Table to perform full scan against.  Does not support writes of any kind.")
    var table = "system.local"

    @WorkloadParameter(
        "Number of ranges (splits) to subdivide each token range into.  Ignored by default.  " +
            "Default is to scan the entire table without ranges.",
    )
    var splits: Int = 1

    lateinit var select: PreparedStatement

    var logger = logger()

    override fun prepare(session: Session) {
        val rq =
            if (splits > 1) {
                ranges = session.cluster.metadata.tokenRanges.flatMap { it.splitEvenly(splits) }
                val tmp = table.split(".")
                var partitionKeys =
                    session.cluster.metadata.getKeyspace(tmp[0])
                        .getTable(tmp[1])
                        .partitionKey.map { it.name }
                        .joinToString(", ")
                logger.info("Using splits on $partitionKeys")
                " WHERE token($partitionKeys) > ? AND token($partitionKeys) < ?"
            } else {
                logger.info("Not using splits because workload.splits parameter=$splits")
                ""
            }
        val s = "SELECT * from $table $rq"
        logger.info("Preparing range query: $s")

        select = session.prepare(s)
    }

    override fun schema(): List<String> {
        return listOf()
    }

    override fun getDefaultReadRate(): Double {
        return 1.0
    }

    override fun getRunner(context: StressContext): IStressRunner {
        return object : IStressRunner {
            override fun getNextMutation(partitionKey: PartitionKey): Operation {
                // we need the ability to say a workload doesn't support mutations
                TODO("Not yet implemented")
            }

            override fun getNextSelect(partitionKey: PartitionKey): Operation {
                return if (splits > 1) {
                    val tmp = ranges.random()
                    Operation.SelectStatement(select.bind(tmp.start.value, tmp.end.value))
                } else {
                    Operation.SelectStatement(select.bind())
                }
            }

            override fun getNextDelete(partitionKey: PartitionKey): Operation {
                // we need the ability to say a workload doesn't support deletes
                TODO("Not yet implemented")
            }
        }
    }
}
