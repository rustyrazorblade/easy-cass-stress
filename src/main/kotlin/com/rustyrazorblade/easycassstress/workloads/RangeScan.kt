package com.rustyrazorblade.easycassstress.workloads

import com.datastax.oss.driver.api.core.cql.PreparedStatement
import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.metadata.token.TokenRange
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

    override fun prepare(session: CqlSession) {
        val rq =
            if (splits > 1) {
                val metadata = session.getMetadata()
                val tokenRanges = metadata.getTokenMap().get().tokenRanges
                // Convert to ArrayList to be able to split and store
                val tokenRangesList = ArrayList(tokenRanges)
                // Use a mutable list to store split ranges
                val splitRanges = mutableListOf<TokenRange>()
                
                // Split each token range
                for (range in tokenRangesList) {
                    splitRanges.addAll(range.splitEvenly(splits))
                }
                
                ranges = splitRanges
                val tmp = table.split(".")
                var partitionKeys =
                    metadata.getKeyspace(tmp[0]).flatMap { ks ->
                        ks.getTable(tmp[1]).map { table ->
                            table.partitionKey.map { col -> col.name.asInternal() }.joinToString(", ")
                        }
                    }.orElseThrow { RuntimeException("Table not found") }
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
                    val bound = select.bind()
                        .setToken(0, tmp.start)
                        .setToken(1, tmp.end)
                    Operation.SelectStatement(bound)
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
