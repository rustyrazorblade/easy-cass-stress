package com.rustyrazorblade.easycassstress.profiles

import com.datastax.driver.core.PreparedStatement
import com.datastax.driver.core.Session
import com.rustyrazorblade.easycassstress.PartitionKey
import com.rustyrazorblade.easycassstress.StressContext
import com.rustyrazorblade.easycassstress.WorkloadParameter

/**
 * this is a bit of an oddball workout because it doesn't support writes.
 */

class TableScan : IStressProfile {
    @WorkloadParameter("Table to perform full scan against.  Does not support writes of any kind.")
    var table = "system.local"

    lateinit var select: PreparedStatement
    override fun prepare(session: Session) {
        select = session.prepare("SELECT * from $table")
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
                return Operation.SelectStatement(select.bind())
            }

            override fun getNextDelete(partitionKey: PartitionKey): Operation {
                // we need the ability to say a workload doesn't support deletes
                TODO("Not yet implemented")
            }

        }
    }

}