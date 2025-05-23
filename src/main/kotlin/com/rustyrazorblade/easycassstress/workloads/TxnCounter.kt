package com.rustyrazorblade.easycassstress.workloads

import com.datastax.oss.driver.api.core.CqlSession
import com.rustyrazorblade.easycassstress.PartitionKey
import com.rustyrazorblade.easycassstress.StressContext
import com.rustyrazorblade.easycassstress.WorkloadParameter

enum class Impl {
    LWT,
    ACCORD,
}

class TxnCounter : IStressProfile {
    @WorkloadParameter("Which type of transaction system to use")
    var impl = Impl.LWT

    @WorkloadParameter("Be implicit or explicit about the update read; false means be explicit, true means be implicit")
    var accordAutoRead: Boolean = true

    @WorkloadParameter("A postfix added to the table name")
    var postfix: String = ""

    private fun name() = "tx_${impl.name.lowercase()}_counter${if (postfix.isEmpty()) "" else "_$postfix"}"

    override fun schema(): List<String> =
        listOf(
            """
            CREATE TABLE IF NOT EXISTS ${name()} (
                id TEXT PRIMARY KEY,
                value INT
            )
            ${if (impl == Impl.ACCORD) "WITH transactional_mode='full'" else ""}
            """.trimIndent(),
        )

    override fun prepare(session: CqlSession) {}

    override fun getRunner(context: StressContext): IStressRunner =
        when (impl) {
            Impl.LWT -> lwtRunner(context)
            Impl.ACCORD -> accordRunner(context)
        }

    private fun lwtRunner(context: StressContext): IStressRunner =
        object : IStressRunner {
            val prepareInsert =
                context.session.prepare(
                    """
                    INSERT INTO ${name()} (id, value) VALUES (?, ?) IF NOT EXISTS
                    """.trimIndent(),
                )
            val update =
                context.session.prepare(
                    """
                    UPDATE ${name()}
                    SET value = value + 1
                    WHERE id = ?
                    IF EXISTS
                    """.trimIndent(),
                )
            val select =
                context.session.prepare(
                    """
                    SELECT *
                    FROM ${name()}
                    WHERE id = ?
                    """.trimIndent(),
                )

            override fun getNextMutation(partitionKey: PartitionKey): Operation = Operation.Mutation(update.bind(partitionKey.getText()))

            override fun getNextPopulate(partitionKey: PartitionKey): Operation =
                Operation.Mutation(prepareInsert.bind(partitionKey.getText(), 0))

            override fun getNextSelect(partitionKey: PartitionKey): Operation {
                val bind = select.bind(partitionKey.getText())
                bind.serialConsistencyLevel = context.mainArguments.serialConsistencyLevel
                return Operation.SelectStatement(bind)
            }

            override fun getNextDelete(partitionKey: PartitionKey): Operation {
                TODO("Counter test does not support delete")
            }
        }

    private fun accordRunner(context: StressContext): IStressRunner =
        object : IStressRunner {
            val prepareInsert =
                context.session.prepare(
                    """
                    BEGIN TRANSACTION
                        LET a = (SELECT * FROM ${name()} WHERE id = ?);
                        IF a IS NULL THEN
                            INSERT INTO ${name()} (id, value)
                            VALUES (?, 0);
                        END IF
                    COMMIT TRANSACTION
                    """.trimIndent(),
                )
            val update =
                context.session.prepare(
                    if (accordAutoRead) {
                        """
                        BEGIN TRANSACTION
                            UPDATE ${name()}
                              SET value += 1
                              WHERE id = ?;
                        COMMIT TRANSACTION
                        """.trimIndent()
                    } else {
                        """
                        BEGIN TRANSACTION
                            LET a = (SELECT * FROM ${name()} WHERE id = ?);
                            IF a IS NOT NULL THEN
                                UPDATE ${name()}
                                  SET value = a.value + 1
                                  WHERE id = ?;
                            END IF
                        COMMIT TRANSACTION
                        """.trimIndent()
                    },
                )
            val select =
                context.session.prepare(
                    """
                    BEGIN TRANSACTION
                        SELECT *
                          FROM ${name()}
                          WHERE id = ?;
                    COMMIT TRANSACTION
                    """.trimIndent(),
                )

            override fun getNextMutation(partitionKey: PartitionKey): Operation =
                Operation.Mutation(
                    if (accordAutoRead) {
                        update.bind(partitionKey)
                    } else {
                        update.bind(partitionKey, partitionKey)
                    },
                )

            override fun getNextPopulate(partitionKey: PartitionKey): Operation =
                Operation.Mutation(prepareInsert.bind(partitionKey, partitionKey))

            override fun getNextSelect(partitionKey: PartitionKey): Operation = Operation.SelectStatement(select.bind(partitionKey))

            override fun getNextDelete(partitionKey: PartitionKey): Operation {
                TODO("Counter test does not support delete")
            }
        }
}
