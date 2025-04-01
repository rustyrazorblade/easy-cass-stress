package com.rustyrazorblade.easycassstress

sealed class DDLStatement {
    class CreateTable() : DDLStatement()

    class Unknown : DDLStatement()

    companion object {
        fun parse(@Suppress("UNUSED_PARAMETER") cql: String): DDLStatement {
            // TODO: Implement CQL parsing logic
            return DDLStatement.Unknown()
        }
    }
}
