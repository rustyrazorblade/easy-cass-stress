package com.rustyrazorblade.easycassstress

/**
 * Will replace the current requirement that a PK can only be a text field
 */
class PartitionKey(val prefix: String, val id: Long) {

    fun getText(): String {
        return prefix + id.toString()
    }
}