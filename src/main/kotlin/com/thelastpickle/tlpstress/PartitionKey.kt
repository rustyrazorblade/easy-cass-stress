package com.thelastpickle.tlpstress

import java.time.Instant

/**
 * Will replace the current requirement that a PK can only be a text field
 */
class PartitionKey(val prefix: String, val id: Long, val generationTime: Instant = Instant.now()) {

    fun getText(): String {
        return prefix + id.toString()
    }
}