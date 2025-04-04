package com.rustyrazorblade.easycassstress.converters

import com.beust.jcommander.IStringConverter
import com.datastax.oss.driver.api.core.ConsistencyLevel

class ConsistencyLevelConverter : IStringConverter<ConsistencyLevel> {
    override fun convert(value: String?): ConsistencyLevel {
        // In driver v4, valueOf is not directly accessible, using a safer approach
        return when (value?.uppercase()) {
            "ANY" -> ConsistencyLevel.ANY
            "ONE" -> ConsistencyLevel.ONE
            "TWO" -> ConsistencyLevel.TWO
            "THREE" -> ConsistencyLevel.THREE
            "QUORUM" -> ConsistencyLevel.QUORUM
            "ALL" -> ConsistencyLevel.ALL
            "LOCAL_ONE" -> ConsistencyLevel.LOCAL_ONE
            "LOCAL_QUORUM" -> ConsistencyLevel.LOCAL_QUORUM
            "EACH_QUORUM" -> ConsistencyLevel.EACH_QUORUM
            "LOCAL_SERIAL" -> ConsistencyLevel.LOCAL_SERIAL
            "SERIAL" -> ConsistencyLevel.SERIAL
            else -> throw IllegalArgumentException("Unknown consistency level: $value")
        }
    }
}
