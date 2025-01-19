package com.rustyrazorblade.easycassstress.converters

import com.beust.jcommander.IStringConverter
import com.datastax.driver.core.ConsistencyLevel

class ConsistencyLevelConverter : IStringConverter<ConsistencyLevel> {
    override fun convert(value: String?): ConsistencyLevel {
        return ConsistencyLevel.valueOf(value!!)
    }
}
