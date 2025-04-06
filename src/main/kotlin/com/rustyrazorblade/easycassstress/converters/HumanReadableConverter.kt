package com.rustyrazorblade.easycassstress.converters

import com.beust.jcommander.IStringConverter

class HumanReadableConverter : IStringConverter<Long> {
    override fun convert(value: String?): Long {
        val regex = """(\d+)([BbMmKk]?)""".toRegex()
        val result = regex.find(value!!)

        return result?.groups?.let {
            val numValue = it[1]?.value
            val label = it[2]

            if (numValue == null) return 0L

            when (label?.value?.lowercase()) {
                "k" -> 1000L * numValue.toLong()
                "m" -> 1000000L * numValue.toLong()
                "b" -> 1000000000L * numValue.toLong()
                else -> numValue.toLong()
            }
        } ?: 0L
    }
}
