package com.rustyrazorblade.easycassstress.generators.functions

import  com.rustyrazorblade.easycassstress.converters.HumanReadableConverter
import  com.rustyrazorblade.easycassstress.generators.FieldGenerator
import org.apache.commons.text.RandomStringGenerator
import java.util.concurrent.ThreadLocalRandom
import  com.rustyrazorblade.easycassstress.generators.Function


@Function(name="random",
        description = "Random numbers.")
class Random : FieldGenerator {




    var min = 0L
    var max = 100000L

    override fun setParameters(params: List<String>) {
        min = HumanReadableConverter().convert(params[0])
        max = HumanReadableConverter().convert(params[1])
    }

    override fun getInt(): Int {
        if(min > Int.MAX_VALUE || max > Int.MAX_VALUE)
            throw Exception("Int larger than Int.MAX_VALUE requested, use a long instead")

        return ThreadLocalRandom.current().nextInt(min.toInt(), max.toInt())
    }

    override fun getText(): String {
        val length = ThreadLocalRandom.current().nextInt(min.toInt(), max.toInt())

        val generator = RandomStringGenerator.Builder().withinRange(65, 90).build()
        return generator.generate(length)
    }

    companion object {
        fun create(min: Long, max: Long) = Random()
                .apply {
                    this.min = min
                    this.max = max
                }
    }

    override fun getDescription() = """
        Completely random data with even distribution.
    """.trimIndent()
}