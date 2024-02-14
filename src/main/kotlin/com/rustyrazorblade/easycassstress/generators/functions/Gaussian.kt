package com.rustyrazorblade.easycassstress.generators.functions
import  com.rustyrazorblade.easycassstress.converters.HumanReadableConverter
import  com.rustyrazorblade.easycassstress.generators.Function
import  com.rustyrazorblade.easycassstress.generators.FieldGenerator


@Function(name="gaussian",
        description = "Gaussian (normal) numerical data distribution")
class Gaussian : FieldGenerator {
    var min: Long = 0
    var max: Long = 1000000

    override fun setParameters(params: List<String>) {
        min = HumanReadableConverter().convert(params[0])
        max = HumanReadableConverter().convert(params[1])
    }


    override fun getDescription() = """
        Generates numbers following a gaussian (normal) distribution.  This is useful for simulating certain workloads which use certain values more than others.
    """.trimIndent()


}