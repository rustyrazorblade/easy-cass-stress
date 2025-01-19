package com.rustyrazorblade.easycassstress.generators.functions

import com.rustyrazorblade.easycassstress.generators.FieldGenerator
import com.rustyrazorblade.easycassstress.generators.Function
import java.util.concurrent.ThreadLocalRandom

@Function(
    name = "lastname",
    description = "Last names.",
)
class LastName : FieldGenerator {
    val names = mutableListOf<String>()

    init {
        val tmp =
            this::class.java.getResource("/names/last.txt")
                .readText()
                .split("\n")
                .map { it.split(" ").first() }
        names.addAll(tmp)
    }

    override fun setParameters(params: List<String>) {
    }

    override fun getText(): String {
        val element = ThreadLocalRandom.current().nextInt(0, names.size)
        return names[element]
    }

    override fun getDescription() =
        """
        Supplies common last names.
        """.trimIndent()
}
