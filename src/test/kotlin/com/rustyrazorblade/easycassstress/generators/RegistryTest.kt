package com.rustyrazorblade.easycassstress.generators

import com.rustyrazorblade.easycassstress.generators.functions.Random
import com.rustyrazorblade.easycassstress.generators.functions.USCities
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

internal class RegistryTest {
    lateinit var registry: Registry

    @BeforeEach
    fun setUp() {
        registry =
            Registry.create()
                .setDefault("test", "city", USCities())
                .setDefault(
                    "test", "age",
                    Random().apply {
                        min = 10
                        max = 100
                    },
                )
    }

    @Test
    fun getOverriddenTypeTest() {
        assertThat(registry.getGenerator("test", "city")).isInstanceOf(USCities::class.java)
        registry.setOverride(
            "test",
            "city",
            Random().apply {
                min = 10
                max = 100
            },
        )

        assertThat(registry.getGenerator("test", "city")).isInstanceOf(Random::class.java)
    }
}
