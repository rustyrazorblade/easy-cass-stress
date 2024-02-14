package com.rustyrazorblade.easycassstress.generators

import  com.rustyrazorblade.easycassstress.generators.functions.FirstName
import org.junit.jupiter.api.Test

internal class FirstNameTest {
    @Test
    fun getNameTest() {
        val tmp = FirstName()
        val n = tmp.getText()
        println(n)

    }
}