package com.rustyrazorblade.easycassstress.generators

import com.rustyrazorblade.easycassstress.generators.functions.Book
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

internal class BookTest {
    @Test
    fun bookSliceTest() {
        val b = Book()
        var previous = ""
        for (i in 1..10) {
            val tmp = b.getText()
            assertThat(tmp).isNotBlank().isNotEqualToIgnoringCase(previous)
            previous = tmp
        }
    }
}
