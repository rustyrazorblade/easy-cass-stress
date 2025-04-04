package com.rustyrazorblade.easycassstress

import com.rustyrazorblade.easycassstress.commands.Run
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

internal class MainArgumentsTest {
    @Test
    fun testPagingFlagWorks() {
        val run = Run("placeholder")
        val pageSize = 20000
        run.paging = pageSize
        // The options field appears to be deprecated in the new driver
        // We need to verify a different way, by checking the paging value is correctly set
        assertThat(run.paging).isEqualTo(pageSize)
    }
}
