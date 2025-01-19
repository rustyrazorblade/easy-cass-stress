package com.rustyrazorblade.easycassstress

import com.rustyrazorblade.easycassstress.commands.Run
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

internal class CommandLineParserTest {
    @Test
    fun testBasicParser() {
        val args = arrayOf("run", "BasicTimeSeries")
        val result = com.rustyrazorblade.easycassstress.CommandLineParser.parse(args)
        assertThat(result.getParsedCommand()).isEqualToIgnoringCase("run")
        assertThat(result.getCommandInstance()).isInstanceOf(Run::class.java)
    }
}
