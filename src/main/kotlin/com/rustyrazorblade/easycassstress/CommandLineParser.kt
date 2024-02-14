package com.rustyrazorblade.easycassstress

import com.beust.jcommander.JCommander
import com.beust.jcommander.Parameter
import  com.rustyrazorblade.easycassstress.commands.*

class MainArgs {

    @Parameter(names = ["--help", "-h"], description = "Shows this help.")
    var help = false

}

class CommandLineParser(val jCommander: JCommander,
                        val commands: Map<String, IStressCommand>) {


    companion object {
        fun parse(arguments: Array<String>): com.rustyrazorblade.easycassstress.CommandLineParser {

            // JCommander set up
            val jcommander = JCommander.newBuilder().programName("easy-cass-stress")
            val args = com.rustyrazorblade.easycassstress.MainArgs()

            // needed to get help
            jcommander.addObject(args)
            // subcommands

            val commands = mapOf(
                    "run" to Run(arguments.joinToString(" ")),
                    "info" to Info(),
                    "list" to ListCommand(),
                    "fields" to Fields())

            for(x in commands.entries) {
                jcommander.addCommand(x.key, x.value)
            }

            val jc = jcommander.build()
            jc.parse(*arguments)

            if (jc.parsedCommand == null) {
                jc.usage()
                System.exit(0)
            }
            return com.rustyrazorblade.easycassstress.CommandLineParser(jc, commands)
        }
    }

    fun execute() {
        getCommandInstance().execute()
    }

    fun getParsedCommand() : String {
        return jCommander.parsedCommand
    }

    fun getCommandInstance() : IStressCommand {
        return commands[getParsedCommand()]!!

    }


}

