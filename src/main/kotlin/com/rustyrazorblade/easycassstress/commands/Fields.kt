package com.rustyrazorblade.easycassstress.commands

import  com.rustyrazorblade.easycassstress.generators.Registry


class Fields : IStressCommand {
    override fun execute() {
        // show each generator
        val registry = Registry.create()

        for(func in registry.getFunctions()) {
            println("Generator: ${func.name}")

            print("Description:")
            println(func.description)

        }

    }
}