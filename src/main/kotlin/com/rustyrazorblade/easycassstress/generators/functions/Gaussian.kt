/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.rustyrazorblade.easycassstress.generators.functions
import com.rustyrazorblade.easycassstress.converters.HumanReadableConverter
import com.rustyrazorblade.easycassstress.generators.FieldGenerator
import com.rustyrazorblade.easycassstress.generators.Function

@Function(
    name = "gaussian",
    description = "Gaussian (normal) numerical data distribution",
)
class Gaussian : FieldGenerator {
    var min: Long = 0
    var max: Long = 1000000

    override fun setParameters(params: List<String>) {
        min = HumanReadableConverter().convert(params[0])
        max = HumanReadableConverter().convert(params[1])
    }

    override fun getDescription() =
        """
        Generates numbers following a gaussian (normal) distribution.  This is useful for simulating certain workloads which use certain values more than others.
        """.trimIndent()
}
