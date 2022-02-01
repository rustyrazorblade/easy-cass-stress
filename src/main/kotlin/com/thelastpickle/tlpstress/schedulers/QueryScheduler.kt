package com.thelastpickle.tlpstress.schedulers

import com.thelastpickle.tlpstress.PartitionKey

interface QueryScheduler {
    fun generateKey() : Sequence<PartitionKey>

    fun start() {

    }

    fun stop() {

    }
}