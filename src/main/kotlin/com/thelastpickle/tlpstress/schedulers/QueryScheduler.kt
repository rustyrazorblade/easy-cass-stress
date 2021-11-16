package com.thelastpickle.tlpstress.schedulers

import com.thelastpickle.tlpstress.PartitionKey

interface QueryScheduler {
    fun start()
    fun generateKey() : Sequence<PartitionKey>
}