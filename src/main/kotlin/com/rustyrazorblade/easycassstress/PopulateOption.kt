package com.rustyrazorblade.easycassstress

sealed class PopulateOption {
    class Standard : PopulateOption()

    class Custom(val rows: Long, val deletes: Boolean = true) : PopulateOption()
}
