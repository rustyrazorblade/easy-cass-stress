package com.rustyrazorblade.easycassstress

sealed class Either<out A, out B> {
    class Left<A>(val value: A) : Either<A, Nothing>()

    class Right<B>(val value: B) : Either<Nothing, B>()
}
