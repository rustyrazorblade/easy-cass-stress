package com.rustyrazorblade.easycassstress

import com.datastax.oss.driver.api.core.metadata.Node
import java.util.function.Predicate

class CoordinatorHostPredicate : Predicate<Node> {
    override fun test(input: Node): Boolean {
        // In driver v4, we don't have direct access to tokens like in v3
        // This is a temporary solution that assumes no nodes are coordinators
        // since we can't access tokens properly for this version
        return false
    }
}
