/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.compiler.planner.logical.plans

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport.VariableStringInterpolator
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport
import org.neo4j.cypher.internal.logical.plans.Apply
import org.neo4j.cypher.internal.logical.plans.Argument
import org.neo4j.cypher.internal.logical.plans.CartesianProduct
import org.neo4j.cypher.internal.logical.plans.Eager
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class LogicalPlanTest extends CypherFunSuite with LogicalPlanningTestSupport {

  test("single row returns itself as the leafs") {
    val argument = Argument(Set(v"a"))

    argument.leaves should equal(Seq(argument))
  }

  test("apply with two arguments should return them both") {
    val argument1 = Argument(Set(v"a"))
    val argument2 = Argument()
    val apply = Apply(argument1, argument2)

    apply.leaves should equal(Seq(argument1, argument2))
  }

  test("apply pyramid should work multiple levels deep") {
    val argument1 = Argument(Set(v"a"))
    val argument2 = Argument()
    val argument3 = Argument(Set(v"b"))
    val argument4 = Argument()
    val apply1 = Apply(argument1, argument2)
    val apply2 = Apply(argument3, argument4)
    val metaApply = Apply(apply1, apply2)

    metaApply.leaves should equal(Seq(argument1, argument2, argument3, argument4))
  }

  test("equals with similar and equal plans") {
    val p1 = Apply(Argument(), Argument())
    val p2 = Apply(Argument(), Argument())
    val p3 = CartesianProduct(Argument(), Argument())

    val p4 = Eager(p1)
    val p5 = Eager(p2)
    val p6 = Eager(p3)

    p1 should equal(p1)
    p1 should equal(p2)
    p1 should not equal (p3)
    p2 should equal(p1)
    p2 should equal(p2)
    p2 should not equal (p3)
    p3 should not equal (p1)
    p3 should not equal (p2)
    p3 should equal(p3)

    p4 should equal(p5)
    p4 should not equal (p6)
  }

  test("leftmostLeaf should return left most leaf") {
    // given
    val p1 = Argument()
    val p2 = Argument()
    val p3 = Argument()
    val p4 = Argument()
    val p5 = Apply(p1, p2)
    val p6 = Apply(p3, p4)

    // when
    val p7 = CartesianProduct(p5, p6)
    // then
    p7.leftmostLeaf should equal(p1)

    // when
    val p8 = CartesianProduct(p6, p5)
    // then
    p8.leftmostLeaf should equal(p3)
  }
}
