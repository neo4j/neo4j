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

import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.LogicalPlanExtension
import org.neo4j.cypher.internal.util.attribution.SequentialIdGen
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class LogicalPlanEqualityTest extends CypherFunSuite with LogicalPlanningTestSupport {

  test("leafs") {
    Leaf(Map("string" -> 42)) should equal(Leaf(Map("string" -> 42)))
    Leaf(Map("string" -> 42)) should not equal Leaf(Map("string" -> 1337))
  }

  test("non-branching trees") {
    Unary(Unary(Leaf("foo"), "bar"), "baz") should equal(Unary(Unary(Leaf("foo"), "bar"), "baz"))
    Unary(Unary(Leaf("foo"), "bar"), "baz") should equal(Unary(Unary(Leaf("foo"), "bar"), "baz"))
    Unary(Unary(Leaf("foo"), "bar"), "baz") should not equal Unary(Leaf("foo"), "bar")
  }

  test("branching trees") {
    // Identical
    Binary(
      Binary(
        Unary(Leaf("left"), "left"),
        Unary(Unary(Leaf("left"), "left"), "left"),
        "branch1"
      ),
      Unary(Leaf("right"), "right"),
      "branch2"
    ) should equal(Binary(
      Binary(
        Unary(Leaf("left"), "left"),
        Unary(Unary(Leaf("left"), "left"), "left"),
        "branch1"
      ),
      Unary(Leaf("right"), "right"),
      "branch2"
    ))

    // Flip a branch
    Binary(
      Binary(
        Unary(Leaf("left"), "left"),
        Unary(Unary(Leaf("left"), "left"), "left"),
        "branch1"
      ),
      Unary(Leaf("right"), "right"),
      "branch2"
    ) should not equal Binary(
      Binary(
        Unary(Unary(Leaf("left"), "left"), "left"),
        Unary(Leaf("left"), "left"),
        "branch1"
      ),
      Unary(Leaf("right"), "right"),
      "branch2"
    )

    // Change a leaf
    Binary(
      Binary(
        Unary(Leaf("left"), "left"),
        Unary(Unary(Leaf("left"), "left"), "left"),
        "branch1"
      ),
      Unary(Leaf("right"), "right"),
      "branch2"
    ) should not equal Binary(
      Binary(
        Unary(Leaf("left"), "left"),
        Unary(Unary(Leaf("DIFFERENT!!"), "left"), "left"),
        "branch1"
      ),
      Unary(Leaf("right"), "right"),
      "branch2"
    )
  }

  case class Binary(left: LogicalPlan, right: LogicalPlan, value: Any)
      extends LogicalPlanExtension(new SequentialIdGen) {

    override val availableSymbols: Set[LogicalVariable] = Set.empty

    override def lhs: Option[LogicalPlan] = Some(left)

    override def rhs: Option[LogicalPlan] = Some(right)
  }

  case class Unary(child: LogicalPlan, value: Any) extends LogicalPlanExtension(new SequentialIdGen) {

    override val availableSymbols: Set[LogicalVariable] = Set.empty

    override def rhs: Option[LogicalPlan] = None

    override def lhs: Option[LogicalPlan] = Some(child)
  }

  case class Leaf(value: Any) extends LogicalPlanExtension(new SequentialIdGen) {

    override def lhs: Option[LogicalPlan] = None

    override def rhs: Option[LogicalPlan] = None

    override val availableSymbols: Set[LogicalVariable] = Set.empty
  }
}
