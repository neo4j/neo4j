/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.compiler.v3_4.planner.logical.plans

import org.neo4j.cypher.internal.compiler.v3_4.planner.LogicalPlanningTestSupport
import org.neo4j.cypher.internal.ir.v3_4.StrictnessMode
import org.neo4j.cypher.internal.util.v3_4.attribution.SequentialIdGen
import org.neo4j.cypher.internal.util.v3_4.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.v3_4.logical.plans.LogicalPlan

class LogicalPlanEqualityTest extends CypherFunSuite with LogicalPlanningTestSupport  {
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
    //Identical
    Binary(
      Binary(
        Unary(Leaf("left"), "left"),
        Unary(Unary(Leaf("left"), "left"), "left"), "branch1"),
      Unary(Leaf("right"), "right"), "branch2") should equal(Binary(
      Binary(
        Unary(Leaf("left"), "left"),
        Unary(Unary(Leaf("left"), "left"), "left"), "branch1"),
      Unary(Leaf("right"), "right"), "branch2"))

    //Flip a branch
    Binary(
      Binary(
        Unary(Leaf("left"), "left"),
        Unary(Unary(Leaf("left"), "left"), "left"), "branch1"),
      Unary(Leaf("right"), "right"), "branch2") should not equal Binary(
      Binary(
        Unary(Unary(Leaf("left"), "left"), "left"),
        Unary(Leaf("left"), "left"), "branch1"),
      Unary(Leaf("right"), "right"), "branch2")

    //Change a leaf
    Binary(
      Binary(
        Unary(Leaf("left"), "left"),
        Unary(Unary(Leaf("left"), "left"), "left"), "branch1"),
      Unary(Leaf("right"), "right"), "branch2") should not equal Binary(
      Binary(
        Unary(Leaf("left"), "left"),
        Unary(Unary(Leaf("DIFFERENT!!"), "left"), "left"), "branch1"),
      Unary(Leaf("right"), "right"), "branch2")
  }

  case class Binary(left: LogicalPlan, right: LogicalPlan, value: Any) extends LogicalPlan(new SequentialIdGen) {

    override val availableSymbols: Set[String] = Set.empty

    override def strictness: StrictnessMode = ???

    override def lhs: Option[LogicalPlan] = Some(left)

    override def rhs: Option[LogicalPlan] = Some(right)
  }

  case class Unary(child: LogicalPlan, value: Any) extends LogicalPlan(new SequentialIdGen) {

    override val availableSymbols: Set[String] = Set.empty

    override def strictness: StrictnessMode = ???

    override def rhs: Option[LogicalPlan] = None

    override def lhs: Option[LogicalPlan] = Some(child)
  }

  case class Leaf(value: Any) extends LogicalPlan(new SequentialIdGen) {

    override def lhs: Option[LogicalPlan] = None

    override def rhs: Option[LogicalPlan] = None

    override val availableSymbols: Set[String] = Set.empty

    override def strictness: StrictnessMode = ???
  }
}
