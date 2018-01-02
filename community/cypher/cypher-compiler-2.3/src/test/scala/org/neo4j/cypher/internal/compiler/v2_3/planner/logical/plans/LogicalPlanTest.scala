/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.cypher.internal.compiler.v2_3.planner.logical.plans

import org.neo4j.cypher.internal.compiler.v2_3.planner.{CardinalityEstimation, LogicalPlanningTestSupport, PlannerQuery}
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite

class LogicalPlanTest extends CypherFunSuite with LogicalPlanningTestSupport  {
  case class TestPlan()(val solved: PlannerQuery with CardinalityEstimation) extends LogicalPlan with LogicalPlanWithoutExpressions {
    def lhs: Option[LogicalPlan] = ???
    def availableSymbols: Set[IdName] = ???
    def rhs: Option[LogicalPlan] = ???
    def strictness = ???
  }

  test("updating the planner query works well, thank you very much") {
    val initialPlan = TestPlan()(solved)

    val updatedPlannerQuery = CardinalityEstimation.lift(PlannerQuery.empty.updateGraph(_.addPatternNodes(IdName("a"))), 0.0)

    val newPlan = initialPlan.updateSolved(updatedPlannerQuery)

    newPlan.solved should equal(updatedPlannerQuery)
  }

  test("single row returns itself as the leafs") {
    val singleRow = Argument(Set(IdName("a")))(solved)()

    singleRow.leafs should equal(Seq(singleRow))
  }

  test("apply with two singlerows should return them both") {
    val singleRow1 = Argument(Set(IdName("a")))(solved)()
    val singleRow2 = SingleRow()(solved)
    val apply = Apply(singleRow1, singleRow2)(solved)

    apply.leafs should equal(Seq(singleRow1, singleRow2))
  }

  test("apply pyramid should work multiple levels deep") {
    val singleRow1 = Argument(Set(IdName("a")))(solved)()
    val singleRow2 = SingleRow()(solved)
    val singleRow3 = Argument(Set(IdName("b")))(solved)()
    val singleRow4 = SingleRow()(solved)
    val apply1 = Apply(singleRow1, singleRow2)(solved)
    val apply2 = Apply(singleRow3, singleRow4)(solved)
    val metaApply = Apply(apply1, apply2)(solved)

    metaApply.leafs should equal(Seq(singleRow1, singleRow2, singleRow3, singleRow4))
  }

  test("calling updateSolved on argument should work") {
    val argument = Argument(Set(IdName("a")))(solved)()
    val updatedPlannerQuery = CardinalityEstimation.lift(PlannerQuery.empty.updateGraph(_.addPatternNodes(IdName("a"))), 0.0)
    val newPlan = argument.updateSolved(updatedPlannerQuery)
    newPlan.solved should equal(updatedPlannerQuery)
  }
}
