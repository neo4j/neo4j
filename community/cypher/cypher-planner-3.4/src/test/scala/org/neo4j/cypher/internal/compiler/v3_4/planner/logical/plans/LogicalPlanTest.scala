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
package org.neo4j.cypher.internal.compiler.v3_4.planner.logical.plans

import org.neo4j.cypher.internal.compiler.v3_4.planner.{FakePlan, LogicalPlanningTestSupport}
import org.neo4j.cypher.internal.ir.v3_4.{CardinalityEstimation, IdName, PlannerQuery}
import org.neo4j.cypher.internal.util.v3_4.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.v3_4.logical.plans.{Apply, Argument}

class LogicalPlanTest extends CypherFunSuite with LogicalPlanningTestSupport  {

  test("updating the planner query works well") {
    val initialPlan = FakePlan()(solved)

    val updatedPlannerQuery = CardinalityEstimation.lift(PlannerQuery.empty.amendQueryGraph(_.addPatternNodes(IdName("a"))), 0.0)

    val newPlan = initialPlan.updateSolved(updatedPlannerQuery)

    newPlan.solved should equal(updatedPlannerQuery)
  }

  test("single row returns itself as the leafs") {
    val argument = Argument(Set(IdName("a")))(solved)

    argument.leaves should equal(Seq(argument))
  }

  test("apply with two arguments should return them both") {
    val argument1 = Argument(Set(IdName("a")))(solved)
    val argument2 = Argument()(solved)
    val apply = Apply(argument1, argument2)(solved)

    apply.leaves should equal(Seq(argument1, argument2))
  }

  test("apply pyramid should work multiple levels deep") {
    val argument1 = Argument(Set(IdName("a")))(solved)
    val argument2 = Argument()(solved)
    val argument3 = Argument(Set(IdName("b")))(solved)
    val argument4 = Argument()(solved)
    val apply1 = Apply(argument1, argument2)(solved)
    val apply2 = Apply(argument3, argument4)(solved)
    val metaApply = Apply(apply1, apply2)(solved)

    metaApply.leaves should equal(Seq(argument1, argument2, argument3, argument4))
  }

  test("calling updateSolved on argument should work") {
    val argument = Argument(Set(IdName("a")))(solved)
    val updatedPlannerQuery = CardinalityEstimation.lift(PlannerQuery.empty.amendQueryGraph(_.addPatternNodes(IdName("a"))), 0.0)
    val newPlan = argument.updateSolved(updatedPlannerQuery)
    newPlan.solved should equal(updatedPlannerQuery)
  }
}
