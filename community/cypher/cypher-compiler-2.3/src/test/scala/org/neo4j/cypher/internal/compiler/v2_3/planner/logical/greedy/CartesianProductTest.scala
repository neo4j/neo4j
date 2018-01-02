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
package org.neo4j.cypher.internal.compiler.v2_3.planner.logical.greedy

import org.mockito.Mockito._
import org.neo4j.cypher.internal.compiler.v2_3.HardcodedGraphStatistics
import org.neo4j.cypher.internal.frontend.v2_3.ast.PatternExpression
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.Cost
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.Metrics.QueryGraphSolverInput
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.plans.{CartesianProduct, LogicalPlan}
import org.neo4j.cypher.internal.compiler.v2_3.planner.{LogicalPlanningTestSupport, QueryGraph}
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite

class CartesianProductTest extends CypherFunSuite with LogicalPlanningTestSupport {

  private implicit val subQueryLookupTable = Map.empty[PatternExpression, QueryGraph]
  private val qg = newMockedQueryGraph

  test("single plan does not produce cartesian product") {
    val plan = newMockedLogicalPlan("a")

    val cost = Map(plan -> 1.0)
    implicit val (table, context) = prepare(cost, plan)

    cartesianProduct(table, qg) shouldBe empty
  }

  test("cartesian product produces the best possible combination") {
    val plan1 = newMockedLogicalPlan("a")
    val plan2 = newMockedLogicalPlan("b")
    def cost(plan: LogicalPlan): Double = plan match {
      case p if p == plan1 => 1.0
      case p if p == plan2 => 2.0
      case CartesianProduct(a, b) => cost(a) + 10 * cost(b)
    }
    implicit val (table, context) = prepare(cost, plan1, plan2)
    cartesianProduct(table, qg).toSet should equal(Set(CartesianProduct(plan2, plan1)(solved)))
  }

  private def prepare(cost: LogicalPlan => Double, plans: LogicalPlan*) = {
    val factory = newMockedMetricsFactory

    when(factory.newCostModel()).thenReturn((plan: LogicalPlan, input: QueryGraphSolverInput) => Cost(cost(plan)))
    implicit val context = newMockedLogicalPlanningContext(
      planContext = newMockedPlanContext,
      metrics = factory.newMetrics(HardcodedGraphStatistics)
    )

    val table = greedyPlanTableWith(plans:_*)

    (table, context)
  }
}
