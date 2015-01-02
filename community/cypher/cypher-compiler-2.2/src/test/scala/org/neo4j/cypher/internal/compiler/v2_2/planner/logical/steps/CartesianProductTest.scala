/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_2.planner.logical.steps

import org.mockito.Matchers._
import org.mockito.Mockito._
import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_2.HardcodedGraphStatistics
import org.neo4j.cypher.internal.compiler.v2_2.ast.PatternExpression
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.Cost
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.Metrics.QueryGraphCardinalityInput
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.plans.{CartesianProduct, LogicalPlan}
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.steps.LogicalPlanProducer._
import org.neo4j.cypher.internal.compiler.v2_2.planner.{LogicalPlanningTestSupport, QueryGraph}

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
    cartesianProduct(table, qg).toSet should equal(Set(planCartesianProduct(plan2, plan1)))
  }

  private def prepare(cost: LogicalPlan => Double, plans: LogicalPlan*) = {
    val factory = newMockedMetricsFactory

    when(factory.newCostModel(any())).thenReturn((plan: LogicalPlan, _: QueryGraphCardinalityInput) => Cost(cost(plan)))
    implicit val context = newMockedLogicalPlanningContext(
      planContext = newMockedPlanContext,
      metrics = factory.newMetrics(HardcodedGraphStatistics, newMockedSemanticTable)
    )

    val table = planTableWith(plans:_*)

    (table, context)
  }
}
