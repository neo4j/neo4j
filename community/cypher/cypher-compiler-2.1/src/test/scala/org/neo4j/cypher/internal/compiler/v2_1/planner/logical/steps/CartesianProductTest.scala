/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_1.planner.logical.steps

import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.PlanTable
import org.neo4j.cypher.internal.compiler.v2_1.planner.LogicalPlanningTestSupport
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.{QueryPlan, LogicalPlan}
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.steps.QueryPlanProducer._
import org.mockito.Mockito._
import org.mockito.Matchers._
import org.neo4j.cypher.internal.compiler.v2_1.HardcodedGraphStatistics

class CartesianProductTest extends CypherFunSuite with LogicalPlanningTestSupport {
  implicit val context = newMockedLogicalPlanContext(
    planContext = newMockedPlanContext,
    metrics = newMockedMetricsFactory.newMetrics(hardcodedStatistics, newMockedSemanticTable))


  test("single plan is returned as is") {
    val plan = newMockedQueryPlan("a")

    val cost = Map(plan.plan -> 1.0)
    implicit val (table, context) = prepare(cost, plan)

    cartesianProduct(table) should equal(plan)
  }

  test("two plans are ordered by cheapest first") {
    val plan1 = newMockedQueryPlan("a")
    val plan2 = newMockedQueryPlan("b")
    val cost = Map(plan1.plan -> 1.0, plan2.plan -> 2.0)

    implicit val (table, context) = prepare(cost, plan1, plan2)

    cartesianProduct(table) should equal(planCartesianProduct(plan1, plan2))
  }

  test("three plans are ordered by cheapest first") {
    val plan1 = newMockedQueryPlan("a")
    val plan2 = newMockedQueryPlan("b")
    val plan3 = newMockedQueryPlan("c")
    val cost = Map(plan1.plan -> 3.0, plan2.plan -> 2.0, plan3.plan -> 1.0)

    implicit val (table, context) = prepare(cost, plan1, plan2, plan3)

    cartesianProduct(table) should equal(
      planCartesianProduct(plan3,
        planCartesianProduct(plan2, plan1)
      )
    )
  }

  private def prepare(cost: LogicalPlan => Double, plans: QueryPlan*) = {
    val factory = newMockedMetricsFactory

    when(factory.newCostModel(any())).thenReturn(cost)
    implicit val context = newMockedLogicalPlanContext(
      planContext = newMockedPlanContext,
      metrics = factory.newMetrics(HardcodedGraphStatistics, newMockedSemanticTable)
    )

    val table = PlanTable(plans.map(p => p.plan.availableSymbols -> p).toMap)

    (table, context)
  }
}
