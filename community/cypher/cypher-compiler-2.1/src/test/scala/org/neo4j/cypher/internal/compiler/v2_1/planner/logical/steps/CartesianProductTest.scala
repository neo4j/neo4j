/**
 * Copyright (c) 2002-2016 "Neo Technology,"
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
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.{Cost, Candidates, PlanTable}
import org.neo4j.cypher.internal.compiler.v2_1.planner.{QueryGraph, LogicalPlanningTestSupport}
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.{QueryPlan, LogicalPlan}
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.steps.QueryPlanProducer._
import org.mockito.Mockito._
import org.mockito.Matchers._
import org.neo4j.cypher.internal.compiler.v2_1.HardcodedGraphStatistics
import org.neo4j.cypher.internal.compiler.v2_1.ast.PatternExpression

class CartesianProductTest extends CypherFunSuite with LogicalPlanningTestSupport {

  private implicit val subQueryLookupTable = Map.empty[PatternExpression, QueryGraph]
  private val qg = newMockedQueryGraph

  test("single plan does not produce cartesian product") {
    val plan = newMockedQueryPlan("a")

    val cost = Map(plan.plan -> 1.0)
    implicit val (table, context) = prepare(cost, plan)

    cartesianProduct(table, qg) should equal(Candidates())
  }

  test("cartesian product produces all possible combinations") {
    val plan1 = newMockedQueryPlan("a")
    val plan2 = newMockedQueryPlan("b")
    val cost = Map(plan1.plan -> 1.0, plan2.plan -> 2.0)

    implicit val (table, context) = prepare(cost, plan1, plan2)

    cartesianProduct(table, qg).plans.toSet should equal(Set(planCartesianProduct(plan1, plan2), planCartesianProduct(plan2, plan1)))
  }

  private def prepare(cost: LogicalPlan => Double, plans: QueryPlan*) = {
    val factory = newMockedMetricsFactory

    when(factory.newCostModel(any())).thenReturn(cost.andThen(Cost.apply))
    implicit val context = newMockedLogicalPlanningContext(
      planContext = newMockedPlanContext,
      metrics = factory.newMetrics(HardcodedGraphStatistics, newMockedSemanticTable)
    )

    val table = PlanTable(plans.map(p => p.plan.availableSymbols -> p).toMap)

    (table, context)
  }
}
