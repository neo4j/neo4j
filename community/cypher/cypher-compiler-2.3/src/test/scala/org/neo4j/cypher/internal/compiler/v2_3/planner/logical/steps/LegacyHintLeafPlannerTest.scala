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
package org.neo4j.cypher.internal.compiler.v2_2.planner.logical.steps

import org.mockito.Mockito._
import org.neo4j.cypher.internal.frontend.v2_3.ast._
import org.neo4j.cypher.internal.compiler.v2_3.planner._
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.Cost
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.Metrics.QueryGraphSolverInput
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.plans.{IdName, LegacyIndexSeek, LogicalPlan}
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.steps.legacyHintLeafPlanner
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite

class LegacyHintLeafPlannerTest extends CypherFunSuite  with LogicalPlanningTestSupport {

  private val statistics = hardcodedStatistics
  private implicit val subQueryLookupTable = Map.empty[PatternExpression, QueryGraph]

  test("Does produce legacy hint leaf plan") {
    val identifier: Identifier = Identifier("n")_
    val hint: NodeByIndexQuery = NodeByIndexQuery(identifier, null, null)(pos)
    val qg = QueryGraph(
      patternNodes = Set(IdName("n")),
      hints = Set(hint)
    )

    val factory = newMockedMetricsFactory
    when(factory.newCostModel()).thenReturn((plan: LogicalPlan, input: QueryGraphSolverInput) => plan match {
      case _: LegacyIndexSeek => Cost(1)
      case _                  => Cost(Double.MaxValue)
    })
    implicit val context = newMockedLogicalPlanningContext(
      planContext = newMockedPlanContext,
      metrics = factory.newMetrics(statistics)
    )
    when(context.semanticTable.isNode(identifier)).thenReturn(true)

    // when
    val resultPlans = legacyHintLeafPlanner(qg)

    // then
    resultPlans should equal(Seq(LegacyIndexSeek(IdName("n"), hint, Set.empty)(null)))
  }

  test("Does not produce legacy hint leaf plan if hinted identifier has already been solved") {
    val identifier: Identifier = Identifier("n")_
    val qg = QueryGraph(
      patternNodes = Set(IdName("n")),
      argumentIds = Set(IdName("n")),
      hints = Set(NodeByIndexQuery(identifier, null, null)(pos))
    )

    val factory = newMockedMetricsFactory
    when(factory.newCostModel()).thenReturn((plan: LogicalPlan, input: QueryGraphSolverInput) => plan match {
      case _: LegacyIndexSeek => Cost(1)
      case _                  => Cost(Double.MaxValue)
    })
    implicit val context = newMockedLogicalPlanningContext(
      planContext = newMockedPlanContext,
      metrics = factory.newMetrics(statistics)
    )
    when(context.semanticTable.isNode(identifier)).thenReturn(true)

    // when
    val resultPlans = legacyHintLeafPlanner(qg)

    // then
    resultPlans should equal(Seq.empty)
  }
}
