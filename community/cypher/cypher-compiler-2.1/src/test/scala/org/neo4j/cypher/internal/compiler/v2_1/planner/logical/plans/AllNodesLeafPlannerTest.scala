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
package org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans

import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_1.ast.Identifier
import org.neo4j.cypher.internal.compiler.v2_1.planner.{QueryProjection, QueryGraph, LogicalPlanningTestSupport}
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.steps.allNodesLeafPlanner
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.Candidates
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.steps.QueryPlanProducer._

class AllNodesLeafPlannerTest
  extends CypherFunSuite
  with LogicalPlanningTestSupport {

  test("simple all nodes scan") {
    // given
    val qg = QueryGraph(projection = QueryProjection(projections = Map("n" -> Identifier("n") _)), patternNodes = Set(IdName("n")))

    implicit val planContext = newMockedPlanContext
    implicit val context = newMockedLogicalPlanContext(
      planContext = planContext,
      queryGraph = qg,
      metrics = newMockedMetricsFactory.newMetrics(newMockedStatistics, newMockedSemanticTable))

    // when
    val resultPlans = allNodesLeafPlanner(context.queryGraph)

    // then
    resultPlans should equal(Candidates(planAllNodesScan(IdName("n"))))
  }
}
