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
package org.neo4j.cypher.internal.compiler.v2_1.planner.logical

import org.neo4j.graphdb.Direction
import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_1.planner.LogicalPlanningTestSupport
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans._
import org.neo4j.cypher.internal.compiler.v2_1.ast._
import org.mockito.Mockito._
import org.mockito.Matchers._

class OptionalMatchPlanningIntegrationTest extends CypherFunSuite with LogicalPlanningTestSupport   {

  test("should build plans containing joins") {
    implicit val planContext = newMockedPlanContext
    val factory = newMockedMetricsFactory
    when(factory.newCardinalityEstimator(any(), any(), any())).thenReturn((plan: LogicalPlan) => plan match {
      case _: AllNodesScan => 2000000
      case _: NodeByLabelScan => 20
      case _: Expand => 10
      case _: OuterHashJoin => 20
      case _: SingleRow => 1
      case _ => Double.MaxValue
    })
    implicit val planner = newPlanner(factory)
    when(planContext.getOptLabelId("X")).thenReturn(None)
    when(planContext.getOptLabelId("Y")).thenReturn(None)

    produceLogicalPlan("MATCH (a:X)-[r1]->(b) OPTIONAL MATCH (b)-[r2]->(c:Y) RETURN b") should equal(
      Projection(
        OuterHashJoin("b",
          Expand(NodeByLabelScan("a", Left("X")), "a", Direction.OUTGOING, Seq(), "b", "r1", SimplePatternLength),
          Expand(NodeByLabelScan("c", Left("Y")), "c", Direction.INCOMING, Seq(), "b", "r2", SimplePatternLength)
        ),
        expressions = Map("b" -> Identifier("b") _)
      )
    )
  }
}
