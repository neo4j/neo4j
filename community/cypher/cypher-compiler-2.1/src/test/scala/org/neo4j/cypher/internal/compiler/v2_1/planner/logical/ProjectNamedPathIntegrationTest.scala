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

import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_1.planner.LogicalPlanningTestSupport
import org.mockito.Mockito._
import org.mockito.Matchers._
import org.neo4j.graphdb.Direction
import org.neo4j.cypher.internal.compiler.v2_1.ast._
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans._

class ProjectNamedPathIntegrationTest extends CypherFunSuite with LogicalPlanningTestSupport   {

  test("should build plans containing path projections") {
    implicit val planContext = newMockedPlanContext
    when(planContext.getOptLabelId("X")).thenReturn(None)
    implicit val planner = newPlanner(newMetricsFactory)

    val patternRel = PatternRelationship("r", ("a", "b"), Direction.OUTGOING, Seq.empty, SimplePatternLength)

    produceLogicalPlan("MATCH p = (a:X)-[r]->(b) RETURN b") should equal(
      Projection(
        ProjectNamedPath(
          NamedRelPath("p", Seq(patternRel)),
          Expand( NodeByLabelScan("a",  Left("X"))(), "a", Direction.OUTGOING, Seq.empty, "b", "r", SimplePatternLength )(patternRel)
        ),
        expressions = Map("b" -> Identifier("b") _)
      )
    )
  }

  test("should build plans containing path projections and path selections") {
    implicit val planContext = newMockedPlanContext
    when(planContext.getOptLabelId("X")).thenReturn(None)
    implicit val planner = newPlanner(newMetricsFactory)

    val patternRel = PatternRelationship("r", ("a", "b"), Direction.OUTGOING, Seq.empty, SimplePatternLength)

    produceLogicalPlan("MATCH p = (a:X)-[r]->(b) WHERE head(nodes(p)) = a RETURN b") should equal(
      Projection(
        Selection(
          Seq(Equals(
            Identifier("a")_,
            FunctionInvocation(FunctionName("head")_, FunctionInvocation(FunctionName("nodes")_, Identifier("p")_)_)_
          )_),
          ProjectNamedPath(
            NamedRelPath("p", Seq(patternRel)),
            Expand( NodeByLabelScan("a",  Left("X"))(), "a", Direction.OUTGOING, Seq.empty, "b", "r", SimplePatternLength )(patternRel)
          )
        ),
        expressions = Map("b" -> Identifier("b") _)
      )
    )
  }
}
