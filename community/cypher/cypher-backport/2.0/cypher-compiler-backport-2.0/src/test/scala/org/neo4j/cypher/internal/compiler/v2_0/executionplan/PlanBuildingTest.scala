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
package org.neo4j.cypher.internal.compiler.v2_0.executionplan

import org.junit.Test
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.cypher.internal.compiler.v2_0.spi.PlanContext
import org.neo4j.cypher.internal.compiler.v2_0.parser.CypherParser
import org.neo4j.cypher.internal.compiler.v2_0.pipes.{Pipe, TraversalMatchPipe, DistinctPipe}
import org.scalatest.mock.MockitoSugar
import org.mockito.Mockito._

class PlanBuildingTest extends MockitoSugar {
  val gds: GraphDatabaseService = null
  val planContext: PlanContext = mock[PlanContext]
  val parser = CypherParser()
  val planBuilder = new ExecutionPlanBuilder(gds)

  @Test def should_use_distinct_pipe_for_distinct() {
    val pipe = buildExecutionPipe("MATCH n RETURN DISTINCT n")

    assert(pipe.exists(_.isInstanceOf[DistinctPipe]), "Expected a DistinctPipe but didn't find any")
  }

  @Test def should_use_traversal_matcher_when_possible() {

    when(planContext.getOptLabelId("Foo")).thenReturn(Some(1))

    val pipe = buildExecutionPipe("match (n:Foo)-->(x) return x")

    assert(pipe.exists(_.isInstanceOf[TraversalMatchPipe]), "Expected a DistinctPipe but didn't find any")
  }

  private def buildExecutionPipe(q: String): Pipe = {
    val abstractQuery = parser.parseToQuery(q)
    planBuilder.buildPipes(planContext, abstractQuery)._1
  }
}
