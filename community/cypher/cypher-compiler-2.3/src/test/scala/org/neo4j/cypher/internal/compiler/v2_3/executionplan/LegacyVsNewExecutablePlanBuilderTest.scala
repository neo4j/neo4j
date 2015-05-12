/*
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
package org.neo4j.cypher.internal.compiler.v2_3.executionplan

import org.mockito.Mockito._
import org.neo4j.cypher.internal.compiler.v2_3.{CompilationPhaseTracer, PreparedQuery}
import org.neo4j.cypher.internal.compiler.v2_3.parser.{CypherParser}
import org.neo4j.cypher.internal.compiler.v2_3.spi.PlanContext
import org.neo4j.cypher.internal.compiler.v2_3.test_helpers.CypherFunSuite

class LegacyVsNewExecutablePlanBuilderTest extends CypherFunSuite {

  val parser = new CypherParser

  test("should delegate var length to old pipe builder") {
    new uses("MATCH ()-[r*]->() RETURN r") {
      result should equal(pipeInfo)
      assertUsed(newBuilder)
    }
  }

  test("should delegate plain shortest path to new pipe builder") {
    new uses("MATCH shortestPath(()-[r*]->()) RETURN r") {
      result should equal(pipeInfo)
      assertUsed(newBuilder)
    }
  }

  test("should delegate shortest path with var length expressions to old pipe builder") {
    new uses("MATCH shortestPath(()-[r*]->({x: ()-[:T*]->()})) RETURN r") {
      result should equal(pipeInfo)
      assertUsed(newBuilder)
    }
  }

  class uses(queryText: String) {
    // given
    val planContext = mock[PlanContext]
    val oldBuilder = mock[ExecutablePlanBuilder]
    val newBuilder = mock[ExecutablePlanBuilder]
    val pipeBuilder = new LegacyVsNewExecutablePlanBuilder(oldBuilder, newBuilder, mock[NewLogicalPlanSuccessRateMonitor])
    val preparedQuery = PreparedQuery(parser.parse(queryText), queryText, Map.empty)(null, Set.empty, null, null)
    val pipeInfo = mock[PipeInfo]
    when( oldBuilder.producePlan(preparedQuery, planContext, CompilationPhaseTracer.NO_TRACING ) ).thenReturn(Right(pipeInfo))
    when( newBuilder.producePlan(preparedQuery, planContext, CompilationPhaseTracer.NO_TRACING ) ).thenReturn(Right(pipeInfo))

    def result = {
      val plan = pipeBuilder.producePlan(preparedQuery, planContext)
      plan.right.toOption.get
    }

    def assertUsed(used: ExecutablePlanBuilder) = {
      val notUsed = if (used == oldBuilder) newBuilder else oldBuilder
      verify( used ).producePlan(preparedQuery, planContext, CompilationPhaseTracer.NO_TRACING)
      verifyNoMoreInteractions( used )
      verifyZeroInteractions( notUsed )
    }
  }
}
