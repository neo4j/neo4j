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
package org.neo4j.cypher.internal.compiler.v2_2.executionplan

import org.mockito.Mockito._
import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_2.PreparedQuery
import org.neo4j.cypher.internal.compiler.v2_2.ast.Statement
import org.neo4j.cypher.internal.compiler.v2_2.parser.{CypherParser, ParserMonitor}
import org.neo4j.cypher.internal.compiler.v2_2.spi.PlanContext

class LegacyVsNewPipeBuilderTest extends CypherFunSuite {

  val parser = new CypherParser(mock[ParserMonitor[Statement]])

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
    val oldBuilder = mock[PipeBuilder]
    val newBuilder = mock[PipeBuilder]
    val pipeBuilder = new LegacyVsNewPipeBuilder(oldBuilder, newBuilder, mock[NewLogicalPlanSuccessRateMonitor])
    val preparedQuery = PreparedQuery(parser.parse(queryText), queryText, Map.empty)(null, Set.empty, null)
    val pipeInfo = mock[PipeInfo]
    when( oldBuilder.producePlan(preparedQuery, planContext ) ).thenReturn(pipeInfo)
    when( newBuilder.producePlan(preparedQuery, planContext ) ).thenReturn(pipeInfo)

    def result = pipeBuilder.producePlan(preparedQuery, planContext)

    def assertUsed(used: PipeBuilder) = {
      val notUsed = if (used == oldBuilder) newBuilder else oldBuilder
      verify( used ).producePlan(preparedQuery, planContext)
      verifyNoMoreInteractions( used )
      verifyZeroInteractions( notUsed )
    }
  }
}
