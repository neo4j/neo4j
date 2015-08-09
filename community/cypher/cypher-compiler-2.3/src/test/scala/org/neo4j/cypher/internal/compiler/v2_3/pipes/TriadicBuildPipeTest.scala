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
package org.neo4j.cypher.internal.compiler.v2_3.pipes

import org.neo4j.cypher.internal.compiler.v2_3.symbols._
import org.neo4j.cypher.internal.compiler.v2_3.test_helpers.CypherFunSuite
import org.neo4j.graphdb.Node

import scala.collection.JavaConverters._

class TriadicBuildPipeTest extends CypherFunSuite {
  private implicit val monitor = mock[PipeMonitor]

  test("build from input") {
    val input = createFakePipeWith(0, 1, 2, 3, 4, 5)
    val pipe = TriadicBuildPipe(input, "a")()
    val queryState = QueryStateHelper.empty
    pipe.createResults(queryState).map(ctx => ctx("a"))

    queryState.triadicSets("a").asScala should equal(Set(0, 1, 2, 3, 4, 5))
  }

  test("build from input with doubles") {
    val input = createFakePipeWith(0, 1, 2, 3, 4, 5, 0, 1, 2, 3, 4, 5)
    val pipe = TriadicBuildPipe(input, "a")()
    val queryState = QueryStateHelper.empty
    pipe.createResults(queryState).map(ctx => ctx("a"))

    queryState.triadicSets("a").asScala should equal(Set(0, 1, 2, 3, 4, 5))
  }

  test("build ignores nulls") {
    val input = createFakePipeWith(0, 2, 3, null)
    val pipe = TriadicBuildPipe(input, "a")()
    val queryState = QueryStateHelper.empty
    pipe.createResults(queryState).map(ctx => ctx("a"))

    queryState.triadicSets("a").asScala should equal(Set(0, 2, 3))
  }

  private def createFakePipeWith(count: Any*): FakePipe = {
    import org.mockito.Mockito.when

    def nodeWithId(id: Long) = {
      val n = mock[Node]
      when(n.getId).thenReturn(id)
      n
    }

    val in = count.map {
      case i: Int => Map("a" -> nodeWithId(i))
      case null => Map("a" -> null)
    }

    new FakePipe(in, "x" -> CTNode)
  }
}
