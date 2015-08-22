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

import org.neo4j.collection.primitive.Primitive
import org.neo4j.cypher.internal.compiler.v2_3.symbols._
import org.neo4j.cypher.internal.semantics.v2_3.test_helpers.CypherFunSuite
import org.neo4j.graphdb.Node

class TriadicProbePipeTest extends CypherFunSuite {
  private implicit val monitor = mock[PipeMonitor]

  test("build from input") {
    // given
    val input = createFakePipeWith(0 -> List(1, 2, 3, 4, 5))
    val pipe = TriadicProbePipe(input, "foo", "x", "a")()
    val queryState = QueryStateHelper.empty
    val set = Primitive.longSet()
    set.add(2)
    set.add(3)
    set.add(4)
    queryState.triadicState.update("x", set)

    // when
    val result = pipe.createResults(queryState).map(m => m("a").asInstanceOf[Node].getId).toList

    // then we don't see nodes in the TriadicSet
    result should equal(List(1, 5))
  }


  private def createFakePipeWith(data: (Int, List[Any])*): FakePipe = {
    import org.mockito.Mockito.when

    def nodeWithId(id: Long) = {
      val n = mock[Node]
      when(n.getId).thenReturn(id)
      n
    }

    val in = data.flatMap {
      case (x, related) =>
        related.map {
          case a: Int => Map("a" -> nodeWithId(a), "foo" -> nodeWithId(x))
          case null => Map("a" -> null, "foo" -> nodeWithId(x))
        }
    }

    new FakePipe(in, "x" -> CTNode, "a" -> CTNode)
  }
}
