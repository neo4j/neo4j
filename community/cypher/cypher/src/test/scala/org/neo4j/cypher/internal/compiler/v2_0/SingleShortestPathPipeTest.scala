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
package org.neo4j.cypher.internal.compiler.v2_0

import commands._
import pipes.{ShortestPathPipe, FakePipe}
import symbols._
import org.neo4j.cypher.GraphDatabaseJUnitSuite
import org.neo4j.graphdb.{Direction, Node, Path}
import org.junit.Test
import collection.mutable.Map

class SingleShortestPathPipeTest extends GraphDatabaseJUnitSuite {

  val path = ShortestPath("p", SingleNode("a"), SingleNode("b"), Seq(), Direction.BOTH, Some(15), single = true, relIterator = None)

  def runThroughPipeAndGetPath(a: Node, b: Node, path: ShortestPath): Path = {
    val source = new FakePipe(List(Map("a" -> a, "b" -> b)), "a"->CTNode, "b"->CTNode)

    val pipe = new ShortestPathPipe(source, path)
    graph.inTx(pipe.createResults(QueryStateHelper.empty).next()("p").asInstanceOf[Path])
  }

  @Test def shouldReturnTheShortestPathBetweenTwoNodes() {
    val a = createNode("a")
    val b = createNode("b")

    val r = relate(a, b, "rel")

    val resultPath = runThroughPipeAndGetPath(a, b, path)

    val number_of_relationships_in_path = resultPath.length()

    assert(number_of_relationships_in_path === 1)
    assert(resultPath.lastRelationship() === r)
    assert(resultPath.startNode() === a)
    assert(resultPath.endNode() === b)
  }
}
