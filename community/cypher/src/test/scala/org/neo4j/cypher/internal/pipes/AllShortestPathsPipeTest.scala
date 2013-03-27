/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.cypher.internal.pipes

import org.junit.Test
import org.scalatest.Assertions
import org.neo4j.cypher.GraphDatabaseTestBase
import org.neo4j.cypher.internal.commands.ShortestPath
import org.neo4j.graphdb.{Direction, Node, Path}
import collection.Traversable
import collection.mutable.Map
import org.neo4j.cypher.internal.symbols.NodeType

class AllShortestPathsPipeTest extends GraphDatabaseTestBase with Assertions {
  def runThroughPipeAndGetPath(a: Node, b: Node) = {
    val source = new FakePipe(List(Map("a" -> a, "b" -> b)), "a" -> NodeType(), "b" -> NodeType())

    val pipe = new ShortestPathPipe(source, ShortestPath("p", "a", "b", Seq(), Direction.BOTH, Some(15), optional = true, single = false, relIterator = None))
    pipe.createResults(QueryStateHelper.empty).toList.map(m => m("p").asInstanceOf[Path])
  }

  @Test def shouldReturnTheShortestPathBetweenTwoNodes() {
    val (a, _, _, d) = createDiamond()

    val resultPaths = runThroughPipeAndGetPath(a, d)

    assert(resultPaths.size === 2)

    resultPaths.foreach(resultPath => {
      val number_of_relationships_in_path = resultPath.length()

      assert(number_of_relationships_in_path === 2)
      assert(resultPath.startNode() === a)
      assert(resultPath.endNode() === d)
    })
  }

  @Test def shouldReturnNullWhenOptional() {
    val a = createNode("a")
    val b = createNode("b")
    // The secret is in what's not there - there is no relationship between a and b

    assert(runThroughPipeAndGetPath(a, b) === List(null))
  }
}