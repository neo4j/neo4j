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
package org.neo4j.cypher.internal.compiler.v2_2

import org.neo4j.cypher.GraphDatabaseFunSuite
import org.neo4j.cypher.internal.compiler.v2_2.commands.{ShortestPath, SingleNode}
import org.neo4j.cypher.internal.compiler.v2_2.pipes.{FakePipe, PipeMonitor, ShortestPathPipe}
import org.neo4j.cypher.internal.compiler.v2_2.symbols._
import org.neo4j.graphdb.{Direction, Node, Path}

import scala.collection.mutable.Map

class AllShortestPathsPipeTest extends GraphDatabaseFunSuite {

  private implicit val monitor = mock[PipeMonitor]

  def runThroughPipeAndGetPath(a: Node, b: Node) = {
    val source = new FakePipe(List(Map("a" -> a, "b" -> b)), "a" -> CTNode, "b" -> CTNode)

    val pipe = new ShortestPathPipe(source, ShortestPath("p", SingleNode("a"), SingleNode("b"), Seq(), Direction.BOTH, false,
      Some(15), single = false, relIterator = None))()
    graph.inTx(pipe.createResults(QueryStateHelper.empty).toList.map(m => m("p").asInstanceOf[Path]))
  }

  test("should return the shortest path between two nodes") {
    val (a, _, _, d) = createDiamond()

    val resultPaths = runThroughPipeAndGetPath(a, d)

    resultPaths should have size 2

    resultPaths.foreach(resultPath => {
      val number_of_relationships_in_path = resultPath.length()

      number_of_relationships_in_path should equal(2)
      resultPath.startNode() should equal(a)
      resultPath.endNode() should equal(d)
    })
  }
}
