/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_3

import org.neo4j.cypher.GraphDatabaseFunSuite
import org.neo4j.cypher.internal.compiler.v2_3.commands._
import org.neo4j.cypher.internal.compiler.v2_3.pipes.{FakePipe, PipeMonitor, ShortestPathPipe}
import org.neo4j.cypher.internal.frontend.v2_3.SemanticDirection
import org.neo4j.cypher.internal.frontend.v2_3.symbols._
import org.neo4j.graphdb.{Node, Path}

class SingleShortestPathPipeTest extends GraphDatabaseFunSuite {
  private implicit val monitor = mock[PipeMonitor]
  private val path = ShortestPath("p", SingleNode("a"), SingleNode("b"), Seq(), SemanticDirection.BOTH, false, Some(15), single = true, relIterator = None)

  test("should return the shortest path between two nodes") {
    val a = createNode("a")
    val b = createNode("b")

    val r = relate(a, b, "rel")

    val resultPath = runThroughPipeAndGetPath(a, b, path)

    val number_of_relationships_in_path = resultPath.length()

    number_of_relationships_in_path should equal(1)
    resultPath.lastRelationship() should equal(r)
    resultPath.startNode() should equal(a)
    resultPath.endNode() should equal(b)
  }

  private def runThroughPipeAndGetPath(a: Node, b: Node, path: ShortestPath): Path = {
    val source = new FakePipe(List(Map("a" -> a, "b" -> b)), "a"->CTNode, "b"->CTNode)

    val pipe = new ShortestPathPipe(source, path)()
    graph.withTx(tx => pipe.createResults(QueryStateHelper.queryStateFrom(graph, tx)).next()("p").asInstanceOf[Path])
  }
}
