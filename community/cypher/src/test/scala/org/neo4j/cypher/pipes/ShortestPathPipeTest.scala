/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
package org.neo4j.cypher.pipes

import org.junit.Test
import org.scalatest.Assertions
import org.neo4j.cypher.GraphDatabaseTestBase
import org.neo4j.graphdb.Path
class ShortestPathPipeTest extends GraphDatabaseTestBase with Assertions {
  @Test def shouldReturnTheShortestPathBetweenTwoNodes() {
    val a = createNode("a")
    val b = createNode("b")

    val r = relate(a, b, "rel")

    val source = new FakePipe(List(Map("a" -> a, "b" -> b)))

    val pipe = new ShortestPathPipe(source, "p", "a", "b")
    val resultPath = pipe.head("p").asInstanceOf[Path]

    val number_of_relationships_in_path = resultPath.length()

    assert(number_of_relationships_in_path === 1)
    assert(resultPath.lastRelationship() === r)
    assert(resultPath.startNode() === a)
    assert(resultPath.endNode() === b)
  }
}