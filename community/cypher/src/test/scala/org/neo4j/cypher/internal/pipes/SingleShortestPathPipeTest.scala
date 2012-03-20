/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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

import org.scalatest.Assertions
import org.neo4j.cypher.GraphDatabaseTestBase
import org.neo4j.graphdb.{Direction, Node, Path}
import org.neo4j.cypher.internal.commands._
import org.junit.{Ignore, Test}
import collection.mutable.Map

class SingleShortestPathPipeTest extends GraphDatabaseTestBase with Assertions {

  val path = ShortestPath("p", "a", "b", None, Direction.BOTH, Some(15), optional = true, single = true, None, True())

  def runThroughPipeAndGetPath(a: Node, b: Node, path: ShortestPath): Path = {
    val source = new FakePipe(List(Map("a" -> a, "b" -> b)))


    val pipe = new SingleShortestPathPipe(source, path)
    pipe.createResults(Map()).head("p").asInstanceOf[Path]
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

  @Test def shouldReturnNullWhenOptional() {
    val a = createNode("a")
    val b = createNode("b")
    // The secret is in what's not there - there is no relationship between a and b

    assert(runThroughPipeAndGetPath(a, b, path) === null)
  }

  @Ignore
  @Test def shouldReturnLongerPathIfShorterDoesntMatchPredicate() {
    // Two paths exist: a->b->c, and a->c
    // The shorter is not a match, because of a property
    // on the relationship.
    
    val a = createNode()
    val b = createNode()
    val c = createNode()


    relate(a, b, "rel", Map("foo" -> "bar"))
    relate(b, c, "rel", Map("foo" -> "bar"))

    relate(a, c, "rel", Map("foo" -> "notBar"))

    val pred = AllInIterable(RelationshipFunction(Entity("p")), "r", Equals(Property("r", "foo"), Literal("bar")))
    val path = ShortestPath("p", "a", "b", None, Direction.OUTGOING, None, false, true, Some("r"), pred)


    val result = runThroughPipeAndGetPath(a, c, path)
    assert(2 === result.length())
  }
}