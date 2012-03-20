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
package org.neo4j.cypher.internal.commands

import org.neo4j.cypher.GraphDatabaseTestBase
import org.scalatest.Assertions
import org.junit.{Before, Test}
import org.neo4j.graphdb.{Node, Direction}
import org.junit.Assert._


class HasRelationshipTest extends GraphDatabaseTestBase with Assertions {
  var a: Node = null
  var b: Node = null
  val aValue = Entity("a")
  val bValue = Entity("b")

  @Before
  def init() {
    a = createNode()
    b = createNode()
  }

  def createPredicate(dir: Direction, relType: Seq[String]): HasRelationshipTo = HasRelationshipTo(Entity("a"), Entity("b"), dir, relType)

  @Test def givenTwoRelatedNodesThenReturnsTrue() {
    relate(a, b)

    val predicate = createPredicate(Direction.BOTH, Seq())

    assertTrue("Expected the predicate to return true, but it didn't", predicate.isMatch(Map("a" -> a, "b" -> b)))
  }

  @Test def checksTheRelationshipType() {
    relate(a, b, "KNOWS")

    val predicate = createPredicate(Direction.BOTH, Seq("FEELS"))

    assertFalse("Expected the predicate to return false, but it didn't", predicate.isMatch(Map("a" -> a, "b" -> b)))
  }

  @Test def checksTheRelationshipTypeAndDirection() {
    relate(a, b, "KNOWS")

    val predicate = createPredicate(Direction.INCOMING, Seq("KNOWS"))

    assertFalse("Expected the predicate to return false, but it didn't", predicate.isMatch(Map("a" -> a, "b" -> b)))
  }
}