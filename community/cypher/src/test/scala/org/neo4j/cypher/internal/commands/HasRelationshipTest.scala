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
package org.neo4j.cypher.internal.commands

import expressions.Identifier
import org.neo4j.cypher.GraphDatabaseTestBase
import org.scalatest.Assertions
import org.junit.{Before, Test}
import org.neo4j.graphdb.{Node, Direction}
import org.junit.Assert._
import org.neo4j.cypher.internal.spi.gdsimpl.TransactionBoundQueryContext
import org.neo4j.cypher.internal.pipes.{QueryStateHelper, NullDecorator, QueryState}
import org.neo4j.cypher.internal.ExecutionContext


class HasRelationshipTest extends GraphDatabaseTestBase with Assertions {
  var a: Node = null
  var b: Node = null
  var ctx:ExecutionContext=null
  val aValue = Identifier("a")
  val bValue = Identifier("b")
  var state: QueryState = null

  @Before
  def init() {
    a = createNode()
    b = createNode()
    ctx = ExecutionContext().newWith(Map("a" -> a, "b" -> b))
    state = QueryStateHelper.queryStateFrom(graph)
  }

  def createPredicate(dir: Direction, relType: Seq[String]): HasRelationshipTo = HasRelationshipTo(Identifier("a"), Identifier("b"), dir, relType)

  @Test def givenTwoRelatedNodesThenReturnsTrue() {
    relate(a, b)

    val predicate = createPredicate(Direction.BOTH, Seq())

    assertTrue("Expected the predicate to return true, but it didn't", predicate.isMatch(ctx)(state))
  }

  @Test def checksTheRelationshipType() {
    relate(a, b, "KNOWS")

    val predicate = createPredicate(Direction.BOTH, Seq("FEELS"))

    assertFalse("Expected the predicate to return false, but it didn't", predicate.isMatch(ctx)(state))
  }

  @Test def checksTheRelationshipTypeAndDirection() {
    relate(a, b, "KNOWS")

    val predicate = createPredicate(Direction.INCOMING, Seq("KNOWS"))

    assertFalse("Expected the predicate to return false, but it didn't", predicate.isMatch(ctx)(state))
  }
}