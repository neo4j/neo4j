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
package org.neo4j.cypher.internal.pipes.matching

import org.neo4j.cypher.GraphDatabaseTestBase
import org.scalatest.Assertions
import org.junit.Test
import org.neo4j.graphdb.Direction
import org.neo4j.cypher.internal.commands.True


class JoinerTest extends GraphDatabaseTestBase with Assertions {

  @Test def simpleJoin() {
    val a = createNode()
    val b = createNode()
    val r = relate(a, b, "X")

    val joiner = new Joiner(new Start(Seq("A")), "A", Direction.OUTGOING, "B", Seq(), "r", True())
    assert(joiner.getResult(Map("A" -> a)).toList === List(Map("A" -> a, "B" -> b, "r" -> r)))
  }

  @Test def bothWays() {
    val a = createNode()
    val b = createNode()
    val c = createNode()
    val r1 = relate(a, b, "X")
    val r2 = relate(b, c, "X")

    val joiner = new Joiner(new Start(Seq("A")), "A", Direction.BOTH, "B", Seq(), "r", True())
    val traversable = joiner.getResult(Map("A" -> b))
    assert(traversable.toSet === Set(
      Map("A" -> b, "B" -> a, "r" -> r1),
      Map("A" -> b, "B" -> c, "r" -> r2))
    )
  }

  @Test def twoPatterns() {
    val a = createNode()
    val b = createNode()
    val c = createNode()
    val r1 = relate(a, b, "X")
    val r2 = relate(b, c, "X")

    val joiner = new Joiner(new Start(Seq("A")), "A", Direction.OUTGOING, "B", Seq(), "r", True())
    val joiner2 = new Joiner(joiner, "B", Direction.OUTGOING, "C", Seq(), "r2", True())


    val traversable = joiner2.getResult(Map("A" -> a))
    assert(traversable.toSet === Set(Map("A" -> a, "B" -> b, "r" -> r1, "C" -> c, "r2" -> r2)))
  }

  @Test def twoPatternsBis() {
    val a = createNode()
    val b = createNode()
    val c = createNode()
    val r1 = relate(a, b, "X")
    val r2 = relate(a, c, "X")

    val joiner = new Joiner(new Start(Seq("A")), "A", Direction.OUTGOING, "B", Seq(), "r", True())
    val joiner2 = new Joiner(joiner, "A", Direction.OUTGOING, "C", Seq(), "r2", True())


    val traversable = joiner2.getResult(Map("A" -> a))
    assert(traversable.toList === List(Map("A" -> a, "B" -> b, "C" -> c, "r" -> r1, "r2" -> r2), Map("A" -> a, "B" -> c, "C" -> b, "r" -> r2, "r2" -> r1)))
  }
}