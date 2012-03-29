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
import org.neo4j.cypher.{PathImpl, GraphDatabaseTestBase}
import org.neo4j.graphdb.{Relationship, Node, Direction}
import org.junit.{Before, Test}
import org.neo4j.cypher.internal.commands.{True, Pattern, NamedPath, VarLengthRelatedTo}
import collection.mutable.Map

class NamedPathPipeTest extends GraphDatabaseTestBase with Assertions {
  var a: Node = null
  var b: Node = null
  var c: Node = null
  var r1: Relationship = null
  var r2: Relationship = null
  var pattern: Pattern = null

  @Before def init() {
    pattern = VarLengthRelatedTo("x", "a", "b", None, None, Seq(), Direction.BOTH, None, false, True())
    a = createNode("a")
    b = createNode("b")
    c = createNode("c")
    r1 = relate(a, b, "R")
    r2 = relate(b, c, "R")
  }


  @Test def pathsAreExtractedCorrectly() {
    val p = PathImpl(a, r1, b, r2, c)

    val inner = new FakePipe(Seq(Map("a" -> a, "b" -> b, "x" -> p)))
    val pipe = new NamedPathPipe(inner, NamedPath("p", pattern))

    assert(pipe.createResults(Map()) === List(Map("a" -> a, "b" -> b, "x" -> p, "p" -> p)))
  }

  @Test def pathsAreTurnedRightSideAround() {
    val p = PathImpl(c, r2, b, r1, a)

    val inner = new FakePipe(Seq(Map("a" -> a, "b" -> b, "x" -> p)))
    val pipe = new NamedPathPipe(inner, NamedPath("p", pattern))

    assert(pipe.createResults(Map()) === List(Map("a" -> a, "b" -> b, "x" -> p, "p" -> PathImpl(a, r1, b, r2, c))))
  }
}