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
package org.neo4j.cypher.internal.compiler.v2_0.pipes

import org.neo4j.cypher.internal.compiler.v2_0._
import org.neo4j.cypher.internal._
import org.neo4j.cypher.GraphDatabaseJUnitSuite
import org.neo4j.graphdb._
import org.junit.{Before, Test}

class NamedPathPipeTest extends GraphDatabaseJUnitSuite {
  var a: Node = null
  var b: Node = null
  var c: Node = null
  var r1: Relationship = null
  var r2: Relationship = null
  var p: Path = null
  var inputPipe: Pipe = null
  val varLengthPath = ParsedVarLengthRelation(name = "x", props = Map.empty, start = ParsedEntity("a"),
    end = ParsedEntity("c"), typ = Seq.empty, dir = Direction.OUTGOING, optional = false, minHops = None,
    maxHops = None, relIterator = None)
  val singleRelationship = ParsedRelation("r1", "a", "b", Seq.empty, Direction.OUTGOING)
  var pathElements: Seq[PropertyContainer] = null

  @Before
  def init() {
    // x = a-->b-->c

    a = createNode("a")
    b = createNode("b")
    c = createNode("c")
    r1 = relate(a, b, "R")
    r2 = relate(b, c, "R")
    pathElements = Seq(a, r1, b, r2, c)
    p = PathImpl(pathElements: _*)
    inputPipe = new FakePipe(Seq(Map("a" -> a, "r1" -> r1, "b" -> b, "r2" -> r2, "c" -> c, "x" -> p)))
  }

  @Test
  def singleNodePath() {
    assert(createNamedPath(ParsedEntity("a")) === Seq(a))
  }

  @Test
  def testSingleRelationship() {
    assert(createNamedPath(singleRelationship) === Seq(a, r1, b))
  }

  @Test
  def testVarlengthPath() {
    assert(createNamedPath(varLengthPath) === pathElements)
  }

  @Test
  def optionalVarlengthPath() {
    inputPipe = new FakePipe(Seq(Map("a" -> a, "r1" -> r1, "b" -> b, "r2" -> r2, "c" -> c, "x" -> null)))

    assert(createNamedPath(varLengthPath.copy(optional = true)) === null)
  }

  @Test
  def pathsAreTurnedRightSideAround() {
    // MATCH p = a-[*]->c
    val p = PathImpl(c, r2, b, r1, a)

    inputPipe = new FakePipe(Seq(Map("a" -> a, "c" -> c, "x" -> p)))
    assert(createNamedPath(varLengthPath) === pathElements)
  }

  @Test
  def pathsAreTurnedRightSideAround2() {
    // MATCH p = a-[r1]->b-[*]->c
    val p = PathImpl(c, r2, b)

    inputPipe = new FakePipe(Seq(Map("a" -> a, "r1" -> r1, "b" -> b, "c" -> c, "x" -> p)))

    assert(createNamedPath(singleRelationship, varLengthPath.copy(start = ParsedEntity("b"))) === pathElements)
  }

  private def createNamedPath(patterns: AbstractPattern*): Any = {
    val pipe = new NamedPathPipe(inputPipe, "p", patterns)
    val results = pipe.createResults(QueryStateHelper.empty).toList

    Option(results.head("p")).
      map(_.asInstanceOf[PathImpl].pathEntities).
      getOrElse(null)
  }
}
