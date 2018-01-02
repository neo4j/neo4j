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
package org.neo4j.cypher.internal.compiler.v2_3.pipes

import org.neo4j.cypher.GraphDatabaseFunSuite
import org.neo4j.cypher.internal._
import org.neo4j.cypher.internal.compiler.v2_3._
import org.neo4j.cypher.internal.compiler.v2_3.commands.expressions
import org.neo4j.cypher.internal.frontend.v2_3.SemanticDirection
import org.neo4j.graphdb._

class NamedPathPipeTest extends GraphDatabaseFunSuite {
  private implicit val monitor = mock[PipeMonitor]

  private var aNode: Node = null
  private var bNode: Node = null
  private var cNode: Node = null
  private var r1: Relationship = null
  private var r2: Relationship = null
  private var p: Path = null
  private val varLengthPath = ParsedVarLengthRelation(
    name = "x",
    props = Map.empty, start = ParsedEntity("a"),
    end = ParsedEntity("c"),
    typ = Seq.empty,
    dir = SemanticDirection.OUTGOING,
    optional = false,
    minHops = None,
    maxHops = None,
    relIterator = None
  )
  private val singleRelationship = ParsedRelation("r1", "a", "b", Seq.empty, SemanticDirection.OUTGOING)

  override def beforeEach() {
    super.beforeEach()

    // x = a-->b-->c
    aNode = createNode("a")
    bNode = createNode("b")
    cNode = createNode("c")
    r1 = relate(aNode, bNode, "R")
    r2 = relate(bNode, cNode, "R")
    p = expressions.PathImpl(Seq(aNode, r1, bNode, r2, cNode): _*)
  }

  test("single node path") {
    val inputPipe = new FakePipe(Seq(Map("a" -> aNode, "r1" -> r1, "b" -> bNode, "r2" -> r2, "c" -> cNode, "x" -> p)))

    createNamedPath(inputPipe, ParsedEntity("a")) should equal(Seq(aNode))
  }

  test("test single relationship") {
    val inputPipe = new FakePipe(Seq(Map("a" -> aNode, "r1" -> r1, "b" -> bNode, "r2" -> r2, "c" -> cNode, "x" -> p)))

    createNamedPath(inputPipe, singleRelationship) should equal(Seq(aNode, r1, bNode))
  }

  test("test varlength path") {
    val inputPipe = new FakePipe(Seq(Map("a" -> aNode, "r1" -> r1, "b" -> bNode, "r2" -> r2, "c" -> cNode, "x" -> p)))

    createNamedPath(inputPipe, varLengthPath) should equal(Seq(aNode, r1, bNode, r2, cNode))
  }

  test("optional varlength path") {
    val inputPipe = new FakePipe(Seq(Map("a" -> aNode, "r1" -> r1, "b" -> bNode, "r2" -> r2, "c" -> cNode, "x" -> null)))

    createNamedPath(inputPipe, varLengthPath.copy(optional = true)) should be(null.asInstanceOf[Any])
  }

  test("paths are turned right side around") {
    // MATCH p = a-[*]->c
    val p = expressions.PathImpl(cNode, r2, bNode, r1, aNode)
    val inputPipe = new FakePipe(Seq(Map("a" -> aNode, "c" -> cNode, "x" -> p)))

    createNamedPath(inputPipe, varLengthPath) should equal(Seq(aNode, r1, bNode, r2, cNode))
  }

  test("paths are turned right side around 2") {
    // MATCH p = a-[r1]->b-[*]->c
    val p = expressions.PathImpl(cNode, r2, bNode)
    val inputPipe = new FakePipe(Seq(Map("a" -> aNode, "r1" -> r1, "b" -> bNode, "c" -> cNode, "x" -> p)))

    createNamedPath(inputPipe, singleRelationship, varLengthPath.copy(start = ParsedEntity("b"))) should equal(Seq(aNode, r1, bNode, r2, cNode))
  }

  private def createNamedPath(inputPipe: Pipe, patterns: AbstractPattern*): Any = {
    val pipe = new NamedPathPipe(inputPipe, "p", patterns)
    val results = pipe.createResults(QueryStateHelper.empty).toList

    Option(results.head("p")).map(_.asInstanceOf[expressions.PathImpl].pathEntities).orNull
  }
}
