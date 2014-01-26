/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_1.planner

import org.neo4j.cypher.internal.compiler.v2_1._
import org.scalatest.FunSuite
import org.parboiled.scala._
import org.neo4j.cypher.Direction
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class QueryGraphTest extends FunSuite with parser.Query {

  private def parse(input: String): ast.Query =
    ReportingParseRunner(Query ~ EOI).run(input).result match {
      case Some(ast) => ast
      case None => fail("oh noes!")
    }

  test("simple pattern with two nodes") {
    val astObject = parse("match (a)-->(b) return *")
    val edges = Seq(GraphRelationship(Id(0), Id(1), Direction.OUTGOING, Seq.empty))
    val projection = Seq.empty

    val result = QueryGraphBuilder.build(astObject)

    assert(result === QueryGraph(Id(1), edges, Seq.empty, projection))
  }

  test("simple pattern with three nodes") {
    val astObject = parse("match (a)-->(b)-->(c) return *")
    val edges = Seq(GraphRelationship(Id(0), Id(1), Direction.OUTGOING, Seq.empty), GraphRelationship(Id(1), Id(2), Direction.OUTGOING, Seq.empty))
    val projection = Seq.empty

    val result = QueryGraphBuilder.build(astObject)

    assert(result === QueryGraph(Id(2), edges, Seq.empty, projection))
  }

  test("labeled nodes") {
    val astObject = parse("match (a:Foo)-->(b:Bar) return *")
    val edges = Seq(GraphRelationship(Id(0), Id(1), Direction.OUTGOING, Seq.empty))
    val projection = Seq.empty

    val result = QueryGraphBuilder.build(astObject)

    assert(result === QueryGraph(Id(1), edges, Seq(
      Id(0) -> NodeLabelSelection(Label("Foo")),
      Id(1) -> NodeLabelSelection(Label("Bar"))), projection))
  }
}

