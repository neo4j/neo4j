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

import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_1.ast._
import org.neo4j.cypher.internal.compiler.v2_1.parser.CypherParser
import org.neo4j.cypher.internal.compiler.v2_1.DummyPosition
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.Id

class SimpleQueryGraphBuilderTest extends CypherFunSuite {

  val parser = new CypherParser()

  test("projection only query") {
    val ast = parse("RETURN 42")
    val builder = new SimpleQueryGraphBuilder
    val qg = builder.produce(ast)
    qg.projection should equal(Seq("42" -> SignedIntegerLiteral("42")(DummyPosition(0))))
  }

  test("multiple projection query") {
    val ast = parse("RETURN 42, 'foo'")
    val builder = new SimpleQueryGraphBuilder
    val qg = builder.produce(ast)
    qg.projection should equal(Seq(
      "42" -> SignedIntegerLiteral("42")(DummyPosition(0)),
      "'foo'" -> StringLiteral("foo")(DummyPosition(0))
    ))
  }

  test("match n return n") {
    val ast = parse("MATCH n RETURN n")
    val builder = new SimpleQueryGraphBuilder
    val qg = builder.produce(ast)

    qg.projection should equal(Seq(
      "n" -> Identifier("n")(DummyPosition(0))
    ))

    qg.identifiers should equal(Set(Id("n")))
  }

  def parse(s: String): Query =
    parser.parse(s).asInstanceOf[Query]

}