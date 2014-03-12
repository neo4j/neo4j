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

import org.neo4j.cypher.internal.compiler.v2_1.planner.{QueryGraph, Selections, SimpleQueryGraphBuilder}
import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_1.ast._
import org.neo4j.cypher.internal.compiler.v2_1.parser.{ParserMonitor, CypherParser}
import org.neo4j.cypher.internal.compiler.v2_1.DummyPosition
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.IdName

class SimpleQueryGraphBuilderTest extends CypherFunSuite {

  // TODO: we may want to have normalized queries instead that simply parsed queries
  val parser = new CypherParser(mock[ParserMonitor])
  val pos = DummyPosition(0)

  val builder = new SimpleQueryGraphBuilder
  def buildQueryGraph(query: String): QueryGraph = {
    val ast = parser.parse(query).asInstanceOf[Query]
    builder.produce(ast)
  }

  test("projection only query") {
    val qg = buildQueryGraph("RETURN 42")
    qg.projections should equal(Map("42" -> SignedIntegerLiteral("42")(pos)))
  }

  test("multiple projection query") {
    val qg = buildQueryGraph("RETURN 42, 'foo'")
    qg.projections should equal(Map(
      "42" -> SignedIntegerLiteral("42")(pos),
      "'foo'" -> StringLiteral("foo")(pos)
    ))
  }

  test("match n return n") {
    val qg = buildQueryGraph("MATCH n RETURN n")
    qg.projections should equal(Map(
      "n" -> Identifier("n")(pos)
    ))

    qg.identifiers should equal(Set(IdName("n")))
  }

  test("match n where n:Awesome return n") {
    val qg = buildQueryGraph("MATCH n WHERE n:Awesome:Foo RETURN n")

    qg.projections should equal(Map(
      "n" -> Identifier("n")(pos)
    ))

    qg.selections should equal(Selections(Seq(
      Set(IdName("n")) -> HasLabels(Identifier("n")(pos), Seq(LabelName("Awesome")()(pos)))(pos),
      Set(IdName("n")) -> HasLabels(Identifier("n")(pos), Seq(LabelName("Foo")()(pos)))(pos)
    )))

    qg.identifiers should equal(Set(IdName("n")))
  }

  test("match n where id(n) = 42 return n") {
    val qg = buildQueryGraph("MATCH n WHERE id(n) = 42 RETURN n")

    qg.projections should equal(Map(
      "n" -> Identifier("n")(pos)
    ))

    qg.selections should equal(Selections(List(
      Set(IdName("n")) -> Equals(
        FunctionInvocation(Identifier("id")(pos), distinct = false, Vector(Identifier("n")(pos)))(pos),
        SignedIntegerLiteral("42")(pos)
      )(pos)
    )))

    qg.identifiers should equal(Set(IdName("n")))
  }

  test("match n where n:Label and id(n) = 42 return n") {
    val qg = buildQueryGraph("MATCH n WHERE n:Label AND id(n) = 42 RETURN n")
    qg.projections should equal(Map(
      "n" -> Identifier("n")(pos)
    ))

    qg.selections should equal(Selections(List(
      Set(IdName("n")) -> HasLabels(Identifier("n")(pos), Seq(LabelName("Label")()(pos)))(pos),
      Set(IdName("n")) -> Equals(
        FunctionInvocation(Identifier("id")(pos), distinct = false, Vector(Identifier("n")(pos)))(pos),
        SignedIntegerLiteral("42")(pos)
      )(pos)
    )))

    qg.identifiers should equal(Set(IdName("n")))
  }
}
