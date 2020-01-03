/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.logical.builder

import org.neo4j.cypher.internal.v4_0.ast.{ProcedureResult, ProcedureResultItem, UnresolvedCall}
import org.neo4j.cypher.internal.v4_0.expressions._
import org.neo4j.cypher.internal.v4_0.util.InputPosition
import org.neo4j.cypher.internal.v4_0.util.test_helpers.{CypherFunSuite, TestName}

class ParserTest extends CypherFunSuite with TestName
{
  private val pos = InputPosition.NONE

  test("a AS b") {
    Parser.parseProjections(testName) should be(Map("b" -> Variable("a")(pos)))
  }

  // Finds cached property
  test("cache[n.prop] AS b") {
    Parser.parseProjections(testName) should be(Map("b" -> CachedProperty("n", Variable("n")(pos), PropertyKeyName("prop")(pos), NODE_TYPE)(pos)))
  }

  test("b.foo + 5 AS abc09") {
    Parser.parseProjections(testName) should be(Map("abc09" -> Add(Property(Variable("b")(pos), PropertyKeyName("foo")(pos))(pos), SignedDecimalIntegerLiteral("5")(pos))(pos)))
  }

  // Finds nested cached property
  test("cache[b.foo] + 5 AS abc09") {
    Parser.parseProjections(testName) should be(Map("abc09" -> Add(CachedProperty("b", Variable("b")(pos), PropertyKeyName("foo")(pos), NODE_TYPE)(pos), SignedDecimalIntegerLiteral("5")(pos))(pos)))
  }

  test("n:Label") {
    Parser.parseExpression(testName) should be(HasLabels(Variable("n")(pos), Seq(LabelName("Label")(pos)))(pos))
  }

  test("`  n@31`") {
    Parser.parseExpression(testName) should be(Variable("  n@31")(pos))
  }

  test("CALL") {
    val expected = UnresolvedCall(
      Namespace(List("db", "my"))(pos),
      ProcedureName("proc")(pos),
      Some(Seq(SignedDecimalIntegerLiteral("1")(pos))),
      Some(ProcedureResult(IndexedSeq(
        ProcedureResultItem(None, Variable("foo")(pos))(pos),
        ProcedureResultItem(ProcedureOutput("bar")(pos), Variable("boo")(pos))(pos)
      ))(pos))
    )(pos)

    Parser.parseProcedureCall("db.my.proc(1) YIELD foo, bar AS boo") should be(expected)
  }
}
