/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.logical.builder

import org.neo4j.cypher.internal.ast.ProcedureResult
import org.neo4j.cypher.internal.ast.ProcedureResultItem
import org.neo4j.cypher.internal.ast.UnresolvedCall
import org.neo4j.cypher.internal.expressions.Add
import org.neo4j.cypher.internal.expressions.AllIterablePredicate
import org.neo4j.cypher.internal.expressions.CachedProperty
import org.neo4j.cypher.internal.expressions.FilterScope
import org.neo4j.cypher.internal.expressions.FunctionInvocation
import org.neo4j.cypher.internal.expressions.FunctionName
import org.neo4j.cypher.internal.expressions.HasLabelsOrTypes
import org.neo4j.cypher.internal.expressions.LabelOrRelTypeName
import org.neo4j.cypher.internal.expressions.NODE_TYPE
import org.neo4j.cypher.internal.expressions.Namespace
import org.neo4j.cypher.internal.expressions.NotEquals
import org.neo4j.cypher.internal.expressions.ProcedureName
import org.neo4j.cypher.internal.expressions.ProcedureOutput
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.SignedDecimalIntegerLiteral
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.util.test_helpers.TestName

class ParserTest extends CypherFunSuite with TestName {
  private val pos = InputPosition.NONE

  test("a AS b") {
    Parser.parseProjections(testName) should be(Map("b" -> Variable("a")(pos)))
  }

  // Finds cached property
  test("cache[n.prop] AS b") {
    Parser.parseProjections(testName) should be(Map("b" -> CachedProperty(
      Variable("n")(pos),
      Variable("n")(pos),
      PropertyKeyName("prop")(pos),
      NODE_TYPE
    )(pos)))
  }

  test("b.foo + 5 AS abc09") {
    Parser.parseProjections(testName) should be(Map("abc09" -> Add(
      Property(Variable("b")(pos), PropertyKeyName("foo")(pos))(pos),
      SignedDecimalIntegerLiteral("5")(pos)
    )(pos)))
  }

  // Finds nested cached property
  test("cache[b.foo] + 5 AS abc09") {
    Parser.parseProjections(testName) should be(Map("abc09" -> Add(
      CachedProperty(Variable("b")(pos), Variable("b")(pos), PropertyKeyName("foo")(pos), NODE_TYPE)(pos),
      SignedDecimalIntegerLiteral("5")(pos)
    )(pos)))
  }

  test("n:Label") {
    Parser.parseExpression(testName) should be(HasLabelsOrTypes(
      Variable("n")(pos),
      Seq(LabelOrRelTypeName("Label")(pos))
    )(pos))
  }

  test("`  n@31`") {
    Parser.parseExpression(testName) should be(Variable("  n@31")(pos))
  }

  test("All(rel in relationships(path) WHERE id(rel) <> 5)") {
    Parser.parseExpression(testName) should be(
      AllIterablePredicate(
        FilterScope(
          Variable("rel")(pos),
          Some(NotEquals(
            FunctionInvocation(
              FunctionName(Namespace(List())(pos), "id")(pos),
              distinct = false,
              IndexedSeq(Variable("rel")(pos))
            )(pos),
            SignedDecimalIntegerLiteral("5")(pos)
          )(pos))
        )(pos),
        FunctionInvocation(
          FunctionName(Namespace(List())(pos), "relationships")(pos),
          distinct = false,
          IndexedSeq(Variable("path")(pos))
        )(pos)
      )(pos)
    )
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
