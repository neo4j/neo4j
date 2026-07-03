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

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.ast.ProcedureResult
import org.neo4j.cypher.internal.ast.ProcedureResultItem
import org.neo4j.cypher.internal.ast.UnresolvedCall
import org.neo4j.cypher.internal.expressions.Add
import org.neo4j.cypher.internal.expressions.AllIterablePredicate
import org.neo4j.cypher.internal.expressions.CachedProperty
import org.neo4j.cypher.internal.expressions.FilterScope
import org.neo4j.cypher.internal.expressions.FunctionInvocation
import org.neo4j.cypher.internal.expressions.HasLabelsOrTypes
import org.neo4j.cypher.internal.expressions.LabelOrRelTypeName
import org.neo4j.cypher.internal.expressions.NODE_TYPE
import org.neo4j.cypher.internal.expressions.NotEquals
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.SignedDecimalIntegerLiteral
import org.neo4j.cypher.internal.runtime.ast.RuntimeConstant
import org.neo4j.cypher.internal.util.FunctionName
import org.neo4j.cypher.internal.util.Namespace
import org.neo4j.cypher.internal.util.ProcedureName
import org.neo4j.cypher.internal.util.ProcedureOutput
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.util.test_helpers.TestName

class ParserTest extends CypherFunSuite with TestName with AstConstructionTestSupport {

  test("a AS b") {
    Parser.Latest.parseProjections(testName) should be(Map("b" -> varFor("a")))
  }

  // Finds cached property
  test("cache[n.prop] AS b") {
    Parser.Latest.parseProjections(testName) should be(Map("b" -> CachedProperty(
      varFor("n"),
      varFor("n"),
      PropertyKeyName("prop")(pos),
      NODE_TYPE
    )(pos)))
  }

  test("b.foo + 5 AS abc09") {
    Parser.Latest.parseProjections(testName) should be(Map("abc09" -> Add(
      Property(varFor("b"), PropertyKeyName("foo")(pos))(pos),
      SignedDecimalIntegerLiteral("5")(pos)
    )(pos)))
  }

  // Finds nested cached property
  test("cache[b.foo] + 5 AS abc09") {
    Parser.Latest.parseProjections(testName) should be(Map("abc09" -> Add(
      CachedProperty(varFor("b"), varFor("b"), PropertyKeyName("foo")(pos), NODE_TYPE)(pos),
      SignedDecimalIntegerLiteral("5")(pos)
    )(pos)))
  }

  test("n:Label") {
    Parser.Latest.parseExpression(testName) should be(HasLabelsOrTypes(
      varFor("n"),
      Seq(LabelOrRelTypeName("Label")(pos))
    )(pos))
  }

  test("`  n@31`") {
    Parser.Latest.parseExpression(testName) should be(varFor("  n@31"))
  }

  test("All(rel in relationships(path) WHERE id(rel) <> 5)") {
    Parser.Latest.parseExpression(testName) should be(
      AllIterablePredicate(
        FilterScope(
          varFor("rel"),
          Some(NotEquals(
            FunctionInvocation(
              FunctionName(Namespace(List())(pos), "id")(pos),
              distinct = false,
              IndexedSeq(varFor("rel"))
            )(pos),
            SignedDecimalIntegerLiteral("5")(pos)
          )(pos))
        )(pos),
        FunctionInvocation(
          FunctionName(Namespace(List())(pos), "relationships")(pos),
          distinct = false,
          IndexedSeq(varFor("path"))
        )(pos)
      )(pos)
    )
  }

  test("RuntimeConstant(v, 5)") {
    Parser.Latest.parseExpression(testName) should be(
      RuntimeConstant(varFor("v"), SignedDecimalIntegerLiteral("5")(pos))
    )
  }

  test("RuntimeConstant(v, datetime($p))") {
    Parser.Latest.parseExpression(testName) match {
      case RuntimeConstant(variable, inner: FunctionInvocation) =>
        variable shouldBe varFor("v")
        inner.functionName.name shouldBe "datetime"
      case other => fail(s"unexpected parse result: $other")
    }
  }

  test("CALL") {
    val expected = UnresolvedCall(
      ProcedureName(Namespace(List("db", "my"))(pos), "proc")(pos),
      Some(Seq(SignedDecimalIntegerLiteral("1")(pos))),
      Some(ProcedureResult(IndexedSeq(
        ProcedureResultItem(None, varFor("foo"))(pos),
        ProcedureResultItem(ProcedureOutput("bar")(pos), varFor("boo"))(pos)
      ))(pos)),
      isStandalone = false
    )(pos)

    Parser.Latest.parseProcedureCall("db.my.proc(1) YIELD foo, bar AS boo") should be(expected)
  }
}
