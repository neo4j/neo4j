/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.cypher.internal.ast.factory.neo4j

import org.neo4j.cypher.internal.expressions.NodePattern
import org.neo4j.cypher.internal.util.OpenCypherExceptionFactory
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.util.test_helpers.TestName

class NodePatternPredicateParserTest extends CypherFunSuite with TestName with JavaccParserAstTestBase[NodePattern] {

  implicit val parser: JavaccRule[NodePattern] = JavaccRule.NodePattern

  for {
    (maybeLabelExpression, maybeLabelExpressionAst) <-
      Seq(("", None), (":Foo|Bar", Some(labelDisjunction(labelLeaf("Foo"), labelLeaf("Bar")))))
    (maybeProperties, maybePropertiesAst) <-
      Seq(("", None), ("{prop: 'test'}", Some(mapOf("prop" -> literalString("test")))))
  } yield {

    test(s"MATCH (n$maybeLabelExpression $maybeProperties WHERE n.otherProp > 123)") {
      parseNodePatterns(testName) shouldBe Seq(
        nodePat(
          Some("n"),
          maybeLabelExpressionAst,
          maybePropertiesAst,
          Some(greaterThan(prop("n", "otherProp"), literalInt(123)))
        )
      )
    }

    test(s"MATCH ($maybeLabelExpression $maybeProperties WHERE true)") {
      parseNodePatterns(testName) shouldBe Seq(
        nodePat(
          None,
          maybeLabelExpressionAst,
          maybePropertiesAst,
          Some(trueLiteral)
        )
      )
    }
  }

  test("MATCH (n WHERE n.prop > 123)") {
    val expected = Seq(
      nodePat(
        Some("n"),
        predicates = Some(greaterThan(prop("n", "prop"), literalInt(123)))
      )
    )
    parseNodePatterns(testName) shouldBe expected
    parseNodePatterns(testName.replaceAllLiterally("WHERE", "wHeRe")) shouldBe expected
  }

  test("(n:A:B:C {prop: 42} WHERE n.otherProp < 123)") {
    givesIncludingPositions {
      NodePattern(
        variable = Some(varFor("n", (1, 2, 1))),
        labelExpression = Some(
          labelColonConjunction(
            labelColonConjunction(
              labelLeaf("A", (1, 4, 3)),
              labelLeaf("B", (1, 6, 5))
            ),
            labelLeaf("C", (1, 8, 7))
          )
        ),
        properties = Some(mapOf("prop" -> literalInt(42))),
        predicate = Some(lessThan(prop("n", "otherProp"), literalInt(123)))
      )((1, 1, 0))
    }
  }

  test("MATCH (WHERE)") {
    parseNodePatterns(testName) shouldBe Seq(nodePat(Some("WHERE")))
  }

  /* This case is ambiguous from a language standpoint, it could be either
   * 1. an inlined WHERE clause with a map expression (which would fail in semantic checking as WHERE expects a boolean expression)
   * 2. a node named WHERE with a property map
   * As the second case is not just syntactically but also semantically correct, the parser has been programmed to prefer it.
   */
  test("MATCH (WHERE {prop: 123})") {
    parseNodePatterns(testName) shouldBe Seq(
      nodePat(Some("WHERE"), properties = Some(mapOf("prop" -> literal(123))))
    )
  }

  test("MATCH (WHERE WHERE {prop: 123})") {
    parseNodePatterns(testName) shouldBe Seq(
      nodePat(Some("WHERE"), predicates = Some(mapOf("prop" -> literal(123))))
    )
  }

  test("MATCH (WHERE {prop: 123} WHERE {prop: 123})") {
    parseNodePatterns(testName) shouldBe Seq(
      nodePat(
        Some("WHERE"),
        properties = Some(mapOf("prop" -> literal(123))),
        predicates = Some(mapOf("prop" -> literal(123)))
      )
    )
  }

  test("MATCH (WHERE WHERE WHERE.prop > 123)") {
    parseNodePatterns(testName) shouldBe Seq(
      nodePat(
        Some("WHERE"),
        predicates = Some(greaterThan(prop("WHERE", "prop"), literalInt(123)))
      )
    )
  }

  test("MATCH (WHERE WHERE.WHERE='WHERE')") {
    parseNodePatterns(testName) shouldBe Seq(
      nodePat(predicates = Some(equals(prop("WHERE", "WHERE"), literalString("WHERE"))))
    )
  }

  test("RETURN [(n:A WHERE n.prop >= 123)-->(end WHERE end.prop < 42) | n]") {
    parseNodePatterns(testName).toSet shouldBe Set(
      nodePat(
        Some("n"),
        Some(labelLeaf("A")),
        predicates = Some(greaterThanOrEqual(prop("n", "prop"), literalInt(123)))
      ),
      nodePat(
        Some("end"),
        predicates = Some(lessThan(prop("end", "prop"), literalInt(42)))
      )
    )
  }

  test("RETURN exists((n {prop: 'test'} WHERE n.otherProp = 123)-->(end WHERE end.prop = 42)) AS result") {
    parseNodePatterns(testName).toSet shouldBe Set(
      nodePat(
        Some("n"),
        properties = Some(mapOf("prop" -> literalString("test"))),
        predicates = Some(equals(prop("n", "otherProp"), literalInt(123)))
      ),
      nodePat(
        Some("end"),
        predicates = Some(equals(prop("end", "prop"), literalInt(42)))
      )
    )
  }

  private val exceptionFactory = OpenCypherExceptionFactory(None)

  private def parseNodePatterns(query: String): Seq[NodePattern] = {
    val ast = JavaCCParser.parse(query, exceptionFactory)
    ast.folder.findAllByClass[NodePattern]
  }
}
