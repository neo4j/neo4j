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
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.OpenCypherExceptionFactory
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.util.test_helpers.TestName

class NodePatternPredicateParserTest extends CypherFunSuite with TestName with JavaccParserAstTestBase[NodePattern] {

  implicit val parser: JavaccRule[NodePattern] = JavaccRule.NodePattern

  test("MATCH (n WHERE n.prop > 123)") {
    val expected = Seq(
      NodePattern(
        Some(varFor("n")),
        None,
        None,
        Some(greaterThan(prop("n", "prop"), literalInt(123)))
      )(pos)
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
              labelAtom("A", (1, 4, 3)),
              labelAtom("B", (1, 6, 5))
            ),
            labelAtom("C", (1, 8, 7))
          )
        ),
        properties = Some(mapOf("prop" -> literalInt(42))),
        predicate = Some(lessThan(prop("n", "otherProp"), literalInt(123)))
      )((1, 1, 0))
    }
  }

  test("MATCH (WHERE WHERE WHERE.prop > 123)") {
    parseNodePatterns(testName) shouldBe Seq(
      NodePattern(
        Some(varFor("WHERE")),
        None,
        None,
        Some(greaterThan(prop("WHERE", "prop"), literalInt(123)))
      )(pos)
    )
  }

  test("RETURN [(n:A WHERE n.prop >= 123)-->(end WHERE end.prop < 42) | n]") {
    parseNodePatterns(testName).toSet shouldBe Set(
      NodePattern(
        Some(varFor("n")),
        Some(labelAtom("A")),
        None,
        Some(greaterThanOrEqual(prop("n", "prop"), literalInt(123)))
      )(pos),
      NodePattern(
        Some(varFor("end")),
        None,
        None,
        Some(lessThan(prop("end", "prop"), literalInt(42)))
      )(pos)
    )
  }

  test("RETURN exists((n {prop: 'test'} WHERE n.otherProp = 123)-->(end WHERE end.prop = 42)) AS result") {
    parseNodePatterns(testName).toSet shouldBe Set(
      NodePattern(
        Some(varFor("n")),
        None,
        Some(mapOf("prop" -> literalString("test"))),
        Some(equals(prop("n", "otherProp"), literalInt(123)))
      )(pos),
      NodePattern(
        Some(varFor("end")),
        None,
        None,
        Some(equals(prop("end", "prop"), literalInt(42)))
      )(pos)
    )
  }

  test("MATCH (WHERE {prop: 123})") {
    parseNodePatterns(testName) shouldBe Seq(
      NodePattern(
        Some(varFor("WHERE")),
        None,
        Some(mapOf("prop" -> literal(123))),
        None
      )(pos)
    )
  }

  test("MATCH (:Label {prop: 123} WHERE 2 > 1)") {
    val e = the[Exception] thrownBy parseNodePatterns(testName)
    e.getMessage should include("Invalid input 'WHERE'")
  }

  private val exceptionFactory = OpenCypherExceptionFactory(None)

  private def parseNodePatterns(query: String): Seq[NodePattern] = {
    val ast = JavaCCParser.parse(query, exceptionFactory, new AnonymousVariableNameGenerator())
    ast.folder.findAllByClass[NodePattern]
  }
}
