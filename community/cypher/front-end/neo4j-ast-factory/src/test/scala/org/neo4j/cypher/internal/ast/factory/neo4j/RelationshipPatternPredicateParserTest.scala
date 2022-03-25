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

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.expressions.RelationshipPattern
import org.neo4j.cypher.internal.expressions.SemanticDirection.OUTGOING
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.OpenCypherExceptionFactory
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.util.test_helpers.TestName

class RelationshipPatternPredicateParserTest extends CypherFunSuite with TestName with AstConstructionTestSupport {

  test("MATCH (n)-[r WHERE r.prop > 123]->()") {
    parseRelationshipPatterns(testName) shouldBe Seq(
      RelationshipPattern(
        Some(varFor("r")),
        None,
        None,
        None,
        Some(greaterThan(prop("r", "prop"), literalInt(123))),
        OUTGOING
      )(pos)
    )
  }

  test("MATCH (n)-[r:Foo|Bar*1..5 {prop: 'test'} WHERE r.otherProp > 123]->()") {
    parseRelationshipPatterns(testName) shouldBe Seq(
      RelationshipPattern(
        Some(varFor("r")),
        Some(labelDisjunction(labelRelTypeLeaf("Foo"), labelRelTypeLeaf("Bar"))),
        Some(Some(range(Some(1), Some(5)))),
        Some(mapOf("prop" -> literalString("test"))),
        Some(greaterThan(prop("r", "otherProp"), literalInt(123))),
        OUTGOING
      )(pos)
    )
  }

  test("MATCH ()-[r:R|S|T {prop: 42} WHERE r.otherProp > 123]->()") {
    parseRelationshipPatterns(testName) shouldBe Seq(
      RelationshipPattern(
        Some(varFor("r")),
        Some(labelDisjunction(labelDisjunction(labelRelTypeLeaf("R"), labelRelTypeLeaf("S")), labelRelTypeLeaf("T"))),
        None,
        Some(mapOf("prop" -> literal(42))),
        Some(greaterThan(prop("r", "otherProp"), literalInt(123))),
        OUTGOING
      )(pos)
    )
  }

  test("MATCH ()-[WHERE WHERE WHERE.prop > 123]->()") {
    parseRelationshipPatterns(testName) shouldBe Seq(
      RelationshipPattern(
        Some(varFor("WHERE")),
        None,
        None,
        None,
        Some(greaterThan(prop("WHERE", "prop"), literalInt(123))),
        OUTGOING
      )(pos)
    )
  }

  test("RETURN [()-[r:R WHERE r.prop > 123]->() | r]") {
    parseRelationshipPatterns(testName) shouldBe Seq(
      RelationshipPattern(
        Some(varFor("r")),
        Some(labelRelTypeLeaf("R")),
        None,
        None,
        Some(greaterThan(prop("r", "prop"), literalInt(123))),
        OUTGOING
      )(pos)
    )
  }

  test("RETURN exists(()-[r {prop: 'test'} WHERE r.otherProp = 123]->()) AS result") {
    parseRelationshipPatterns(testName) shouldBe Seq(
      RelationshipPattern(
        Some(varFor("r")),
        None,
        None,
        Some(mapOf("prop" -> literal("test"))),
        Some(equals(prop("r", "otherProp"), literalInt(123))),
        OUTGOING
      )(pos)
    )
  }

  private val exceptionFactory = OpenCypherExceptionFactory(None)

  private def parseRelationshipPatterns(query: String): Seq[RelationshipPattern] = {
    val ast = JavaCCParser.parse(query, exceptionFactory, new AnonymousVariableNameGenerator())
    ast.folder.findAllByClass[RelationshipPattern]
  }
}
