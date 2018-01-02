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
package org.neo4j.cypher.internal.compiler.v2_3.planner

import org.neo4j.cypher.internal.frontend.v2_3.ast.{Query, Statement, SingleQuery}
import org.neo4j.cypher.internal.frontend.v2_3.notification.{MissingPropertyNameNotification, MissingRelTypeNotification, MissingLabelNotification}
import org.neo4j.cypher.internal.frontend.v2_3.{PropertyKeyId, RelTypeId, InputPosition, LabelId, SemanticTable}
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite

class CheckForUnresolvedTokensTest extends CypherFunSuite with AstRewritingTestSupport {

  test("warn when missing label") {
    //given
    val semanticTable = new SemanticTable

    //when
    val ast = parse("MATCH (a:A)-->(b:B) RETURN *")

    //then
    checkForUnresolvedTokens(ast, semanticTable) should equal(Seq(
      MissingLabelNotification(InputPosition(9, 1, 10), "A"),
      MissingLabelNotification(InputPosition(17, 1, 18), "B")))
  }

  test("don't warn when labels are there") {
    //given
    val semanticTable = new SemanticTable
    semanticTable.resolvedLabelIds.put("A", LabelId(42))
    semanticTable.resolvedLabelIds.put("B", LabelId(84))

    //when
    val ast = parse("MATCH (a:A)-->(b:B) RETURN *")

    //then
    checkForUnresolvedTokens(ast, semanticTable) shouldBe empty
  }

  test("warn when missing relationship type") {
    //given
    val semanticTable = new SemanticTable
    semanticTable.resolvedLabelIds.put("A", LabelId(42))
    semanticTable.resolvedLabelIds.put("B", LabelId(84))

    //when
    val ast = parse("MATCH (a:A)-[r:R1|R2]->(b:B) RETURN *")

    //then
    checkForUnresolvedTokens(ast, semanticTable) should equal(Seq(
      MissingRelTypeNotification(InputPosition(15, 1, 16), "R1"),
      MissingRelTypeNotification(InputPosition(18, 1, 19), "R2")))
  }

  test("don't warn when relationship types are there") {
    //given
    val semanticTable = new SemanticTable
    semanticTable.resolvedLabelIds.put("A", LabelId(42))
    semanticTable.resolvedLabelIds.put("B", LabelId(84))
    semanticTable.resolvedRelTypeNames.put("R1", RelTypeId(1))
    semanticTable.resolvedRelTypeNames.put("R2", RelTypeId(2))

    //when
    val ast = parse("MATCH (a:A)-[r:R1|R2]->(b:B) RETURN *")

    //then
    checkForUnresolvedTokens(ast, semanticTable) shouldBe empty
  }

  test("warn when missing property key name") {
    //given
    val semanticTable = new SemanticTable

    //when
    val ast = parse("MATCH (a {prop: 42}) RETURN a")

    //then
    checkForUnresolvedTokens(ast, semanticTable) should equal(Seq(
      MissingPropertyNameNotification(InputPosition(10, 1, 11), "prop")))
  }

  test("don't warn when property key name is there") {
    //given
    val semanticTable = new SemanticTable
    semanticTable.resolvedPropertyKeyNames.put("prop", PropertyKeyId(42))

    //when
    val ast = parse("MATCH (a {prop: 42}) RETURN a")

    //then
    checkForUnresolvedTokens(ast, semanticTable) shouldBe empty
  }

  private def parse(query: String): Query = parser.parse(query) match {
    case q: Query => q
    case _ => fail("Must be a Query")
  }

}
