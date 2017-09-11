/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v3_4.planner

import org.neo4j.cypher.internal.apa.v3_4.InputPosition
import org.neo4j.cypher.internal.compiler.v3_4.IDPPlannerName
import org.neo4j.cypher.internal.compiler.v3_4.phases.LogicalPlanState
import org.neo4j.cypher.internal.compiler.v3_4.test_helpers.ContextHelper
import org.neo4j.cypher.internal.frontend.v3_4.ast.Query
import org.neo4j.cypher.internal.frontend.v3_4.notification.{InternalNotification, MissingLabelNotification, MissingPropertyNameNotification, MissingRelTypeNotification}
import org.neo4j.cypher.internal.frontend.v3_4.phases.RecordingNotificationLogger
import org.neo4j.cypher.internal.apa.v3_4.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.frontend.v3_4._

class CheckForUnresolvedTokensTest extends CypherFunSuite with AstRewritingTestSupport {

  test("warn when missing label") {
    //given
    val semanticTable = new SemanticTable

    //when
    val ast = parse("MATCH (a:A)-->(b:B) RETURN *")

    //then
    val notifications = checkForTokens(ast, semanticTable)

    notifications should equal(Set(
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
    checkForTokens(ast, semanticTable) shouldBe empty
  }

  test("warn when missing relationship type") {
    //given
    val semanticTable = new SemanticTable
    semanticTable.resolvedLabelIds.put("A", LabelId(42))
    semanticTable.resolvedLabelIds.put("B", LabelId(84))

    //when
    val ast = parse("MATCH (a:A)-[r:R1|R2]->(b:B) RETURN *")

    //then
    checkForTokens(ast, semanticTable) should equal(Set(
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
    checkForTokens(ast, semanticTable) shouldBe empty
  }

  test("warn when missing property key name") {
    //given
    val semanticTable = new SemanticTable

    //when
    val ast = parse("MATCH (a) WHERE a.prop = 42 RETURN a")

    //then
    checkForTokens(ast, semanticTable) should equal(Set(
      MissingPropertyNameNotification(InputPosition(18, 1, 19), "prop")))
  }

  test("don't warn when property key name is there") {
    //given
    val semanticTable = new SemanticTable
    semanticTable.resolvedPropertyKeyNames.put("prop", PropertyKeyId(42))

    //when
    val ast = parse("MATCH (a {prop: 42}) RETURN a")

    //then
    checkForTokens(ast, semanticTable) shouldBe empty
  }

  test("don't warn for literal maps") {
    //given
    val semanticTable = new SemanticTable

    //when
    val ast = parse("RETURN {prop: 'foo'}")

    //then
    checkForTokens(ast, semanticTable) shouldBe empty
  }

  private def checkForTokens(ast: Query, semanticTable: SemanticTable): Set[InternalNotification] = {
    val notificationLogger = new RecordingNotificationLogger
    val compilationState = LogicalPlanState(queryText = "apa", startPosition = None, plannerName = IDPPlannerName, maybeStatement = Some(ast), maybeSemanticTable = Some(semanticTable))
    val context = ContextHelper.create(notificationLogger = notificationLogger)
    CheckForUnresolvedTokens.transform(compilationState, context)
    notificationLogger.notifications
  }

  private def parse(query: String): Query = parser.parse(query) match {
    case q: Query => q
    case _ => fail("Must be a Query")
  }

}
