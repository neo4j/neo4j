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
package org.neo4j.cypher.internal.compiler.planner

import org.mockito.Mockito.when
import org.neo4j.cypher.internal.ast.ASTAnnotationMap
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.ast.Query
import org.neo4j.cypher.internal.ast.factory.neo4j.JavaCCParser
import org.neo4j.cypher.internal.ast.semantics.ExpressionTypeInfo
import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.compiler.MissingLabelNotification
import org.neo4j.cypher.internal.compiler.MissingPropertyNameNotification
import org.neo4j.cypher.internal.compiler.MissingRelTypeNotification
import org.neo4j.cypher.internal.compiler.Neo4jCypherExceptionFactory
import org.neo4j.cypher.internal.compiler.phases.LogicalPlanState
import org.neo4j.cypher.internal.compiler.test_helpers.ContextHelper
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.FunctionInvocation
import org.neo4j.cypher.internal.expressions.FunctionName
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.ir.PlannerQuery
import org.neo4j.cypher.internal.planner.spi.IDPPlannerName
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.InternalNotification
import org.neo4j.cypher.internal.util.LabelId
import org.neo4j.cypher.internal.util.PropertyKeyId
import org.neo4j.cypher.internal.util.RecordingNotificationLogger
import org.neo4j.cypher.internal.util.RelTypeId
import org.neo4j.cypher.internal.util.symbols.CTDate
import org.neo4j.cypher.internal.util.symbols.CTDuration
import org.neo4j.cypher.internal.util.symbols.CTMap
import org.neo4j.cypher.internal.util.symbols.CTPoint
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.values.storable.DurationFields
import org.neo4j.values.storable.PointFields
import org.neo4j.values.storable.TemporalValue.TemporalFields

import scala.jdk.CollectionConverters.SetHasAsScala

class CheckForUnresolvedTokensTest extends CypherFunSuite with AstConstructionTestSupport
    with LogicalPlanConstructionTestSupport {

  test("warn when missing label") {
    // given
    val semanticTable = new SemanticTable

    // when
    val ast = parse("MATCH (a:A)-->(b:B) RETURN *")

    // then
    val notifications = checkForTokens(ast, semanticTable)

    notifications should equal(Set(
      MissingLabelNotification(InputPosition(9, 1, 10), "A"),
      MissingLabelNotification(InputPosition(17, 1, 18), "B")
    ))
  }

  test("don't warn when labels are there") {
    // given
    val semanticTable = new SemanticTable
    semanticTable.resolvedLabelNames.put("A", LabelId(42))
    semanticTable.resolvedLabelNames.put("B", LabelId(84))

    // when
    val ast = parse("MATCH (a:A)-->(b:B) RETURN *")

    // then
    checkForTokens(ast, semanticTable) shouldBe empty
  }

  test("warn when missing relationship type") {
    // given
    val semanticTable = new SemanticTable
    semanticTable.resolvedLabelNames.put("A", LabelId(42))
    semanticTable.resolvedLabelNames.put("B", LabelId(84))

    // when
    val ast = parse("MATCH (a:A)-[r:R1|R2]->(b:B) RETURN *")

    // then
    checkForTokens(ast, semanticTable) should equal(Set(
      MissingRelTypeNotification(InputPosition(15, 1, 16), "R1"),
      MissingRelTypeNotification(InputPosition(18, 1, 19), "R2")
    ))
  }

  test("don't warn when relationship types are there") {
    // given
    val semanticTable = new SemanticTable
    semanticTable.resolvedLabelNames.put("A", LabelId(42))
    semanticTable.resolvedLabelNames.put("B", LabelId(84))
    semanticTable.resolvedRelTypeNames.put("R1", RelTypeId(1))
    semanticTable.resolvedRelTypeNames.put("R2", RelTypeId(2))

    // when
    val ast = parse("MATCH (a:A)-[r:R1|R2]->(b:B) RETURN *")

    // then
    checkForTokens(ast, semanticTable) shouldBe empty
  }

  test("warn when missing node property key name") {
    // given
    val semanticTable = new SemanticTable().addNode(Variable("a")(InputPosition(16, 1, 17)))

    // when
    val ast = parse("MATCH (a) WHERE a.prop = 42 RETURN a")

    // then
    checkForTokens(ast, semanticTable) should equal(Set(
      MissingPropertyNameNotification(InputPosition(18, 1, 19), "prop")
    ))
  }

  test("don't warn when node property key name is there") {
    // given
    val semanticTable = new SemanticTable().addNode(Variable("a")(InputPosition(7, 1, 8)))
    semanticTable.resolvedPropertyKeyNames.put("prop", PropertyKeyId(0))

    // when
    val ast = parse("MATCH (a {prop: 42}) RETURN a")

    // then
    checkForTokens(ast, semanticTable) shouldBe empty
  }

  test("warn when missing relationship property key name") {
    // given
    val semanticTable = new SemanticTable().addRelationship(Variable("r")(InputPosition(23, 1, 24)))

    // when
    val ast = parse("MATCH ()-[r]->() WHERE r.prop = 42 RETURN r")

    // then
    checkForTokens(ast, semanticTable) should equal(Set(
      MissingPropertyNameNotification(InputPosition(25, 1, 26), "prop")
    ))
  }

  test("don't warn when relationship property key name is there") {
    // given
    val semanticTable = new SemanticTable().addRelationship(Variable("r")(InputPosition(10, 1, 11)))
    semanticTable.resolvedPropertyKeyNames.put("prop", PropertyKeyId(0))

    // when
    val ast = parse("MATCH ()-[r {prop: 42}]->() RETURN r")

    // then
    checkForTokens(ast, semanticTable) shouldBe empty
  }

  test("don't warn for map keys") {
    // given
    val emptyTypeMap = ASTAnnotationMap.empty[Expression, ExpressionTypeInfo]
    val semanticTypes = emptyTypeMap.updated(Variable("map")(InputPosition(32, 1, 33)), ExpressionTypeInfo(CTMap))
    val semanticTable = new SemanticTable(types = semanticTypes)
    semanticTable.resolvedPropertyKeyNames.put("key", PropertyKeyId(0))

    // when
    val ast = parse("WITH {key: 'val'} as map RETURN map.key")

    // then
    checkForTokens(ast, semanticTable) shouldBe empty
  }

  test("don't warn for literal maps") {
    // given
    val semanticTable = new SemanticTable

    // when
    val ast = parse("RETURN {prop: 'foo'}")

    // then
    checkForTokens(ast, semanticTable) shouldBe empty
  }

  test("don't warn when using point properties") {
    // given
    val emptyTypeMap = ASTAnnotationMap.empty[Expression, ExpressionTypeInfo]
    val semanticTypes = emptyTypeMap.updated(functionAt("point", 16), ExpressionTypeInfo(CTPoint))
    val semanticTable = new SemanticTable(types = semanticTypes).addNode(Variable("a")(InputPosition(22, 1, 23)))
    semanticTable.resolvedPropertyKeyNames.put("prop", PropertyKeyId(42))

    PointFields.values().foreach { property =>
      // when
      val ast = parse(s"MATCH (a) WHERE point(a.prop).${property.propertyKey} = 42 RETURN a")

      // then
      checkForTokens(ast, semanticTable) shouldBe empty
    }
  }

  test("don't warn when using temporal properties") {
    // given
    val emptyTypeMap = ASTAnnotationMap.empty[Expression, ExpressionTypeInfo]
    val semanticTypes = emptyTypeMap.updated(functionAt("date", 16), ExpressionTypeInfo(CTDate))
    val semanticTable = new SemanticTable(types = semanticTypes).addNode(Variable("a")(InputPosition(21, 1, 22)))
    semanticTable.resolvedPropertyKeyNames.put("prop", PropertyKeyId(42))

    TemporalFields.allFields().asScala.foreach { property =>
      // when
      val ast = parse(s"MATCH (a) WHERE date(a.prop).$property = 42 RETURN a")

      // then
      checkForTokens(ast, semanticTable) shouldBe empty
    }
  }

  test("don't warn when using duration properties") {
    // given
    val emptyTypeMap = ASTAnnotationMap.empty[Expression, ExpressionTypeInfo]
    val semanticTypes = emptyTypeMap.updated(functionAt("duration", 16), ExpressionTypeInfo(CTDuration))
    val semanticTable = new SemanticTable(types = semanticTypes).addNode(Variable("a")(InputPosition(25, 1, 26)))
    semanticTable.resolvedPropertyKeyNames.put("prop", PropertyKeyId(42))

    DurationFields.values().foreach { property =>
      // when
      val ast = parse(s"MATCH (a) WHERE duration(a.prop).${property.propertyKey} = 42 RETURN a")

      // then
      checkForTokens(ast, semanticTable) shouldBe empty
    }
  }

  test("don't warn when using special property keys, independent of case") {
    // given
    val emptyTypeMap = ASTAnnotationMap.empty[Expression, ExpressionTypeInfo]
    val semanticTypes = emptyTypeMap.updated(propertyAt(16), ExpressionTypeInfo(CTDuration))
    val semanticTable = new SemanticTable(types = semanticTypes).addNode(Variable("a")(InputPosition(16, 1, 17)))
    semanticTable.resolvedPropertyKeyNames.put("prop", PropertyKeyId(42))

    Seq("X", "yEaRs", "DAY", "epochMillis").foreach { property =>
      // when
      val ast = parse(s"MATCH (a) WHERE a.prop.$property = 42 RETURN a")
      // then
      checkForTokens(ast, semanticTable) shouldBe empty
    }
  }

  private def checkForTokens(ast: Query, semanticTable: SemanticTable): Set[InternalNotification] = {
    val notificationLogger = new RecordingNotificationLogger
    val plannerQuery = mock[PlannerQuery]
    when(plannerQuery.readOnly).thenReturn(true)
    val compilationState = LogicalPlanState(
      queryText = "apa",
      startPosition = None,
      plannerName = IDPPlannerName,
      newStubbedPlanningAttributes,
      new AnonymousVariableNameGenerator(),
      maybeStatement = Some(ast),
      maybeSemanticTable = Some(semanticTable),
      maybeQuery = Some(plannerQuery)
    )
    val context = ContextHelper.create(notificationLogger = notificationLogger)
    CheckForUnresolvedTokens.transform(compilationState, context)
    notificationLogger.notifications
  }

  private def parse(query: String): Query =
    JavaCCParser.parse(query, Neo4jCypherExceptionFactory(query, None)) match {
      case q: Query => q
      case _        => fail("Must be a Query")
    }

  private def functionAt(name: String, offset: Int): FunctionInvocation = {
    val functionPos = InputPosition(offset, 1, offset + 1)
    FunctionInvocation(FunctionName(name)(functionPos), propertyAt(offset))(functionPos)
  }

  private def propertyAt(offset: Int): Property = {
    val variablePos = InputPosition(offset, 1, offset + 1)
    val propertyPos = InputPosition(offset + 2, 1, offset + 3)
    Property(Variable("a")(variablePos), PropertyKeyName("prop")(propertyPos))(propertyPos)
  }

}
