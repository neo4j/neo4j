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
package org.neo4j.cypher.internal.compiler.planner.logical.steps

import org.mockito.ArgumentMatchers.any
import org.mockito.ArgumentMatchers.anyString
import org.mockito.Mockito.when
import org.neo4j.common.EntityType
import org.neo4j.cypher.internal.ast.Hint
import org.neo4j.cypher.internal.ast.UsingIndexHint
import org.neo4j.cypher.internal.ast.UsingJoinHint
import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.compiler.IndexHintUnfulfillableNotification
import org.neo4j.cypher.internal.compiler.JoinHintUnfulfillableNotification
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport
import org.neo4j.cypher.internal.compiler.planner.logical.LogicalPlanningContext
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.expressions.SemanticDirection.BOTH
import org.neo4j.cypher.internal.ir.PatternRelationship
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.ir.RegularSinglePlannerQuery
import org.neo4j.cypher.internal.ir.SimplePatternLength
import org.neo4j.cypher.internal.ir.VarPatternLength
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.planner.spi.PlanContext
import org.neo4j.cypher.internal.util.RecordingNotificationLogger
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.exceptions.HintException
import org.neo4j.exceptions.IndexHintException
import org.neo4j.exceptions.InternalException
import org.neo4j.exceptions.JoinHintException

class VerifyBestPlanTest extends CypherFunSuite with LogicalPlanningTestSupport {

  private def newNodeIndexHint(): Hint = { UsingIndexHint(varFor("a"), labelOrRelTypeName("User"), Seq(PropertyKeyName("name")(pos)))_ }

  private def newRelationshipIndexHint(): Hint = { UsingIndexHint(varFor("r"), labelOrRelTypeName("User"), Seq(PropertyKeyName("name")(pos)))_ }

  private def newJoinHint(): Hint = { UsingJoinHint(Seq(varFor("a")))_ }

  private def newQueryWithNodeIndexHint() = RegularSinglePlannerQuery(
    QueryGraph(
      patternNodes = Set("a", "b")
    ).addHints(Set(newNodeIndexHint())))

  private def newQueryWithRelationshipIndexHint() = RegularSinglePlannerQuery(
    QueryGraph(
      patternNodes = Set("a", "b"),
      patternRelationships = Set(PatternRelationship("r", ("a", "b"), BOTH, Seq.empty, SimplePatternLength))
    ).addHints(Set(newRelationshipIndexHint())))

  private def newQueryWithJoinHint() = RegularSinglePlannerQuery(
    QueryGraph(
      patternNodes = Set("a", "b")
    ).addHints(Set(newJoinHint())))

  private def getPlanContext(hasIndex: Boolean): PlanContext = {
    val planContext = newMockedPlanContext()
    when(planContext.btreeIndexExistsForLabelAndProperties(anyString(), any())).thenReturn(hasIndex)
    when(planContext.btreeIndexExistsForRelTypeAndProperties(anyString(), any())).thenReturn(hasIndex)
    planContext
  }

  private def getSimpleLogicalPlanWithAandB(context: LogicalPlanningContext) : LogicalPlan = {
    newMockedLogicalPlan(context.planningAttributes, "a", "b")
  }

  private def getSimpleLogicalPlanWithAandBandR(context: LogicalPlanningContext) : LogicalPlan = {
    newMockedLogicalPlanWithPatterns(context.planningAttributes, Set("a", "b"), Seq(PatternRelationship("r", ("a", "b"), BOTH, Seq.empty, SimplePatternLength)))
  }

  private def getSemanticTable: SemanticTable = {
    val semanticTable = newMockedSemanticTable
    when(semanticTable.isNodeNoFail("a")).thenReturn(true)
    when(semanticTable.isNodeNoFail(varFor("a"))).thenReturn(true)

    when(semanticTable.isNodeNoFail("b")).thenReturn(true)
    when(semanticTable.isNodeNoFail(varFor("b"))).thenReturn(true)

    when(semanticTable.isRelationshipNoFail("r")).thenReturn(true)
    when(semanticTable.isRelationshipNoFail(varFor("r"))).thenReturn(true)
    semanticTable
  }

  test("should throw when finding plan that does not solve all pattern nodes") {
    val query = RegularSinglePlannerQuery(
      QueryGraph(
        patternNodes = Set("a", "b")
      )
    )
    val context = newMockedLogicalPlanningContext(planContext = newMockedPlanContext())
    val plan = newMockedLogicalPlan(context.planningAttributes, "b")

    a [InternalException] should be thrownBy {
      VerifyBestPlan(plan, query, context)
    }
  }

  test("should throw when finding plan that does not solve all pattern relationships") {
    val patternRel = PatternRelationship("r", ("a", "b"), SemanticDirection.OUTGOING, Seq.empty, VarPatternLength.unlimited)
    val query = RegularSinglePlannerQuery(
      QueryGraph(
        patternNodes = Set("a", "b"),
        patternRelationships = Set(patternRel)
      )
    )
    val context = newMockedLogicalPlanningContext(planContext= newMockedPlanContext())

    a [InternalException] should be thrownBy {
      VerifyBestPlan(getSimpleLogicalPlanWithAandB(context), query, context)
    }
  }

  test("should not throw when finding plan that does solve all pattern nodes") {
    val query = RegularSinglePlannerQuery(
      QueryGraph(
        patternNodes = Set("a", "b")
      )
    )
    val context = newMockedLogicalPlanningContext(planContext = newMockedPlanContext())

    VerifyBestPlan(getSimpleLogicalPlanWithAandB(context), query, context) // should not throw
  }

  test("should throw when finding plan that contains unfulfillable node index hint") {
    val context = newMockedLogicalPlanningContext(planContext = getPlanContext(false), useErrorsOverWarnings = true)

    the [IndexHintException] thrownBy {
      VerifyBestPlan(getSimpleLogicalPlanWithAandB(context), newQueryWithNodeIndexHint(), context)
    } should have message "No such index: INDEX FOR (`a`:`User`) ON (`a`.`name`)"
  }

  test("should throw when finding plan that contains unfulfillable relationship index hint") {
    val context = newMockedLogicalPlanningContext(
      planContext = getPlanContext(false),
      semanticTable = getSemanticTable,
      useErrorsOverWarnings = true)

    the [IndexHintException] thrownBy {
      VerifyBestPlan(getSimpleLogicalPlanWithAandBandR(context), newQueryWithRelationshipIndexHint(), context)
    } should have message "No such index: INDEX FOR ()-[`r`:`User`]-() ON (`r`.`name`)"
  }

  test("should throw when finding plan that contains unfulfillable join hint") {
    val context = newMockedLogicalPlanningContext(planContext = getPlanContext(false), useErrorsOverWarnings = true)

    a [JoinHintException] should be thrownBy {
      VerifyBestPlan(getSimpleLogicalPlanWithAandB(context), newQueryWithJoinHint(), context)
    }
  }

  test("should issue warning when finding plan that contains unfulfillable node index hint") {
    val notificationLogger = new RecordingNotificationLogger
    val context = newMockedLogicalPlanningContext(planContext = getPlanContext(false), notificationLogger = notificationLogger, useErrorsOverWarnings = false)

    VerifyBestPlan(getSimpleLogicalPlanWithAandB(context), newQueryWithNodeIndexHint(), context) // should not throw
    notificationLogger.notifications should contain(IndexHintUnfulfillableNotification("a", "User", Seq("name"), EntityType.NODE))
  }

  test("should issue warning when finding plan that contains unfulfillable relationship index hint") {
    val notificationLogger = new RecordingNotificationLogger
    val context = newMockedLogicalPlanningContext(planContext = getPlanContext(false),
      notificationLogger = notificationLogger,
      semanticTable = getSemanticTable,
      useErrorsOverWarnings = false)

    VerifyBestPlan(getSimpleLogicalPlanWithAandBandR(context), newQueryWithRelationshipIndexHint(), context) // should not throw
    notificationLogger.notifications should contain(IndexHintUnfulfillableNotification("r", "User", Seq("name"), EntityType.RELATIONSHIP))
  }

  test("should issue warning when finding plan that contains unfulfillable join hint") {
    val notificationLogger = new RecordingNotificationLogger
    val context = newMockedLogicalPlanningContext(planContext = getPlanContext(false), notificationLogger = notificationLogger, useErrorsOverWarnings = false)

    VerifyBestPlan(getSimpleLogicalPlanWithAandB(context), newQueryWithJoinHint(), context) // should not throw
    val result = notificationLogger.notifications
    result should contain(JoinHintUnfulfillableNotification(Array("a")))
  }

  test("should succeed when finding plan that contains fulfillable node index hint") {
    val notificationLogger = new RecordingNotificationLogger
    val context = newMockedLogicalPlanningContext(planContext = getPlanContext(true), notificationLogger = notificationLogger, useErrorsOverWarnings = false)
    val plan: LogicalPlan = newMockedLogicalPlan(Set("a", "b"), context.planningAttributes, hints = Set[Hint](newNodeIndexHint()))

    VerifyBestPlan(plan, newQueryWithNodeIndexHint(), context) // should not throw
    notificationLogger.notifications should be(empty)
  }

  test("should succeed when finding plan that contains fulfillable relationship index hint") {
    val notificationLogger = new RecordingNotificationLogger
    val context = newMockedLogicalPlanningContext(planContext = getPlanContext(true), notificationLogger = notificationLogger, useErrorsOverWarnings = false)
    val plan: LogicalPlan = newMockedLogicalPlanWithPatterns(
      context.planningAttributes,
      Set("a", "b"),
      Seq(PatternRelationship("r", ("a", "b"), BOTH, Seq.empty, SimplePatternLength)),
      hints = Set[Hint](newRelationshipIndexHint()))

    VerifyBestPlan(plan, newQueryWithRelationshipIndexHint(), context) // should not throw
    notificationLogger.notifications should be(empty)
  }

  test("should succeed when finding plan that contains fulfillable join hint") {
    val notificationLogger = new RecordingNotificationLogger
    val context = newMockedLogicalPlanningContext(planContext = getPlanContext(true), notificationLogger = notificationLogger, useErrorsOverWarnings = false)
    val plan: LogicalPlan = newMockedLogicalPlan(Set("a", "b"), context.planningAttributes, hints = Set[Hint](newJoinHint()))

    VerifyBestPlan(plan, newQueryWithJoinHint(), context) // should not throw
    notificationLogger.notifications should be(empty)
  }

  test("should throw when finding plan that does not contain a fulfillable node index hint") {
    val context = newMockedLogicalPlanningContext(planContext = getPlanContext(true), semanticTable = getSemanticTable, useErrorsOverWarnings = false)

    a [HintException] should be thrownBy {
      VerifyBestPlan(getSimpleLogicalPlanWithAandB(context), newQueryWithNodeIndexHint(), context)
    }
  }

  test("should throw when finding plan that does not contain a fulfillable relationship index hint") {
    val context = newMockedLogicalPlanningContext(planContext = getPlanContext(true), semanticTable = getSemanticTable, useErrorsOverWarnings = false)

    a [HintException] should be thrownBy {
      VerifyBestPlan(getSimpleLogicalPlanWithAandBandR(context), newQueryWithRelationshipIndexHint(), context)
    }
  }

  test("should throw when finding plan that contains unfulfillable node text index hint") {
    val planContext = newMockedPlanContext()
    when(planContext.textIndexExistsForLabelAndProperties(any(), any())).thenReturn(true)

    val context = newMockedLogicalPlanningContext(planContext = planContext, semanticTable = getSemanticTable, useErrorsOverWarnings = true)

    the [IndexHintException] thrownBy {
      VerifyBestPlan(getSimpleLogicalPlanWithAandB(context), newQueryWithNodeIndexHint(), context)
    } should have message "No such index: INDEX FOR (`a`:`User`) ON (`a`.`name`)"
  }

  test("should throw when finding plan that does not contain a fulfillable node text index hint") {
    val planContext = newMockedPlanContext()
    when(planContext.textIndexExistsForLabelAndProperties(any(), any())).thenReturn(true)

    val context = newMockedLogicalPlanningContext(planContext = planContext, semanticTable = getSemanticTable, useErrorsOverWarnings = true)
      .copy(planningTextIndexesEnabled = true)

    a [HintException] should be thrownBy {
      VerifyBestPlan(getSimpleLogicalPlanWithAandB(context), newQueryWithNodeIndexHint(), context)
    }
  }

  test("should throw when finding plan that contains unfulfillable relationship text index hint") {
    val planContext = newMockedPlanContext()
    when(planContext.textIndexExistsForRelTypeAndProperties(any(), any())).thenReturn(true)

    val context = newMockedLogicalPlanningContext(planContext = planContext, semanticTable = getSemanticTable, useErrorsOverWarnings = true)

    the [IndexHintException] thrownBy {
      VerifyBestPlan(getSimpleLogicalPlanWithAandBandR(context), newQueryWithRelationshipIndexHint(), context)
    } should have message "No such index: INDEX FOR ()-[`r`:`User`]-() ON (`r`.`name`)"
  }

  test("should throw when finding plan that does not contain a fulfillable relationship text index hint") {
    val planContext = newMockedPlanContext()
    when(planContext.textIndexExistsForRelTypeAndProperties(any(), any())).thenReturn(true)

    val context = newMockedLogicalPlanningContext(planContext = planContext, semanticTable = getSemanticTable, useErrorsOverWarnings = true)
      .copy(planningTextIndexesEnabled = true)

    a [HintException] should be thrownBy {
      VerifyBestPlan(getSimpleLogicalPlanWithAandBandR(context), newQueryWithRelationshipIndexHint(), context)
    }
  }
}
