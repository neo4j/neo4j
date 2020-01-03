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
package org.neo4j.cypher.internal.compiler.planner.logical.steps

import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._
import org.neo4j.cypher.internal.compiler.planner._
import org.neo4j.cypher.internal.compiler.planner.logical.LogicalPlanningContext
import org.neo4j.cypher.internal.compiler.{IndexHintUnfulfillableNotification, JoinHintUnfulfillableNotification}
import org.neo4j.cypher.internal.ir.{PatternRelationship, QueryGraph, RegularSinglePlannerQuery, VarPatternLength}
import org.neo4j.cypher.internal.planner.spi.PlanContext
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.v4_0.ast._
import org.neo4j.cypher.internal.v4_0.expressions.{PropertyKeyName, SemanticDirection}
import org.neo4j.cypher.internal.v4_0.frontend.phases.RecordingNotificationLogger
import org.neo4j.cypher.internal.v4_0.util.test_helpers.CypherFunSuite
import org.neo4j.exceptions.{HintException, IndexHintException, InternalException, JoinHintException}

class ExtractBestPlanTest extends CypherFunSuite with LogicalPlanningTestSupport {

  private def newIndexHint(): Hint = { UsingIndexHint(varFor("a"), labelName("User"), Seq(PropertyKeyName("name")(pos)))_ }

  private def newJoinHint(): Hint = { UsingJoinHint(Seq(varFor("a")))_ }

  private def newQueryWithIdxHint() = RegularSinglePlannerQuery(
    QueryGraph(
      patternNodes = Set("a", "b")
    ).addHints(Set(newIndexHint())))

  private def newQueryWithJoinHint() = RegularSinglePlannerQuery(
    QueryGraph(
      patternNodes = Set("a", "b")
    ).addHints(Set(newJoinHint())))

  private def getPlanContext(hasIndex: Boolean): PlanContext = {
    val planContext = newMockedPlanContext()
    when(planContext.indexExistsForLabelAndProperties(anyString(), any())).thenReturn(hasIndex)
    planContext
  }

  private def getSimpleLogicalPlanWithAandB(context: LogicalPlanningContext) : LogicalPlan = {
    newMockedLogicalPlan(context.planningAttributes, "a", "b")
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
      verifyBestPlan(plan, query, context)
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
      verifyBestPlan(getSimpleLogicalPlanWithAandB(context), query, context)
    }
  }

  test("should not throw when finding plan that does solve all pattern nodes") {
    val query = RegularSinglePlannerQuery(
      QueryGraph(
        patternNodes = Set("a", "b")
      )
    )
    val context = newMockedLogicalPlanningContext(planContext = newMockedPlanContext())

    verifyBestPlan(getSimpleLogicalPlanWithAandB(context), query, context) // should not throw
  }

  test("should throw when finding plan that contains unfulfillable index hint") {
    val context = newMockedLogicalPlanningContext(planContext = getPlanContext(false), useErrorsOverWarnings = true)

    a [IndexHintException] should be thrownBy {
      verifyBestPlan(getSimpleLogicalPlanWithAandB(context), newQueryWithIdxHint(), context)
    }
  }

  test("should throw when finding plan that contains unfulfillable join hint") {
    val context = newMockedLogicalPlanningContext(planContext = getPlanContext(false), useErrorsOverWarnings = true)

    a [JoinHintException] should be thrownBy {
      verifyBestPlan(getSimpleLogicalPlanWithAandB(context), newQueryWithJoinHint(), context)
    }
  }

  test("should issue warning when finding plan that contains unfulfillable index hint") {
    val notificationLogger = new RecordingNotificationLogger
    val context = newMockedLogicalPlanningContext(planContext = getPlanContext(false), notificationLogger = notificationLogger, useErrorsOverWarnings = false)

    verifyBestPlan(getSimpleLogicalPlanWithAandB(context), newQueryWithIdxHint(), context) // should not throw
    notificationLogger.notifications should contain(IndexHintUnfulfillableNotification("User", Seq("name")))
  }

  test("should issue warning when finding plan that contains unfulfillable join hint") {
    val notificationLogger = new RecordingNotificationLogger
    val context = newMockedLogicalPlanningContext(planContext = getPlanContext(false), notificationLogger = notificationLogger, useErrorsOverWarnings = false)

    verifyBestPlan(getSimpleLogicalPlanWithAandB(context), newQueryWithJoinHint(), context) // should not throw
    val result = notificationLogger.notifications
    result should contain(JoinHintUnfulfillableNotification(Array("a")))
  }

  test("should succeed when finding plan that contains fulfillable index hint") {
    val notificationLogger = new RecordingNotificationLogger
    val context = newMockedLogicalPlanningContext(planContext = getPlanContext(true), notificationLogger = notificationLogger, useErrorsOverWarnings = false)
    val plan: LogicalPlan = newMockedLogicalPlan(Set("a", "b"), context.planningAttributes, hints = Set[Hint](newIndexHint()))

    verifyBestPlan(plan, newQueryWithIdxHint(), context) // should not throw
    notificationLogger.notifications should be(empty)
  }

  test("should succeed when finding plan that contains fulfillable join hint") {
    val notificationLogger = new RecordingNotificationLogger
    val context = newMockedLogicalPlanningContext(planContext = getPlanContext(true), notificationLogger = notificationLogger, useErrorsOverWarnings = false)
    val plan: LogicalPlan = newMockedLogicalPlan(Set("a", "b"), context.planningAttributes, hints = Set[Hint](newJoinHint()))

    verifyBestPlan(plan, newQueryWithJoinHint(), context) // should not throw
    notificationLogger.notifications should be(empty)
  }

  test("should throw when finding plan that does not contain a fulfillable index hint") {
    val context = newMockedLogicalPlanningContext(planContext = getPlanContext(true), useErrorsOverWarnings = false)

    a [HintException] should be thrownBy {
      verifyBestPlan(getSimpleLogicalPlanWithAandB(context), newQueryWithIdxHint(), context)
    }
  }
}
