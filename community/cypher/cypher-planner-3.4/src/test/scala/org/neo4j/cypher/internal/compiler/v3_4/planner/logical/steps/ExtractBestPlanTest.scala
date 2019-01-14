/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.cypher.internal.compiler.v3_4.planner.logical.steps

import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._
import org.neo4j.cypher.internal.util.v3_4.{HintException, IndexHintException, InternalException, JoinHintException}
import org.neo4j.cypher.internal.compiler.v3_4.planner._
import org.neo4j.cypher.internal.frontend.v3_4.ast._
import org.neo4j.cypher.internal.frontend.v3_4.notification.{IndexHintUnfulfillableNotification, JoinHintUnfulfillableNotification}
import org.neo4j.cypher.internal.frontend.v3_4.phases.RecordingNotificationLogger
import org.neo4j.cypher.internal.util.v3_4.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.ir.v3_4.{PatternRelationship, VarPatternLength, _}
import org.neo4j.cypher.internal.planner.v3_4.spi.PlanningAttributes.{Cardinalities, Solveds}
import org.neo4j.cypher.internal.planner.v3_4.spi.{IndexDescriptor, PlanContext}
import org.neo4j.cypher.internal.v3_4.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.v3_4.expressions.{LabelName, PatternExpression, PropertyKeyName, SemanticDirection}

class ExtractBestPlanTest extends CypherFunSuite with LogicalPlanningTestSupport {

  private val subQueryLookupTable = Map.empty[PatternExpression, QueryGraph]

  private def newIndexHint(): Hint = { UsingIndexHint(varFor("a"), LabelName("User")_, Seq(PropertyKeyName("name")(pos)))_ }

  private def newJoinHint(): Hint = { UsingJoinHint(Seq(varFor("a")))_ }

  private def newQueryWithIdxHint() = RegularPlannerQuery(
    QueryGraph(
      patternNodes = Set("a", "b")
    ).addHints(Set(newIndexHint())))

  private def newQueryWithJoinHint() = RegularPlannerQuery(
    QueryGraph(
      patternNodes = Set("a", "b")
    ).addHints(Set(newJoinHint())))

  private def getPlanContext(hasIndex: Boolean): PlanContext = {

    val planContext = newMockedPlanContext
    val indexDescriptor: Option[IndexDescriptor] =
      if (hasIndex) Option(IndexDescriptor(0, 0))
      else None

    when(planContext.indexGet(anyString(),any())).thenReturn(indexDescriptor)
    when(planContext.uniqueIndexGet(anyString(),any())).thenReturn(None)
    planContext
  }

  private def getSimpleLogicalPlanWithAandB(solveds: Solveds, cardinalities: Cardinalities) : LogicalPlan = {
    newMockedLogicalPlan(solveds, cardinalities, "a", "b")
  }

  test("should throw when finding plan that does not solve all pattern nodes") {
    val query = RegularPlannerQuery(
      QueryGraph(
        patternNodes = Set("a", "b")
      )
    )
    val (logicalPlanContext, solveds, cardinalities) = newMockedLogicalPlanningContext(
      planContext = newMockedPlanContext)
    val plan = newMockedLogicalPlan(solveds, cardinalities, "b")

    a [InternalException] should be thrownBy {
      verifyBestPlan(plan, query, logicalPlanContext, solveds, cardinalities)
    }
  }

  test("should throw when finding plan that does not solve all pattern relationships") {
    val patternRel = PatternRelationship("r", ("a", "b"), SemanticDirection.OUTGOING, Seq.empty, VarPatternLength.unlimited)
    val query = RegularPlannerQuery(
      QueryGraph(
        patternNodes = Set("a", "b"),
        patternRelationships = Set(patternRel)
      )
    )
    val (logicalPlanContext, solveds, cardinalities) = newMockedLogicalPlanningContext(
      planContext= newMockedPlanContext
    )

    a [InternalException] should be thrownBy {
      verifyBestPlan(getSimpleLogicalPlanWithAandB(solveds, cardinalities), query, logicalPlanContext, solveds, cardinalities)
    }
  }

  test("should not throw when finding plan that does solve all pattern nodes") {
    val query = RegularPlannerQuery(
      QueryGraph(
        patternNodes = Set("a", "b")
      )
    )
    val (logicalPlanContext, solveds, cardinalities) = newMockedLogicalPlanningContext(
      planContext = newMockedPlanContext)

    verifyBestPlan(getSimpleLogicalPlanWithAandB(solveds, cardinalities), query, logicalPlanContext, solveds, cardinalities).availableSymbols should equal(Set("a", "b"))
  }

  test("should throw when finding plan that contains unfulfillable index hint") {
    val (logicalPlanContext, solveds, cardinalities) = newMockedLogicalPlanningContext(
      planContext = getPlanContext(false), useErrorsOverWarnings = true)

    a [IndexHintException] should be thrownBy {
      verifyBestPlan(getSimpleLogicalPlanWithAandB(solveds, cardinalities), newQueryWithIdxHint(), logicalPlanContext, solveds, cardinalities)
    }
  }

  test("should throw when finding plan that contains unfulfillable join hint") {
    val (logicalPlanContext, solveds, cardinalities) = newMockedLogicalPlanningContext(
      planContext = getPlanContext(false), useErrorsOverWarnings = true)

    a [JoinHintException] should be thrownBy {
      verifyBestPlan(getSimpleLogicalPlanWithAandB(solveds, cardinalities), newQueryWithJoinHint(), logicalPlanContext, solveds, cardinalities)
    }
  }

  test("should issue warning when finding plan that contains unfulfillable index hint") {
    val notificationLogger = new RecordingNotificationLogger
    val (logicalPlanContext, solveds, cardinalities) = newMockedLogicalPlanningContext(
      planContext = getPlanContext(false), useErrorsOverWarnings = false,
      notificationLogger = notificationLogger)

    verifyBestPlan(getSimpleLogicalPlanWithAandB(solveds, cardinalities), newQueryWithIdxHint(), logicalPlanContext, solveds, cardinalities).availableSymbols should equal(Set("a", "b"))
    notificationLogger.notifications should contain(IndexHintUnfulfillableNotification("User", Seq("name")))
  }

  test("should issue warning when finding plan that contains unfulfillable join hint") {
    val notificationLogger = new RecordingNotificationLogger
    val (logicalPlanContext, solveds, cardinalities) = newMockedLogicalPlanningContext(
      planContext = getPlanContext(false), useErrorsOverWarnings = false,
      notificationLogger = notificationLogger)

    verifyBestPlan(getSimpleLogicalPlanWithAandB(solveds, cardinalities), newQueryWithJoinHint(), logicalPlanContext, solveds, cardinalities).availableSymbols should equal(Set("a", "b"))
    val result = notificationLogger.notifications
    result should contain(JoinHintUnfulfillableNotification(Array("a")))
  }

  test("should succeed when finding plan that contains fulfillable index hint") {
    val notificationLogger = new RecordingNotificationLogger
    val (logicalPlanContext, solveds, cardinalities) = newMockedLogicalPlanningContext(
      planContext = getPlanContext(true), useErrorsOverWarnings = false,
      notificationLogger = notificationLogger)
    val plan: LogicalPlan = newMockedLogicalPlan(Set("a", "b"), solveds, cardinalities, hints = Set[Hint](newIndexHint()))

    verifyBestPlan(plan, newQueryWithIdxHint(), logicalPlanContext, solveds, cardinalities).availableSymbols should equal(Set("a", "b"))
    notificationLogger.notifications should be(empty)
  }

  test("should succeed when finding plan that contains fulfillable join hint") {
    val notificationLogger = new RecordingNotificationLogger
    val (logicalPlanContext, solveds, cardinalities) = newMockedLogicalPlanningContext(
      planContext = getPlanContext(true), useErrorsOverWarnings = false,
      notificationLogger = notificationLogger)
    val plan: LogicalPlan = newMockedLogicalPlan(Set("a", "b"), solveds, cardinalities, hints = Set[Hint](newJoinHint()))

    verifyBestPlan(plan, newQueryWithJoinHint(), logicalPlanContext, solveds, cardinalities).availableSymbols should equal(Set("a", "b"))
    notificationLogger.notifications should be(empty)
  }

  test("should throw when finding plan that does not contain a fulfillable index hint") {
    val (logicalPlanContext, solveds, cardinalities) = newMockedLogicalPlanningContext(
      planContext = getPlanContext(true), useErrorsOverWarnings = false)

    a [HintException] should be thrownBy {
      verifyBestPlan(getSimpleLogicalPlanWithAandB(solveds, cardinalities), newQueryWithIdxHint(), logicalPlanContext, solveds, cardinalities)
    }
  }
}
