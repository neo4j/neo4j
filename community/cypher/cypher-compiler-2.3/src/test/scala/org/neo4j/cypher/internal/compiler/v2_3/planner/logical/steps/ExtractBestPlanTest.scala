/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_3.planner.logical.steps

import org.neo4j.cypher.internal.compiler.v2_3.notification.IndexHintUnfulfillableNotification
import org.neo4j.cypher.internal.compiler.v2_3.spi.PlanContext
import org.neo4j.cypher.internal.compiler.v2_3.{RecordingNotificationLogger, IndexHintException}
import org.neo4j.cypher.internal.compiler.v2_3.ast._
import org.neo4j.cypher.internal.compiler.v2_3.planner._
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.plans._
import org.neo4j.cypher.internal.compiler.v2_3.test_helpers.CypherFunSuite
import org.neo4j.graphdb.Direction
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.neo4j.kernel.api.index.IndexDescriptor

class ExtractBestPlanTest extends CypherFunSuite with LogicalPlanningTestSupport {

  private implicit val subQueryLookupTable = Map.empty[PatternExpression, QueryGraph]

  private def getIndexHint(): Hint = { UsingIndexHint(ident("a"), LabelName("User")_, ident("name"))_ }

  private def getQueryWithIdxHint(): PlannerQuery = {
    val indexHint = getIndexHint()
    val query = PlannerQuery(
      QueryGraph(
        patternNodes = Set(IdName("a"), IdName("b"))
      ).addHints(Set(indexHint))
    )
    query
  }

  private def getPlanContext(hasIndex: Boolean): PlanContext = {

    val planContext = newMockedPlanContext
    val indexDescriptor: Option[IndexDescriptor] = if (hasIndex) Option(new IndexDescriptor(0,0)) else None

    when(planContext.getIndexRule(anyString(),anyString())).thenReturn(indexDescriptor)
    when(planContext.getUniqueIndexRule(anyString(),anyString())).thenReturn(None)
    planContext
  }

  private def getSimpleLogicalPlanWithAandB() : LogicalPlan = {
    newMockedLogicalPlan("a", "b")
  }

  test("should throw when finding plan that does not solve all pattern nodes") {
    val query = PlannerQuery(
      QueryGraph(
        patternNodes = Set(IdName("a"), IdName("b"))
      )
    )
    implicit val logicalPlanContext = newMockedLogicalPlanningContext(
      planContext = newMockedPlanContext)
    val plan = newMockedLogicalPlan("b")
    val planTable = greedyPlanTableWith(plan)

    a [CantHandleQueryException] should be thrownBy {
      verifyBestPlan(planTable.uniquePlan, query)
    }
  }

  test("should throw when finding plan that does not solve all pattern relationships") {
    val patternRel = PatternRelationship("r", ("a", "b"), Direction.OUTGOING, Seq.empty, VarPatternLength.unlimited)
    val query = PlannerQuery(
      QueryGraph(
        patternNodes = Set(IdName("a"), IdName("b")),
        patternRelationships = Set(patternRel)
      )
    )
    implicit val logicalPlanContext = newMockedLogicalPlanningContext(
      planContext= newMockedPlanContext
    )

    a [CantHandleQueryException] should be thrownBy {
      verifyBestPlan(greedyPlanTableWith(getSimpleLogicalPlanWithAandB()).uniquePlan, query)
    }
  }

  test("should not throw when finding plan that does solve all pattern nodes") {
    val query = PlannerQuery(
      QueryGraph(
        patternNodes = Set(IdName("a"), IdName("b"))
      )
    )
    implicit val logicalPlanContext = newMockedLogicalPlanningContext(
      planContext = newMockedPlanContext)

    verifyBestPlan(greedyPlanTableWith(getSimpleLogicalPlanWithAandB()).uniquePlan, query).availableSymbols should equal(Set(IdName("a"), IdName("b")))
  }

  test("should throw when finding plan that contains unfulfillable index hint") {
    implicit val logicalPlanContext = newMockedLogicalPlanningContext(
      planContext = getPlanContext(false), useErrorsOverWarnings = true)

    a [IndexHintException] should be thrownBy {
      verifyBestPlan(greedyPlanTableWith(getSimpleLogicalPlanWithAandB()).uniquePlan, getQueryWithIdxHint())
    }
  }

  test("should issue warning when finding plan that contains unfulfillable index hint") {
    val notificationLogger = new RecordingNotificationLogger
    implicit val logicalPlanContext = newMockedLogicalPlanningContext(
      planContext = getPlanContext(false), useErrorsOverWarnings = false,
      notificationLogger = notificationLogger)

    verifyBestPlan(greedyPlanTableWith(getSimpleLogicalPlanWithAandB()).uniquePlan, getQueryWithIdxHint()).availableSymbols should equal(Set(IdName("a"), IdName("b")))
    notificationLogger.notifications should contain(IndexHintUnfulfillableNotification)
  }

  test("should succeed when finding plan that contains fulfillable index hint") {
    val notificationLogger = new RecordingNotificationLogger
    implicit val logicalPlanContext = newMockedLogicalPlanningContext(
      planContext = getPlanContext(true), useErrorsOverWarnings = false,
      notificationLogger = notificationLogger)
    val plan: LogicalPlan = newMockedLogicalPlan(Set(IdName("a"), IdName("b")), hints = Set[Hint](getIndexHint()))

    verifyBestPlan(greedyPlanTableWith(plan).uniquePlan, getQueryWithIdxHint()).availableSymbols should equal(Set(IdName("a"), IdName("b")))
    notificationLogger.notifications should be(empty)
  }

  test("should throw when finding plan that does not contain a fulfillable index hint") {
    implicit val logicalPlanContext = newMockedLogicalPlanningContext(
      planContext = getPlanContext(true), useErrorsOverWarnings = false)

    a [CantHandleQueryException] should be thrownBy {
      verifyBestPlan(greedyPlanTableWith(getSimpleLogicalPlanWithAandB()).uniquePlan, getQueryWithIdxHint())
    }
  }
}
