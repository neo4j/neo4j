/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v3_0.executionplan

import org.mockito.Mockito._
import org.neo4j.cypher.internal.compiler.v3_0.planner.CantHandleQueryException
import org.neo4j.cypher.internal.compiler.v3_0.spi.PlanContext
import org.neo4j.cypher.internal.compiler.v3_0.{CompilationPhaseTracer, PreparedQuerySemantics, RecordingNotificationLogger}
import org.neo4j.cypher.internal.frontend.v3_0.notification.PlannerUnsupportedNotification
import org.neo4j.cypher.internal.frontend.v3_0.parser.CypherParser
import org.neo4j.cypher.internal.frontend.v3_0.test_helpers.CypherFunSuite

class FallbackPlanBuilderTest extends CypherFunSuite {

  val parser = new CypherParser

  test("should delegate var length to old pipe builder") {
    new uses("MATCH ()-[r*]->() RETURN r") {
      result should equal(executionPlan)
      assertUsed(newBuilder)
    }
  }

  test("should delegate plain shortest path to new pipe builder") {
    new uses("MATCH shortestPath(()-[r*]->()) RETURN r") {
      result should equal(executionPlan)
      assertUsed(newBuilder)
    }
  }

  test("should delegate shortest path with var length expressions to old pipe builder") {
    new uses("MATCH shortestPath(()-[r*]->({x: ()-[:T*]->()})) RETURN r") {
      result should equal(executionPlan)
      assertUsed(newBuilder)
    }
  }

  test("should warn if falling back from a specified plan") {
    val preparedQuery = new PreparedQuerySemantics(null, null, null, null, null, null)(new RecordingNotificationLogger)
    val builder = mock[ExecutablePlanBuilder]
    when(builder.producePlan(preparedQuery, null, null, null)).thenThrow(classOf[CantHandleQueryException])
    WarningFallbackPlanBuilder(mock[ExecutablePlanBuilder], builder, mock[NewLogicalPlanSuccessRateMonitor])
      .producePlan(preparedQuery, null, null, null)

    preparedQuery.notificationLogger.notifications should contain(PlannerUnsupportedNotification)
  }

  test("should not warn if falling back from fallback plan") {
    val preparedQuery = new PreparedQuerySemantics(null, null, null, null, null, null)(new RecordingNotificationLogger)
    val builder = mock[ExecutablePlanBuilder]
    when(builder.producePlan(preparedQuery, null, null, null)).thenThrow(classOf[CantHandleQueryException])
    SilentFallbackPlanBuilder(mock[ExecutablePlanBuilder], builder, mock[NewLogicalPlanSuccessRateMonitor])
      .producePlan(preparedQuery, null, null, null)

    preparedQuery.notificationLogger.notifications should not contain PlannerUnsupportedNotification

  }

  class uses(queryText: String) {
    // given
    val planContext = mock[PlanContext]
    val nodeManager = mock[EntityAccessor]
    val createFingerprintReference: (Option[PlanFingerprint]) => PlanFingerprintReference = (fp) => mock[PlanFingerprintReference]
    val oldBuilder = mock[ExecutablePlanBuilder]
    val newBuilder = mock[ExecutablePlanBuilder]
    val pipeBuilder = new SilentFallbackPlanBuilder(oldBuilder, newBuilder, mock[NewLogicalPlanSuccessRateMonitor])
    val preparedQuery = PreparedQuerySemantics(parser.parse(queryText), queryText, None, Map.empty, null, null)(null, null, Set.empty)
    val executionPlan = mock[ExecutionPlan]
    when( oldBuilder.producePlan(preparedQuery, planContext, CompilationPhaseTracer.NO_TRACING, createFingerprintReference) ).thenReturn(executionPlan)
    when( newBuilder.producePlan(preparedQuery, planContext, CompilationPhaseTracer.NO_TRACING, createFingerprintReference) ).thenReturn(executionPlan)

    def result = pipeBuilder.producePlan(preparedQuery, planContext, CompilationPhaseTracer.NO_TRACING, createFingerprintReference)

    def assertUsed(used: ExecutablePlanBuilder) = {
      val notUsed = if (used == oldBuilder) newBuilder else oldBuilder
      verify( used ).producePlan(preparedQuery, planContext, CompilationPhaseTracer.NO_TRACING, createFingerprintReference)
      verifyNoMoreInteractions( used )
      verifyZeroInteractions( notUsed )
    }
  }
}
