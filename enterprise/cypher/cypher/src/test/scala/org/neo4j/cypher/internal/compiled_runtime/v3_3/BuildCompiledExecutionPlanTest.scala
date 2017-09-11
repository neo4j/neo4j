/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.compiled_runtime.v3_3

import org.neo4j.cypher.internal.compatibility.v3_3.WrappedMonitors
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.compiled.BuildCompiledExecutionPlan
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.executionplan.NewRuntimeSuccessRateMonitor
import org.neo4j.cypher.internal.compiler.v3_3.phases.LogicalPlanState
import org.neo4j.cypher.internal.compiler.v3_3.planner.CantCompileQueryException
import org.neo4j.cypher.internal.compiler.v3_3.spi.GraphStatistics
import org.neo4j.cypher.internal.compiler.v3_3.{CostBasedPlannerName, HardcodedGraphStatistics, NotImplementedPlanContext}
import org.neo4j.cypher.internal.frontend.v3_3.SemanticTable
import org.neo4j.cypher.internal.frontend.v3_3.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.ir.v3_3.{CardinalityEstimation, QueryGraph, RegularPlannerQuery}
import org.neo4j.cypher.internal.spi.v3_3.codegen.GeneratedQueryStructure
import org.neo4j.cypher.internal.v3_3.logical.plans.{LogicalPlan, ProduceResult, SingleRow}
import org.neo4j.kernel.monitoring.Monitors

class BuildCompiledExecutionPlanTest extends CypherFunSuite {

  private val solved = CardinalityEstimation.lift(RegularPlannerQuery(QueryGraph.empty), 0.0)

  test("should tell the monitor when building works") {
    // Given
    val monitors = WrappedMonitors(new Monitors)
    val monitor = new SpyingMonitor
    monitors.addMonitorListener(monitor)

    // When
    process(monitors, ProduceResult(Seq.empty, SingleRow()(solved)))

    // Then
    monitor.successfullyPlanned should equal(true)
  }

  test("should tell the monitor when building does not work") {
    // Given
    val monitors = WrappedMonitors(new Monitors)
    val monitor = new SpyingMonitor
    monitors.addMonitorListener(monitor)

    process(monitors, SingleRow()(solved))

    // Then
    monitor.failedToPlan should equal(true)
  }

  private def process(monitors: WrappedMonitors, plan: LogicalPlan) = {
    plan.assignIds()
    val context = codegen.CompiledRuntimeContextHelper.create(
      monitors = monitors,
      planContext = new NotImplementedPlanContext {
        override def statistics: GraphStatistics = HardcodedGraphStatistics
      }, codeStructure = GeneratedQueryStructure)

    val state = LogicalPlanState("apa", None, CostBasedPlannerName.default,
                                 maybeLogicalPlan = Some(plan), maybeSemanticTable = Some(new SemanticTable()))

    // When
    BuildCompiledExecutionPlan.process(state, context)
  }

  class SpyingMonitor extends NewRuntimeSuccessRateMonitor {
    var successfullyPlanned = false
    override def newPlanSeen(plan: LogicalPlan): Unit = successfullyPlanned = true

    var failedToPlan = false
    override def unableToHandlePlan(plan: LogicalPlan, origin: CantCompileQueryException): Unit = failedToPlan = true
  }
}
