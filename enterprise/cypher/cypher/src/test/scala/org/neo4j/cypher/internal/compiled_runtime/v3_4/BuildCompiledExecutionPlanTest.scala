/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.cypher.internal.compiled_runtime.v3_4

import org.neo4j.cypher.internal.compatibility.v3_4.WrappedMonitors
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.compiled.BuildCompiledExecutionPlan
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.executionplan.NewRuntimeSuccessRateMonitor
import org.neo4j.cypher.internal.compiler.v3_4.NotImplementedPlanContext
import org.neo4j.cypher.internal.compiler.v3_4.phases.LogicalPlanState
import org.neo4j.cypher.internal.compiler.v3_4.planner.{CantCompileQueryException, HardcodedGraphStatistics}
import org.neo4j.cypher.internal.frontend.v3_4.semantics.SemanticTable
import org.neo4j.cypher.internal.planner.v3_4.spi.PlanningAttributes.{Cardinalities, Solveds}
import org.neo4j.cypher.internal.planner.v3_4.spi.{CostBasedPlannerName, GraphStatistics}
import org.neo4j.cypher.internal.spi.v3_4.codegen.GeneratedQueryStructure
import org.neo4j.cypher.internal.util.v3_4.attribution.SequentialIdGen
import org.neo4j.cypher.internal.util.v3_4.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.v3_4.logical.plans.{Argument, LogicalPlan, ProduceResult}
import org.neo4j.kernel.monitoring.Monitors

class BuildCompiledExecutionPlanTest extends CypherFunSuite {

  implicit val idGen = new SequentialIdGen()

  test("should tell the monitor when building works") {
    // Given
    val monitors = WrappedMonitors(new Monitors)
    val monitor = new SpyingMonitor
    monitors.addMonitorListener(monitor)

    // When
    process(monitors, ProduceResult(Argument(), Seq.empty))

    // Then
    monitor.successfullyPlanned should equal(true)
  }

  test("should tell the monitor when building does not work") {
    // Given
    val monitors = WrappedMonitors(new Monitors)
    val monitor = new SpyingMonitor
    monitors.addMonitorListener(monitor)

    process(monitors, Argument())

    // Then
    monitor.failedToPlan should equal(true)
  }

  private def process(monitors: WrappedMonitors, plan: LogicalPlan) = {
    val context = codegen.CompiledRuntimeContextHelper.create(
      monitors = monitors,
      planContext = new NotImplementedPlanContext {
        override def statistics: GraphStatistics = HardcodedGraphStatistics
      }, codeStructure = GeneratedQueryStructure)

    val state = LogicalPlanState("apa", None, CostBasedPlannerName.default, new Solveds, new Cardinalities,
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
