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
package org.neo4j.cypher.internal.compatibility.v3_3

import java.time.Clock

import org.neo4j.cypher.internal.compatibility.v3_4.{MonitoringCacheAccessor, WrappedMonitors => WrappedMonitorsV3_4}
import org.neo4j.cypher.internal.compatibility.v3_4.runtime._
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.executionplan.{ExecutionPlan => ExecutionPlanV3_4}
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.phases.CompilationState
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.{CommunityRuntimeContext => CommunityRuntimeContextV3_4}
import org.neo4j.cypher.internal.compiler.v3_3
import org.neo4j.cypher.internal.compiler.v3_3.{CypherCompilerFactory, DPPlannerName => DPPlannerNameV3_3, IDPPlannerName => IDPPlannerNameV3_3}
import org.neo4j.cypher.internal.compiler.v3_4._
import org.neo4j.cypher.internal.compiler.v3_4.phases.LogicalPlanState
import org.neo4j.cypher.internal.frontend.v3_3.ast.{Statement => StatementV3_3}
import org.neo4j.cypher.internal.frontend.v3_3.phases.{Monitors => MonitorsV3_3}
import org.neo4j.cypher.internal.frontend.v3_4.phases.{Monitors, Transformer}
import org.neo4j.cypher.{CypherPlanner, CypherRuntime, CypherUpdateStrategy}
import org.neo4j.kernel.monitoring.{Monitors => KernelMonitors}
import org.neo4j.logging.Log

case class CostCompatibility[CONTEXT3_3 <: v3_3.phases.CompilerContext,
CONTEXT3_4 <: CommunityRuntimeContextV3_4,
T <: Transformer[CONTEXT3_4, LogicalPlanState, CompilationState]](configV3_4: CypherCompilerConfiguration,
                                                                  clock: Clock,
                                                                  kernelMonitors: KernelMonitors,
                                                                  log: Log,
                                                                  planner: CypherPlanner,
                                                                  runtime: CypherRuntime,
                                                                  updateStrategy: CypherUpdateStrategy,
                                                                  runtimeBuilder: RuntimeBuilder[T],
                                                                  contextCreatorV3_3: v3_3.ContextCreator[CONTEXT3_3],
                                                                  contextCreatorV3_4: ContextCreator[CONTEXT3_4])
  extends Compatibility[CONTEXT3_3, CONTEXT3_4, T] {

  assert(contextCreatorV3_3 != null && contextCreatorV3_4 != null)
  override val maybePlannerName: Option[v3_3.CostBasedPlannerName] = planner match {
    case CypherPlanner.default => None
    case CypherPlanner.cost | CypherPlanner.idp => Some(IDPPlannerNameV3_3)
    case CypherPlanner.dp => Some(DPPlannerNameV3_3)
    case _ => throw new IllegalArgumentException(s"unknown cost based planner: ${planner.name}")
  }

  override val maybeUpdateStrategy: Option[v3_3.UpdateStrategy] = updateStrategy match {
    case CypherUpdateStrategy.eager => Some(v3_3.eagerUpdateStrategy)
    case _ => None
  }

  override val maybeRuntimeName: Option[RuntimeName] = runtime match {
    case CypherRuntime.default => None
    case CypherRuntime.interpreted => Some(InterpretedRuntimeName)
    case CypherRuntime.slotted => Some(SlottedRuntimeName)
    case CypherRuntime.morsel => Some(MorselRuntimeName)
    case CypherRuntime.compiled => Some(CompiledRuntimeName)
  }

  protected override val compiler: v3_3.CypherCompiler[CONTEXT3_3] = {

    val monitors: MonitorsV3_3 = WrappedMonitors(kernelMonitors)

    new CypherCompilerFactory().costBasedCompiler(configV3_3, clock, monitors, rewriterSequencer,
      maybePlannerName, maybeUpdateStrategy, contextCreatorV3_3)
  }

  override val logger = new StringInfoLogger(log)

  override val monitorsV3_3: MonitorsV3_3 = WrappedMonitors(kernelMonitors)

  override val monitorsV3_4: Monitors= WrappedMonitorsV3_4(kernelMonitors)

  override val cacheMonitor: AstCacheMonitor = monitorsV3_3.newMonitor[AstCacheMonitor](monitorTag)

  override val cacheAccessor: MonitoringCacheAccessor[StatementV3_3, ExecutionPlanV3_4] = new MonitoringCacheAccessor[StatementV3_3, ExecutionPlanV3_4](cacheMonitor)
  monitorsV3_3.addMonitorListener(logStalePlanRemovalMonitor(logger), monitorTag)
}
