/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_3

import org.neo4j.cypher.internal.compiler.v2_3.CompilationPhaseTracer.CompilationPhase._
import org.neo4j.cypher.internal.compiler.v2_3.executionplan.{NewRuntimeSuccessRateMonitor, PipeInfo}
import org.neo4j.cypher.internal.compiler.v2_3.helpers._
import org.neo4j.cypher.internal.compiler.v2_3.planner.execution.{PipeExecutionBuilderContext, PipeExecutionPlanBuilder}
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.compiler.v2_3.spi.PlanContext
import org.neo4j.cypher.internal.frontend.v2_3.SemanticTable
import org.neo4j.helpers.Clock

object RuntimeBuilder {
  def create(runtimeName: Option[RuntimeName], interpretedProducer: InterpretedPlanBuilder) = InterpretedRuntimeBuilder(interpretedProducer)
}

trait RuntimeBuilder {

  def apply(logicalPlan: LogicalPlan, pipeBuildContext: PipeExecutionBuilderContext, planContext: PlanContext,
            tracer: CompilationPhaseTracer, semanticTable: SemanticTable,
            monitor: NewRuntimeSuccessRateMonitor, plannerName: PlannerName,
            preparedQuery: PreparedQuery): PipeInfo
}

case class InterpretedRuntimeBuilder(interpretedProducer: InterpretedPlanBuilder) extends RuntimeBuilder {

  override def apply(logicalPlan: LogicalPlan, pipeBuildContext: PipeExecutionBuilderContext, planContext: PlanContext,
                     tracer: CompilationPhaseTracer, semanticTable: SemanticTable,
                     monitor: NewRuntimeSuccessRateMonitor, plannerName: PlannerName,
                     preparedQuery: PreparedQuery) = {
    interpretedProducer.apply(logicalPlan, pipeBuildContext, planContext, tracer)
  }
}

case class InterpretedPlanBuilder(clock: Clock, monitors: Monitors) {

  def apply(logicalPlan: LogicalPlan, pipeBuildContext: PipeExecutionBuilderContext,
            planContext: PlanContext, tracer: CompilationPhaseTracer) =
    closing(tracer.beginPhase(PIPE_BUILDING)) {
      new PipeExecutionPlanBuilder(clock, monitors).build(logicalPlan)(pipeBuildContext, planContext)
    }
}
