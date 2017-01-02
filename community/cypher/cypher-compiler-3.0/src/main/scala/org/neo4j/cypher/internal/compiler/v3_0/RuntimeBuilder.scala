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
package org.neo4j.cypher.internal.compiler.v3_0

import java.time.Clock

import org.neo4j.cypher.internal.compiler.v3_0.CompilationPhaseTracer.CompilationPhase._
import org.neo4j.cypher.internal.compiler.v3_0.executionplan.InterpretedExecutionPlanBuilder.interpretedToExecutionPlan
import org.neo4j.cypher.internal.compiler.v3_0.executionplan.{ExecutionPlan, NewRuntimeSuccessRateMonitor, PlanFingerprint, PlanFingerprintReference}
import org.neo4j.cypher.internal.compiler.v3_0.helpers._
import org.neo4j.cypher.internal.compiler.v3_0.planner.PeriodicCommit
import org.neo4j.cypher.internal.compiler.v3_0.planner.execution.{PipeExecutionBuilderContext, PipeExecutionPlanBuilder}
import org.neo4j.cypher.internal.compiler.v3_0.planner.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.compiler.v3_0.spi.PlanContext
import org.neo4j.cypher.internal.frontend.v3_0.SemanticTable

object RuntimeBuilder {

  def create(runtimeName: Option[RuntimeName], interpretedProducer: InterpretedPlanBuilder) = InterpretedRuntimeBuilder(
    interpretedProducer)
}

trait RuntimeBuilder {

  def apply(periodicCommit: Option[PeriodicCommit], logicalPlan: LogicalPlan,
            pipeBuildContext: PipeExecutionBuilderContext,
            planContext: PlanContext, tracer: CompilationPhaseTracer, semanticTable: SemanticTable,
            monitor: NewRuntimeSuccessRateMonitor, plannerName: PlannerName,
            preparedQuery: PreparedQuerySemantics,
            createFingerprintReference: Option[PlanFingerprint] => PlanFingerprintReference,
            config: CypherCompilerConfiguration): ExecutionPlan
}

case class InterpretedRuntimeBuilder(interpretedProducer: InterpretedPlanBuilder) extends RuntimeBuilder {

  override def apply(periodicCommit: Option[PeriodicCommit], logicalPlan: LogicalPlan,
                     pipeBuildContext: PipeExecutionBuilderContext,
                     planContext: PlanContext, tracer: CompilationPhaseTracer, semanticTable: SemanticTable,
                     monitor: NewRuntimeSuccessRateMonitor, plannerName: PlannerName,
                     preparedQuery: PreparedQuerySemantics,
                     createFingerprintReference: Option[PlanFingerprint] => PlanFingerprintReference,
                     config: CypherCompilerConfiguration): ExecutionPlan =
    interpretedProducer(periodicCommit, logicalPlan, pipeBuildContext, planContext, tracer, preparedQuery,
                        createFingerprintReference, config)

}

case class InterpretedPlanBuilder(clock: Clock, monitors: Monitors, typeConverter: RuntimeTypeConverter) {

  def apply(periodicCommit: Option[PeriodicCommit], logicalPlan: LogicalPlan,
            pipeBuildContext: PipeExecutionBuilderContext,
            planContext: PlanContext, tracer: CompilationPhaseTracer, preparedQuery: PreparedQuerySemantics,
            createFingerprintReference: Option[PlanFingerprint] => PlanFingerprintReference,
            config: CypherCompilerConfiguration) =
    closing(tracer.beginPhase(PIPE_BUILDING)) {
      interpretedToExecutionPlan(new PipeExecutionPlanBuilder(clock, monitors)
        .build(periodicCommit, logicalPlan)(pipeBuildContext, planContext),
        planContext, preparedQuery, createFingerprintReference, config, typeConverter)
    }
}
