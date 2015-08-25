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
package org.neo4j.cypher.internal.compiler.v2_3

import org.neo4j.cypher.internal.compiler.v2_3.CompilationPhaseTracer.CompilationPhase._
import org.neo4j.cypher.internal.compiler.v2_3.codegen.{CodeGenerator, CodeStructure}
import org.neo4j.cypher.internal.compiler.v2_3.executionplan.{CompiledPlan, GeneratedQuery, NewRuntimeSuccessRateMonitor, PipeInfo}
import org.neo4j.cypher.internal.compiler.v2_3.helpers._
import org.neo4j.cypher.internal.compiler.v2_3.planner.CantCompileQueryException
import org.neo4j.cypher.internal.compiler.v2_3.planner.execution.{PipeExecutionBuilderContext, PipeExecutionPlanBuilder}
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.compiler.v2_3.spi.PlanContext
import org.neo4j.cypher.internal.frontend.v2_3.notification.RuntimeUnsupportedNotification
import org.neo4j.cypher.internal.frontend.v2_3.{InternalException, InvalidArgumentException, SemanticTable}
import org.neo4j.helpers.Clock

object RuntimeBuilder {
  def create(runtimeName: Option[RuntimeName], interpretedProducer: InterpretedPlanBuilder,
            compiledProducer: CompiledPlanBuilder, useErrorsOverWarnings: Boolean) = runtimeName match {
    case None | Some(InterpretedRuntimeName) => InterpretedRuntimeBuilder(interpretedProducer)
    case Some(CompiledRuntimeName) if useErrorsOverWarnings => ErrorReportingRuntimeBuilder(compiledProducer)
    case Some(CompiledRuntimeName) => WarningFallbackRuntimeBuilder(interpretedProducer, compiledProducer)
  }
}
trait RuntimeBuilder {

  def apply(logicalPlan: LogicalPlan, pipeBuildContext: PipeExecutionBuilderContext, planContext: PlanContext,
            tracer: CompilationPhaseTracer, semanticTable: SemanticTable,
            monitor: NewRuntimeSuccessRateMonitor, plannerName: PlannerName,
            preparedQuery: PreparedQuery): Either[CompiledPlan, PipeInfo] = {
    try {
      Left(compiledProducer(logicalPlan, semanticTable, planContext, monitor, tracer, plannerName))
    } catch {
      case e: CantCompileQueryException =>
        monitor.unableToHandlePlan(logicalPlan, e)
        fallback(preparedQuery)
        Right(interpretedProducer(logicalPlan, pipeBuildContext, planContext, tracer))
    }
  }

  def compiledProducer: CompiledPlanBuilder

  def interpretedProducer: InterpretedPlanBuilder

  def fallback(preparedQuery: PreparedQuery): Unit
}

case class SilentFallbackRuntimeBuilder(interpretedProducer: InterpretedPlanBuilder, compiledProducer: CompiledPlanBuilder)
  extends RuntimeBuilder {

  override def fallback(preparedQuery: PreparedQuery): Unit = {}
}

case class WarningFallbackRuntimeBuilder(interpretedProducer: InterpretedPlanBuilder, compiledProducer: CompiledPlanBuilder)
  extends RuntimeBuilder {

  override def fallback(preparedQuery: PreparedQuery): Unit = preparedQuery.notificationLogger
    .log(RuntimeUnsupportedNotification)
}

case class InterpretedRuntimeBuilder(interpretedProducer: InterpretedPlanBuilder) extends RuntimeBuilder {

  override def apply(logicalPlan: LogicalPlan, pipeBuildContext: PipeExecutionBuilderContext, planContext: PlanContext,
                     tracer: CompilationPhaseTracer, semanticTable: SemanticTable,
                     monitor: NewRuntimeSuccessRateMonitor, plannerName: PlannerName,
                     preparedQuery: PreparedQuery): Either[CompiledPlan, PipeInfo] = {
    Right(interpretedProducer.apply(logicalPlan, pipeBuildContext, planContext, tracer))
  }

  override def compiledProducer = throw new InternalException("This should never be called")

  override def fallback(preparedQuery: PreparedQuery) = throw new InternalException("This should never be called")
}

case class ErrorReportingRuntimeBuilder(compiledProducer: CompiledPlanBuilder) extends RuntimeBuilder {

  override def interpretedProducer = throw new InternalException("This should never be called")

  override def fallback(preparedQuery: PreparedQuery) = throw new
      InvalidArgumentException("The given query is not currently supported in the selected runtime")
}

case class InterpretedPlanBuilder(clock: Clock, monitors: Monitors) {

  def apply(logicalPlan: LogicalPlan, pipeBuildContext: PipeExecutionBuilderContext,
            planContext: PlanContext, tracer: CompilationPhaseTracer) =
    closing(tracer.beginPhase(PIPE_BUILDING)) {
      new PipeExecutionPlanBuilder(clock, monitors).build(logicalPlan)(pipeBuildContext, planContext)
    }
}

case class CompiledPlanBuilder(clock: Clock, structure:CodeStructure[GeneratedQuery]) {

  private val codeGen = new CodeGenerator(structure)

  def apply(logicalPlan: LogicalPlan, semanticTable: SemanticTable, planContext: PlanContext,
            monitor: NewRuntimeSuccessRateMonitor, tracer: CompilationPhaseTracer,
            plannerName: PlannerName) = {
    monitor.newPlanSeen(logicalPlan)
    closing(tracer.beginPhase(CODE_GENERATION)) {
      codeGen.generate(logicalPlan, planContext, clock, semanticTable, plannerName)
    }
  }
}
