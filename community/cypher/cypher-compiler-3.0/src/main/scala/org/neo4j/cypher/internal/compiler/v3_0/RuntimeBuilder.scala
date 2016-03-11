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
package org.neo4j.cypher.internal.compiler.v3_0

import org.neo4j.cypher.internal.compiler.v3_0.CompilationPhaseTracer.CompilationPhase._
import org.neo4j.cypher.internal.compiler.v3_0.CompiledPlanBuilder.createTracer
import org.neo4j.cypher.internal.compiler.v3_0.codegen.profiling.ProfilingTracer
import org.neo4j.cypher.internal.compiler.v3_0.codegen.{CodeGenerator, CodeStructure}
import org.neo4j.cypher.internal.compiler.v3_0.executionplan.ExecutionPlanBuilder.DescriptionProvider
import org.neo4j.cypher.internal.compiler.v3_0.executionplan.InterpretedExecutionPlanBuilder.interpretedToExecutionPlan
import org.neo4j.cypher.internal.compiler.v3_0.executionplan.{ExecutionPlan, GeneratedQuery, InternalExecutionResult, NewRuntimeSuccessRateMonitor, PlanFingerprint, PlanFingerprintReference, Provider, READ_ONLY}
import org.neo4j.cypher.internal.compiler.v3_0.helpers._
import org.neo4j.cypher.internal.compiler.v3_0.planDescription.InternalPlanDescription
import org.neo4j.cypher.internal.compiler.v3_0.planDescription.InternalPlanDescription.Arguments
import org.neo4j.cypher.internal.compiler.v3_0.planner.{PeriodicCommit, CantCompileQueryException}
import org.neo4j.cypher.internal.compiler.v3_0.planner.execution.{PipeExecutionBuilderContext, PipeExecutionPlanBuilder}
import org.neo4j.cypher.internal.compiler.v3_0.planner.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.compiler.v3_0.spi.{GraphStatistics, PlanContext, QueryContext}
import org.neo4j.cypher.internal.frontend.v3_0.notification.{InternalNotification, RuntimeUnsupportedNotification}
import org.neo4j.cypher.internal.frontend.v3_0.{InternalException, InvalidArgumentException, SemanticTable}
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

  def apply(periodicCommit: Option[PeriodicCommit], logicalPlan: LogicalPlan, pipeBuildContext: PipeExecutionBuilderContext,
            planContext: PlanContext, tracer: CompilationPhaseTracer, semanticTable: SemanticTable,
            monitor: NewRuntimeSuccessRateMonitor, plannerName: PlannerName,
            preparedQuery: PreparedQuerySemantics,
            createFingerprintReference: Option[PlanFingerprint] => PlanFingerprintReference,
            config: CypherCompilerConfiguration): ExecutionPlan = {
    try {
      compiledProducer(logicalPlan, semanticTable, planContext, monitor, tracer,
        plannerName, preparedQuery, createFingerprintReference)
    } catch {
      case e: CantCompileQueryException =>
        monitor.unableToHandlePlan(logicalPlan, e)
        fallback(preparedQuery)
        interpretedProducer
          .apply(periodicCommit, logicalPlan, pipeBuildContext, planContext, tracer, preparedQuery, createFingerprintReference, config)
    }
  }

  def compiledProducer: CompiledPlanBuilder

  def interpretedProducer: InterpretedPlanBuilder

  def fallback(preparedQuery: PreparedQuerySemantics): Unit
}

case class SilentFallbackRuntimeBuilder(interpretedProducer: InterpretedPlanBuilder, compiledProducer: CompiledPlanBuilder)
  extends RuntimeBuilder {

  override def fallback(preparedQuery: PreparedQuerySemantics): Unit = {}
}

case class WarningFallbackRuntimeBuilder(interpretedProducer: InterpretedPlanBuilder, compiledProducer: CompiledPlanBuilder)
  extends RuntimeBuilder {

  override def fallback(preparedQuery: PreparedQuerySemantics): Unit = preparedQuery.notificationLogger
    .log(RuntimeUnsupportedNotification)
}

case class InterpretedRuntimeBuilder(interpretedProducer: InterpretedPlanBuilder) extends RuntimeBuilder {
  override def apply(periodicCommit: Option[PeriodicCommit], logicalPlan: LogicalPlan, pipeBuildContext: PipeExecutionBuilderContext,
                     planContext: PlanContext, tracer: CompilationPhaseTracer, semanticTable: SemanticTable,
                     monitor: NewRuntimeSuccessRateMonitor, plannerName: PlannerName,
                     preparedQuery: PreparedQuerySemantics,
                     createFingerprintReference: Option[PlanFingerprint] => PlanFingerprintReference,
                     config: CypherCompilerConfiguration): ExecutionPlan =
    interpretedProducer(periodicCommit, logicalPlan, pipeBuildContext, planContext, tracer, preparedQuery, createFingerprintReference, config)


  override def compiledProducer = throw new InternalException("This should never be called")

  override def fallback(preparedQuery: PreparedQuerySemantics) = throw new InternalException("This should never be called")
}

case class ErrorReportingRuntimeBuilder(compiledProducer: CompiledPlanBuilder) extends RuntimeBuilder {

  override def interpretedProducer = throw new InternalException("This should never be called")

  override def fallback(preparedQuery: PreparedQuerySemantics) = throw new
      InvalidArgumentException("The given query is not currently supported in the selected runtime")
}

case class InterpretedPlanBuilder(clock: Clock, monitors: Monitors) {

  def apply(periodicCommit: Option[PeriodicCommit], logicalPlan: LogicalPlan, pipeBuildContext: PipeExecutionBuilderContext,
            planContext: PlanContext, tracer: CompilationPhaseTracer, preparedQuery: PreparedQuerySemantics,
            createFingerprintReference: Option[PlanFingerprint] => PlanFingerprintReference,
            config: CypherCompilerConfiguration) =
    closing(tracer.beginPhase(PIPE_BUILDING)) {
      interpretedToExecutionPlan(new PipeExecutionPlanBuilder(clock, monitors).build(periodicCommit, logicalPlan)(pipeBuildContext, planContext), planContext, preparedQuery, createFingerprintReference, config)
    }
}

case class CompiledPlanBuilder(clock: Clock, structure:CodeStructure[GeneratedQuery]) {

  private val codeGen = new CodeGenerator(structure)

  def apply(logicalPlan: LogicalPlan, semanticTable: SemanticTable, planContext: PlanContext,
            monitor: NewRuntimeSuccessRateMonitor, tracer: CompilationPhaseTracer,
            plannerName: PlannerName,
            preparedQuery: PreparedQuerySemantics,
            createFingerprintReference:Option[PlanFingerprint]=>PlanFingerprintReference): ExecutionPlan = {
            monitor.newPlanSeen(logicalPlan)
    closing(tracer.beginPhase(CODE_GENERATION)) {
      val compiled = codeGen.generate(logicalPlan, planContext, clock, semanticTable, plannerName)

      new ExecutionPlan {
        val fingerprint = createFingerprintReference(compiled.fingerprint)

        def isStale(lastTxId: () => Long, statistics: GraphStatistics) = fingerprint.isStale(lastTxId, statistics)

        def run(queryContext: QueryContext,
                executionMode: ExecutionMode, params: Map[String, Any]): InternalExecutionResult = {
          val taskCloser = new TaskCloser
          taskCloser.addTask(queryContext.transactionalContext.close)
          try {
            if (executionMode == ExplainMode) {
              //close all statements
              taskCloser.close(success = true)
              new ExplainExecutionResult(compiled.columns.toList,
                compiled.planDescription, READ_ONLY, preparedQuery.notificationLogger.notifications)
            } else
              compiled.executionResultBuilder(queryContext, executionMode, createTracer(executionMode), params, taskCloser)
          } catch {
            case (t: Throwable) =>
              taskCloser.close(success = false)
              throw t
          }
        }

        def plannerUsed: PlannerName = compiled.plannerUsed

        def isPeriodicCommit: Boolean = compiled.periodicCommit.isDefined

        def runtimeUsed = CompiledRuntimeName

        override def notifications(planContext: PlanContext): Seq[InternalNotification] = Seq.empty
      }
    }
  }
}

object CompiledPlanBuilder {

  def createTracer( mode: ExecutionMode ) : DescriptionProvider = mode match {
    case ProfileMode =>
      val tracer = new ProfilingTracer()
      (description: InternalPlanDescription) => (new Provider[InternalPlanDescription] {

        override def get(): InternalPlanDescription = description.map {
          plan: InternalPlanDescription =>
            val data = tracer.get(plan.id)
            plan.
              addArgument(Arguments.DbHits(data.dbHits())).
              addArgument(Arguments.Rows(data.rows())).
              addArgument(Arguments.Time(data.time()))
        }
      }, Some(tracer))
    case _ => (description: InternalPlanDescription) => (new Provider[InternalPlanDescription] {
      override def get(): InternalPlanDescription = description
    }, None)
  }
}
