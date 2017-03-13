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
package org.neo4j.cypher.internal.compiled_runtime.v3_2

import org.neo4j.cypher.internal.compiled_runtime.v3_2.ExecutionPlanBuilder.DescriptionProvider
import org.neo4j.cypher.internal.compiled_runtime.v3_2.codegen._
import org.neo4j.cypher.internal.compiled_runtime.v3_2.codegen.profiling.ProfilingTracer
import org.neo4j.cypher.internal.compiler.v3_2._
import org.neo4j.cypher.internal.compiler.v3_2.executionplan._
import org.neo4j.cypher.internal.compiler.v3_2.phases._
import org.neo4j.cypher.internal.compiler.v3_2.planDescription.InternalPlanDescription
import org.neo4j.cypher.internal.compiler.v3_2.planDescription.InternalPlanDescription.Arguments
import org.neo4j.cypher.internal.compiler.v3_2.planner.CantCompileQueryException
import org.neo4j.cypher.internal.compiler.v3_2.spi.{GraphStatistics, PlanContext, QueryContext}
import org.neo4j.cypher.internal.frontend.v3_2.PlannerName
import org.neo4j.cypher.internal.frontend.v3_2.notification.InternalNotification
import org.neo4j.cypher.internal.frontend.v3_2.phases.CompilationPhaseTracer.CompilationPhase.CODE_GENERATION
import org.neo4j.cypher.internal.frontend.v3_2.phases.Phase

object BuildCompiledExecutionPlan extends Phase[CompiledRuntimeContext, CompilationState, CompilationState] {

  override def phase = CODE_GENERATION

  override def description = "creates runnable byte code"

  override def postConditions = Set.empty// Can't yet guarantee that we can build an execution plan

  override def process(from: CompilationState, context: CompiledRuntimeContext): CompilationState = {
    val runtimeSuccessRateMonitor = context.monitors.newMonitor[NewRuntimeSuccessRateMonitor]()
    try {
      val codeGen = new CodeGenerator(context.codeStructure, context.clock, CodeGenConfiguration(context.debugOptions))
      val compiled: CompiledPlan = codeGen.generate(from.logicalPlan, context.planContext, from.semanticTable, from.plannerName)
      val executionPlan: ExecutionPlan = createExecutionPlan(context, compiled)
      runtimeSuccessRateMonitor.newPlanSeen(from.logicalPlan)
      from.copy(maybeExecutionPlan = Some(executionPlan))
    } catch {
      case e: CantCompileQueryException =>
        runtimeSuccessRateMonitor.unableToHandlePlan(from.logicalPlan, e)
        from.copy(maybeExecutionPlan = None)
    }
  }

  private def createExecutionPlan(context: CompiledRuntimeContext, compiled: CompiledPlan) = new ExecutionPlan {
    private val fingerprint = context.createFingerprintReference(compiled.fingerprint)

    override def isStale(lastTxId: () => Long, statistics: GraphStatistics): Boolean = fingerprint.isStale(lastTxId, statistics)

    override def run(queryContext: QueryContext,
                     executionMode: ExecutionMode, params: Map[String, Any]): InternalExecutionResult = {
      val taskCloser = new TaskCloser
      taskCloser.addTask(queryContext.transactionalContext.close)
      try {
        if (executionMode == ExplainMode) {
          //close all statements
          taskCloser.close(success = true)
          ExplainExecutionResult(compiled.columns.toList,
            compiled.planDescription, READ_ONLY, context.notificationLogger.notifications)
        } else
          compiled.executionResultBuilder(queryContext, executionMode, createTracer(executionMode, queryContext), params, taskCloser)
      } catch {
        case (t: Throwable) =>
          taskCloser.close(success = false)
          throw t
      }
    }

    override def plannerUsed: PlannerName = compiled.plannerUsed

    override def isPeriodicCommit: Boolean = compiled.periodicCommit.isDefined

    override def runtimeUsed = CompiledRuntimeName

    override def notifications(planContext: PlanContext): Seq[InternalNotification] = Seq.empty

    override def plannedIndexUsage: Seq[IndexUsage] = compiled.plannedIndexUsage
  }

  private def createTracer(mode: ExecutionMode, queryContext: QueryContext): DescriptionProvider = mode match {
    case ProfileMode =>
      val tracer = new ProfilingTracer(queryContext.kernelStatisticProvider())
      (description: InternalPlanDescription) =>
        (new Provider[InternalPlanDescription] {

          override def get(): InternalPlanDescription = description.map {
            plan: InternalPlanDescription =>
              val data = tracer.get(plan.id)
              plan.
                addArgument(Arguments.DbHits(data.dbHits())).
                addArgument(Arguments.PageCacheHits(data.pageCacheHits())).
                addArgument(Arguments.PageCacheMisses(data.pageCacheMisses())).
                addArgument(Arguments.Rows(data.rows())).
                addArgument(Arguments.Time(data.time()))
          }
        }, Some(tracer))
    case _ => (description: InternalPlanDescription) =>
      (new Provider[InternalPlanDescription] {
        override def get(): InternalPlanDescription = description
      }, None)
  }
}
