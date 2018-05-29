/*
 * Copyright (c) 2002-2018 "Neo4j,"
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

  override def postConditions = Set.empty // Can't yet guarantee that we can build an execution plan

  override def process(from: CompilationState, context: CompiledRuntimeContext): CompilationState = {
    val runtimeSuccessRateMonitor = context.monitors.newMonitor[NewRuntimeSuccessRateMonitor]()
    try {
      val codeGen = new CodeGenerator(context.codeStructure, context.clock, CodeGenConfiguration(context.debugOptions))
      val compiled: CompiledPlan = codeGen.generate(from.logicalPlan, context.planContext, from.semanticTable(), from.plannerName)
      val executionPlan: ExecutionPlan =
        new CompiledExecutionPlan(compiled,
                                  context.createFingerprintReference(compiled.fingerprint),
                                  notifications(context))
      runtimeSuccessRateMonitor.newPlanSeen(from.logicalPlan)
      from.copy(maybeExecutionPlan = Some(executionPlan))
    } catch {
      case e: CantCompileQueryException =>
        runtimeSuccessRateMonitor.unableToHandlePlan(from.logicalPlan, e)
        from.copy(maybeExecutionPlan = None)
    }
  }

  private def notifications(context: CompiledRuntimeContext):Set[InternalNotification] =
    context.notificationLogger.notifications

  private def createTracer(mode: ExecutionMode, queryContext: QueryContext): DescriptionProvider = mode match {
    case ProfileMode =>
      val tracer = new ProfilingTracer(queryContext.transactionalContext.kernelStatisticProvider)
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

  /**
    * Execution plan for compiled runtime. Beware: will be cached.
    */
  class CompiledExecutionPlan(val compiled: CompiledPlan,
                              val fingerprint: PlanFingerprintReference,
                              val notifications: Set[InternalNotification]) extends ExecutionPlan {

    override def run(queryContext: QueryContext,
                     executionMode: ExecutionMode, params: Map[String, Any]): InternalExecutionResult = {
      val taskCloser = new TaskCloser
      taskCloser.addTask(queryContext.transactionalContext.close)
      try {
        if (executionMode == ExplainMode) {
          //close all statements
          taskCloser.close(success = true)
          ExplainExecutionResult(compiled.columns.toList, compiled.planDescription, READ_ONLY, notifications)
        } else
          compiled.executionResultBuilder(queryContext, executionMode, createTracer(executionMode, queryContext), params, taskCloser)
      } catch {
        case (t: Throwable) =>
          taskCloser.close(success = false)
          throw t
      }
    }

    override def isStale(lastTxId: () => Long, statistics: GraphStatistics) = fingerprint.isStale(lastTxId, statistics)

    override def runtimeUsed = CompiledRuntimeName

    override def notifications(planContext: PlanContext): Seq[InternalNotification] = Seq.empty

    override def plannedIndexUsage: Seq[IndexUsage] = compiled.plannedIndexUsage

    override def isPeriodicCommit: Boolean = compiled.periodicCommit.isDefined

    override def plannerUsed: PlannerName = compiled.plannerUsed
  }
}
