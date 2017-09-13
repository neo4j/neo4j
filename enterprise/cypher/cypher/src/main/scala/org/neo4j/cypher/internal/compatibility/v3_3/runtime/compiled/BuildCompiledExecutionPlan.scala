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
package org.neo4j.cypher.internal.compatibility.v3_3.runtime.compiled

import org.neo4j.cypher.internal.InternalExecutionResult
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.compiled.ExecutionPlanBuilder.DescriptionProvider
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.compiled.codegen._
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.executionplan._
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.helpers.InternalWrapping.asKernelNotification
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.phases.CompilationState
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.planDescription.InternalPlanDescription
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.planDescription.InternalPlanDescription.Arguments
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.{TaskCloser, _}
import org.neo4j.cypher.internal.compiler.v3_3.phases.LogicalPlanState
import org.neo4j.cypher.internal.compiler.v3_3.planner.CantCompileQueryException
import org.neo4j.cypher.internal.compiler.v3_3.spi.{GraphStatistics, PlanContext}
import org.neo4j.cypher.internal.frontend.v3_3.PlannerName
import org.neo4j.cypher.internal.frontend.v3_3.notification.InternalNotification
import org.neo4j.cypher.internal.frontend.v3_3.phases.CompilationPhaseTracer.CompilationPhase.CODE_GENERATION
import org.neo4j.cypher.internal.frontend.v3_3.phases.Phase
import org.neo4j.cypher.internal.spi.v3_3.QueryContext
import org.neo4j.cypher.internal.v3_3.codegen.profiling.ProfilingTracer
import org.neo4j.cypher.internal.v3_3.logical.plans.IndexUsage
import org.neo4j.values.virtual.MapValue

object BuildCompiledExecutionPlan extends Phase[EnterpriseRuntimeContext, LogicalPlanState, CompilationState] {

  override def phase = CODE_GENERATION

  override def description = "creates runnable byte code"

  override def postConditions = Set.empty// Can't yet guarantee that we can build an execution plan

  override def process(from: LogicalPlanState, context: EnterpriseRuntimeContext): CompilationState = {
    val runtimeSuccessRateMonitor = context.monitors.newMonitor[NewRuntimeSuccessRateMonitor]()
    try {
      val codeGen = new CodeGenerator(context.codeStructure, context.clock, CodeGenConfiguration(context.debugOptions))
      val compiled: CompiledPlan = codeGen.generate(from.logicalPlan, context.planContext, from.semanticTable(), from.plannerName)
      val executionPlan: ExecutionPlan = createExecutionPlan(context, compiled)
      runtimeSuccessRateMonitor.newPlanSeen(from.logicalPlan)
      new CompilationState(from, Some(executionPlan))
    } catch {
      case e: CantCompileQueryException =>
        runtimeSuccessRateMonitor.unableToHandlePlan(from.logicalPlan, e)
        new CompilationState(from, None)
    }
  }

  private def createExecutionPlan(context: EnterpriseRuntimeContext, compiled: CompiledPlan) = new ExecutionPlan {
    private val fingerprint = context.createFingerprintReference(compiled.fingerprint)

    override def isStale(lastTxId: () => Long, statistics: GraphStatistics): Boolean = fingerprint.isStale(lastTxId, statistics)

    override def run(queryContext: QueryContext,
                     executionMode: ExecutionMode, params: MapValue): InternalExecutionResult = {
      val taskCloser = new TaskCloser
      taskCloser.addTask(queryContext.transactionalContext.close)
      try {
        if (executionMode == ExplainMode) {
          //close all statements
          taskCloser.close(success = true)
          val logger = context.notificationLogger
          ExplainExecutionResult(compiled.columns.toArray,
                                 compiled.planDescription, READ_ONLY, logger.notifications.map(asKernelNotification(logger.offset)))
        } else
          compiled.executionResultBuilder(queryContext, executionMode, createTracer(executionMode, queryContext),
                                          params, taskCloser)
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
                addArgument(Arguments.PageCacheHitRatio(data.pageCacheHitRatio())).
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
