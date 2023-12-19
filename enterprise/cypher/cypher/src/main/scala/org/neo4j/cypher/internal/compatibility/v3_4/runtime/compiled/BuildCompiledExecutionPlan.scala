/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.cypher.internal.compatibility.v3_4.runtime.compiled

import org.neo4j.cypher.internal.compatibility.v3_4.runtime._
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.compiled.ExecutionPlanBuilder.DescriptionProvider
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.compiled.codegen._
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.executionplan._
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.helpers.InternalWrapping.asKernelNotification
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.phases.CompilationState
import org.neo4j.cypher.internal.compiler.v3_4.phases.LogicalPlanState
import org.neo4j.cypher.internal.compiler.v3_4.planner.CantCompileQueryException
import org.neo4j.cypher.internal.frontend.v3_4.PlannerName
import org.neo4j.cypher.internal.frontend.v3_4.phases.CompilationPhaseTracer.CompilationPhase.CODE_GENERATION
import org.neo4j.cypher.internal.frontend.v3_4.phases.Phase
import org.neo4j.cypher.internal.planner.v3_4.spi.GraphStatistics
import org.neo4j.cypher.internal.planner.v3_4.spi.PlanningAttributes.ReadOnlies
import org.neo4j.cypher.internal.runtime._
import org.neo4j.cypher.internal.runtime.planDescription.InternalPlanDescription
import org.neo4j.cypher.internal.runtime.planDescription.InternalPlanDescription.Arguments
import org.neo4j.cypher.internal.util.v3_4.TaskCloser
import org.neo4j.cypher.internal.v3_4.codegen.profiling.ProfilingTracer
import org.neo4j.cypher.internal.v3_4.logical.plans.IndexUsage
import org.neo4j.graphdb.Notification
import org.neo4j.values.virtual.MapValue

import scala.util.{Failure, Success}

object BuildCompiledExecutionPlan extends Phase[EnterpriseRuntimeContext, LogicalPlanState, CompilationState] {

  override def phase = CODE_GENERATION

  override def description = "creates runnable byte code"

  override def postConditions = Set.empty // Can't yet guarantee that we can build an execution plan

  override def process(from: LogicalPlanState, context: EnterpriseRuntimeContext): CompilationState = {
    val runtimeSuccessRateMonitor = context.monitors.newMonitor[NewRuntimeSuccessRateMonitor]()
    try {
      val codeGen = new CodeGenerator(context.codeStructure, context.clock, CodeGenConfiguration(context.debugOptions))
      val readOnlies = new ReadOnlies
      from.solveds.mapTo(readOnlies, _.readOnly)
      val compiled: CompiledPlan = codeGen.generate(from.logicalPlan, context.planContext, from.semanticTable(), from.plannerName, readOnlies, from.cardinalities)
      val executionPlan: ExecutionPlan =
        new CompiledExecutionPlan(compiled,
                                  context.createFingerprintReference(compiled.fingerprint),
                                  notifications(context))
      runtimeSuccessRateMonitor.newPlanSeen(from.logicalPlan)
      new CompilationState(from, Success(executionPlan))
    } catch {
      case e: CantCompileQueryException =>
        runtimeSuccessRateMonitor.unableToHandlePlan(from.logicalPlan, e)
        new CompilationState(from, Failure(e))
    }
  }

  private def notifications(context: EnterpriseRuntimeContext): Set[Notification] = {
    val mapper = asKernelNotification(context.notificationLogger.offset) _
    context.notificationLogger.notifications.map(mapper)
  }
  private def createTracer(mode: ExecutionMode, queryContext: QueryContext): DescriptionProvider = mode match {
    case ProfileMode =>
      val tracer = new ProfilingTracer(queryContext.transactionalContext.kernelStatisticProvider)
      (description: Provider[InternalPlanDescription]) =>
        (new Provider[InternalPlanDescription] {

          override def get(): InternalPlanDescription = description.get().map {
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
    case _ => (description: Provider[InternalPlanDescription]) => (description, None)
  }

  /**
    * Execution plan for compiled runtime. Beware: will be cached.
    */
  class CompiledExecutionPlan(val compiled: CompiledPlan,
                              val fingerprint: PlanFingerprintReference,
                              val notifications: Set[Notification]) extends ExecutionPlan {

    override def run(queryContext: QueryContext,
                     executionMode: ExecutionMode, params: MapValue): InternalExecutionResult = {
      val taskCloser = new TaskCloser
      taskCloser.addTask(queryContext.transactionalContext.close)
      try {
        if (executionMode == ExplainMode) {
          //close all statements
          taskCloser.close(success = true)
          ExplainExecutionResult(compiled.columns.toArray, compiled.planDescription.get(), READ_ONLY, notifications)
        } else
          compiled.executionResultBuilder(queryContext, executionMode, createTracer(executionMode, queryContext), params, taskCloser)
      } catch {
        case (t: Throwable) =>
          taskCloser.close(success = false)
          throw t
      }
    }

    override def checkPlanResusability(lastTxId: () => Long, statistics: GraphStatistics) = fingerprint.checkPlanReusability(lastTxId, statistics)

    override def runtimeUsed: RuntimeName = CompiledRuntimeName

    override def plannedIndexUsage: Seq[IndexUsage] = compiled.plannedIndexUsage

    override def isPeriodicCommit: Boolean = compiled.periodicCommit.isDefined

    override def plannerUsed: PlannerName = compiled.plannerUsed
  }
}
