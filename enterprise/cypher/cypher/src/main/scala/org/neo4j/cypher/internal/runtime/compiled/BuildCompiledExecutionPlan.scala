/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.runtime.compiled

import org.neo4j.cypher.internal.codegen.profiling.ProfilingTracer
import org.neo4j.cypher.internal.{MaybeReusable, PlanFingerprintReference, ReusabilityState}
import org.neo4j.cypher.internal.compatibility.v3_5.runtime._
import org.neo4j.cypher.internal.compatibility.v3_5.runtime.executionplan._
import org.neo4j.cypher.internal.compatibility.v3_5.runtime.helpers.InternalWrapping.asKernelNotification
import org.neo4j.cypher.internal.compatibility.v3_5.runtime.phases.CompilationState
import org.neo4j.cypher.internal.compiler.v3_5.phases.LogicalPlanState
import org.neo4j.cypher.internal.compiler.v3_5.planner.CantCompileQueryException
import org.neo4j.cypher.internal.frontend.v3_5.PlannerName
import org.neo4j.cypher.internal.frontend.v3_5.phases.CompilationPhaseTracer.CompilationPhase.CODE_GENERATION
import org.neo4j.cypher.internal.frontend.v3_5.phases.Phase
import org.neo4j.cypher.internal.runtime._
import org.neo4j.cypher.internal.runtime.compiled.ExecutionPlanBuilder.DescriptionProvider
import org.neo4j.cypher.internal.runtime.compiled.codegen.{CodeGenConfiguration, CodeGenerator}
import org.neo4j.cypher.internal.runtime.planDescription.InternalPlanDescription
import org.neo4j.cypher.internal.runtime.planDescription.InternalPlanDescription.Arguments
import org.neo4j.cypher.internal.util.v3_5.TaskCloser
import org.neo4j.cypher.internal.v3_5.logical.plans.IndexUsage
import org.neo4j.graphdb.Notification
import org.neo4j.values.virtual.MapValue

import scala.util.{Failure, Success}

object BuildCompiledExecutionPlan extends Phase[EnterpriseRuntimeContext, LogicalPlanState, CompilationState] {

  override def phase = CODE_GENERATION

  override def description = "creates runnable byte code"

  override def postConditions = Set.empty // Can't yet guarantee that we can build an execution plan

  override def process(from: LogicalPlanState, context: EnterpriseRuntimeContext): CompilationState = {
    try {
      val codeGen = new CodeGenerator(context.codeStructure, context.clock, CodeGenConfiguration(context.debugOptions))
      val readOnly = from.solveds(from.logicalPlan.id).readOnly
      val compiled: CompiledPlan = codeGen.generate(from.logicalPlan,
                                                    context.planContext,
                                                    from.semanticTable(),
                                                    from.plannerName,
                                                    readOnly,
                                                    from.cardinalities)
      val executionPlan: ExecutionPlan =
        new CompiledExecutionPlan(compiled,
                                  new PlanFingerprintReference(compiled.fingerprint),
                                  notifications(context))
      new CompilationState(from, Success(executionPlan))
    } catch {
      case e: CantCompileQueryException =>
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

    override def reusability: ReusabilityState = MaybeReusable(fingerprint)

    override def runtimeUsed: RuntimeName = CompiledRuntimeName

    override def plannedIndexUsage: Seq[IndexUsage] = compiled.plannedIndexUsage

    override def isPeriodicCommit: Boolean = compiled.periodicCommit.isDefined

    override def plannerUsed: PlannerName = compiled.plannerUsed
  }
}
