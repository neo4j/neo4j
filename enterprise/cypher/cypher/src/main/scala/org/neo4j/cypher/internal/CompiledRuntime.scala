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
package org.neo4j.cypher.internal

import org.neo4j.cypher.internal.codegen.profiling.ProfilingTracer
import org.neo4j.cypher.internal.compatibility.CypherRuntime
import org.neo4j.cypher.internal.compatibility.v3_5.runtime.executionplan.{Provider, ExecutionPlan => ExecutionPlanv3_5}
import org.neo4j.cypher.internal.compatibility.v3_5.runtime.helpers.InternalWrapping.asKernelNotification
import org.neo4j.cypher.internal.compatibility.v3_5.runtime.{CompiledRuntimeName, ExplainExecutionResult, RuntimeName}
import org.neo4j.cypher.internal.compiler.v3_5.phases.LogicalPlanState
import org.neo4j.cypher.internal.compiler.v3_5.planner.CantCompileQueryException
import org.neo4j.cypher.internal.runtime._
import org.neo4j.cypher.internal.runtime.compiled.ExecutionPlanBuilder.DescriptionProvider
import org.neo4j.cypher.internal.runtime.compiled.codegen.{CodeGenConfiguration, CodeGenerator}
import org.neo4j.cypher.internal.runtime.compiled.CompiledPlan
import org.neo4j.cypher.internal.runtime.planDescription.InternalPlanDescription
import org.neo4j.cypher.internal.runtime.planDescription.InternalPlanDescription.Arguments
import org.neo4j.cypher.internal.v3_5.logical.plans.IndexUsage
import org.neo4j.graphdb.Notification
import org.neo4j.values.virtual.MapValue
import org.opencypher.v9_0.frontend.PlannerName
import org.opencypher.v9_0.util.TaskCloser

object CompiledRuntime extends CypherRuntime[EnterpriseRuntimeContext] {

  @throws[CantCompileQueryException]
  override def compileToExecutable(state: LogicalPlanState, context: EnterpriseRuntimeContext): ExecutionPlanv3_5 = {
    val codeGen = new CodeGenerator(context.codeStructure, context.clock, CodeGenConfiguration(context.debugOptions))
    val readOnly = state.solveds(state.logicalPlan.id).readOnly
    val compiled: CompiledPlan = codeGen.generate(
      state.logicalPlan,
      context.tokenContext,
      state.semanticTable(),
      state.plannerName,
      readOnly,
      state.cardinalities)
    new CompiledExecutionPlan(
      compiled,
      notifications(context))
  }

  private def notifications(context: EnterpriseRuntimeContext): Set[Notification] = {
    val mapper = asKernelNotification(context.notificationLogger.offset) _
    context.notificationLogger.notifications.map(mapper)
  }

  /**
    * Execution plan for compiled runtime. Beware: will be cached.
    */
  class CompiledExecutionPlan(val compiled: CompiledPlan,
                              val notifications: Set[Notification]) extends ExecutionPlanv3_5 {

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
        case t: Throwable =>
          taskCloser.close(success = false)
          throw t
      }
    }

    override def runtimeUsed: RuntimeName = CompiledRuntimeName

    override def plannedIndexUsage: Seq[IndexUsage] = compiled.plannedIndexUsage

    override def isPeriodicCommit: Boolean = compiled.periodicCommit.isDefined

    override def plannerUsed: PlannerName = compiled.plannerUsed
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
}
