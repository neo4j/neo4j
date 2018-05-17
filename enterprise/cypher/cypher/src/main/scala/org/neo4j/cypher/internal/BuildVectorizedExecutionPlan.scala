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
package org.neo4j.cypher.internal

import org.neo4j.cypher.internal.compatibility.v3_5.runtime.PhysicalPlanningAttributes.SlotConfigurations
import org.neo4j.cypher.internal.compatibility.v3_5.runtime.SlotAllocation.PhysicalPlan
import org.neo4j.cypher.internal.compatibility.v3_5.runtime._
import org.neo4j.cypher.internal.compatibility.v3_5.runtime.executionplan.StandardInternalExecutionResult.IterateByAccepting
import org.neo4j.cypher.internal.compatibility.v3_5.runtime.executionplan.{StandardInternalExecutionResult, ExecutionPlan => RuntimeExecutionPlan}
import org.neo4j.cypher.internal.compatibility.v3_5.runtime.helpers.InternalWrapping.asKernelNotification
import org.neo4j.cypher.internal.compatibility.v3_5.runtime.phases.CompilationState
import org.neo4j.cypher.internal.compiler.v3_5.phases.LogicalPlanState
import org.neo4j.cypher.internal.compiler.v3_5.planner.CantCompileQueryException
import org.opencypher.v9_0.frontend.PlannerName
import org.opencypher.v9_0.frontend.phases.CompilationPhaseTracer.CompilationPhase
import org.opencypher.v9_0.frontend.phases._
import org.opencypher.v9_0.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.planner.v3_5.spi.GraphStatistics
import org.neo4j.cypher.internal.planner.v3_5.spi.PlanningAttributes.Cardinalities
import org.neo4j.cypher.internal.runtime.compiled.EnterpriseRuntimeContext
import org.neo4j.cypher.internal.runtime.interpreted.commands.convert.{CommunityExpressionConverter, ExpressionConverters}
import org.neo4j.cypher.internal.runtime.planDescription.InternalPlanDescription.Arguments.{Runtime, RuntimeImpl}
import org.neo4j.cypher.internal.runtime.planDescription.{InternalPlanDescription, LogicalPlan2PlanDescription}
import org.neo4j.cypher.internal.runtime.slotted.expressions.SlottedExpressionConverters
import org.neo4j.cypher.internal.runtime.vectorized.dispatcher.{Dispatcher, SingleThreadedExecutor}
import org.neo4j.cypher.internal.runtime.vectorized.expressions.MorselExpressionConverters
import org.neo4j.cypher.internal.runtime.vectorized.{Pipeline, PipelineBuilder}
import org.neo4j.cypher.internal.runtime.{QueryStatistics, _}
import org.opencypher.v9_0.util.{ExperimentalFeatureNotification, TaskCloser}
import org.neo4j.cypher.internal.v3_5.logical.plans.LogicalPlan
import org.neo4j.cypher.result.QueryResult.QueryResultVisitor
import org.neo4j.graphdb._
import org.neo4j.values.virtual.MapValue

import scala.util.{Failure, Success}

object BuildVectorizedExecutionPlan extends Phase[EnterpriseRuntimeContext, LogicalPlanState, CompilationState] {
  override def phase: CompilationPhaseTracer.CompilationPhase = CompilationPhase.PIPE_BUILDING

  override def description: String = "build pipes"

  override def process(from: LogicalPlanState, context: EnterpriseRuntimeContext): CompilationState = {
    try {
      val (physicalPlan, pipelines) = rewritePlan(context, from.logicalPlan, from.semanticTable())
      val converters: ExpressionConverters = new ExpressionConverters(MorselExpressionConverters,
                                                                      SlottedExpressionConverters,
                                                                      CommunityExpressionConverter)
      val operatorBuilder = new PipelineBuilder(pipelines, converters)
      val operators = operatorBuilder.create(physicalPlan)
      val dispatcher =
        if (context.debugOptions.contains("singlethreaded")) new SingleThreadedExecutor()
        else context.dispatcher
      val fieldNames = from.statement().returnColumns.toArray

      context.notificationLogger.log(
        ExperimentalFeatureNotification("use the morsel runtime at your own peril, " +
                                          "not recommended to be run on production systems"))
      val readOnly = from.solveds(from.logicalPlan.id).readOnly
      val execPlan = VectorizedExecutionPlan(from.plannerName, operators, pipelines, physicalPlan, fieldNames,
                                             dispatcher, context.notificationLogger, readOnly, from.cardinalities)
      new CompilationState(from, Success(execPlan))
    } catch {
      case e: CantCompileQueryException =>
        new CompilationState(from, Failure(e))
    }
  }

  private def rewritePlan(context: EnterpriseRuntimeContext, beforeRewrite: LogicalPlan, semanticTable: SemanticTable) = {
    val physicalPlan: PhysicalPlan = SlotAllocation.allocateSlots(beforeRewrite, semanticTable)
    val slottedRewriter = new SlottedRewriter(context.planContext)
    val logicalPlan = slottedRewriter(beforeRewrite, physicalPlan.slotConfigurations)
    (logicalPlan, physicalPlan.slotConfigurations)
  }

  override def postConditions: Set[Condition] = Set.empty

  case class VectorizedExecutionPlan(plannerUsed: PlannerName,
                                     operators: Pipeline,
                                     slots: SlotConfigurations,
                                     physicalPlan: LogicalPlan,
                                     fieldNames: Array[String],
                                     dispatcher: Dispatcher,
                                     notificationLogger: InternalNotificationLogger,
                                     readOnly: Boolean,
                                     cardinalities: Cardinalities) extends RuntimeExecutionPlan {
    override def run(queryContext: QueryContext, planType: ExecutionMode, params: MapValue): InternalExecutionResult = {
      val taskCloser = new TaskCloser
      taskCloser.addTask(queryContext.transactionalContext.close)
      taskCloser.addTask(queryContext.resources.close)
      val planDescription =
        () => LogicalPlan2PlanDescription(physicalPlan, plannerUsed, readOnly, cardinalities)
          .addArgument(Runtime(MorselRuntimeName.toTextOutput))
          .addArgument(RuntimeImpl(MorselRuntimeName.name))

      if (planType == ExplainMode) {
        //close all statements
        taskCloser.close(success = true)
        ExplainExecutionResult(fieldNames, planDescription(), READ_ONLY,
                               notificationLogger.notifications.map(asKernelNotification(notificationLogger.offset)))
      } else new VectorizedOperatorExecutionResult(operators, physicalPlan, planDescription, queryContext,
                                              params, fieldNames, taskCloser, dispatcher)
      }


    override def isPeriodicCommit: Boolean = false

    override def reusability: ReusabilityState = FineToReuse // TODO: This is a lie.

    override def runtimeUsed: RuntimeName = MorselRuntimeName
  }
}

class VectorizedOperatorExecutionResult(operators: Pipeline,
                                        logicalPlan: LogicalPlan,
                                        executionPlanBuilder: () => InternalPlanDescription,
                                        queryContext: QueryContext,
                                        params: MapValue,
                                        override val fieldNames: Array[String],
                                        taskCloser: TaskCloser,
                                        dispatcher: Dispatcher) extends StandardInternalExecutionResult(queryContext, ProcedureRuntimeName, Some(taskCloser)) with IterateByAccepting {


  override def accept[E <: Exception](visitor: QueryResultVisitor[E]): Unit =
    dispatcher.execute(operators, queryContext, params, taskCloser)(visitor)

  override def queryStatistics(): runtime.QueryStatistics = queryContext.getOptStatistics.getOrElse(QueryStatistics())

  override def executionPlanDescription(): InternalPlanDescription = executionPlanBuilder()

  override def queryType: InternalQueryType = READ_ONLY

  override def executionMode: ExecutionMode = NormalMode

  override def notifications: Iterable[Notification] = Iterable.empty[Notification]

  override def withNotifications(notification: Notification*): InternalExecutionResult = this
}

