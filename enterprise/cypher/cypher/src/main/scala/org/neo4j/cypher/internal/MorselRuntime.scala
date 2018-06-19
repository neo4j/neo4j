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

import org.neo4j.cypher.internal.compatibility.CypherRuntime
import org.neo4j.cypher.internal.compatibility.v3_5.runtime.PhysicalPlanningAttributes.SlotConfigurations
import org.neo4j.cypher.internal.compatibility.v3_5.runtime.SlotAllocation.PhysicalPlan
import org.neo4j.cypher.internal.compatibility.v3_5.runtime._
import org.neo4j.cypher.internal.compatibility.v3_5.runtime.executionplan.StandardInternalExecutionResult.IterateByAccepting
import org.neo4j.cypher.internal.compatibility.v3_5.runtime.executionplan.{StandardInternalExecutionResult, ExecutionPlan => ExecutionPlan_V35}
import org.neo4j.cypher.internal.compatibility.v3_5.runtime.helpers.InternalWrapping.asKernelNotification
import org.neo4j.cypher.internal.compiler.v3_5.ExperimentalFeatureNotification
import org.neo4j.cypher.internal.compiler.v3_5.phases.LogicalPlanState
import org.neo4j.cypher.internal.planner.v3_5.spi.PlanningAttributes.Cardinalities
import org.neo4j.cypher.internal.runtime._
import org.neo4j.cypher.internal.runtime.interpreted.commands.convert.{CommunityExpressionConverter, ExpressionConverters}
import org.neo4j.cypher.internal.runtime.planDescription.InternalPlanDescription.Arguments.{Runtime, RuntimeImpl}
import org.neo4j.cypher.internal.runtime.planDescription.{InternalPlanDescription, LogicalPlan2PlanDescription}
import org.neo4j.cypher.internal.runtime.slotted.expressions.{CompiledExpressionConverter, SlottedExpressionConverters}
import org.neo4j.cypher.internal.runtime.vectorized.dispatcher.{Dispatcher, SingleThreadedExecutor}
import org.neo4j.cypher.internal.runtime.vectorized.expressions.MorselExpressionConverters
import org.neo4j.cypher.internal.runtime.vectorized.{Pipeline, PipelineBuilder}
import org.neo4j.cypher.internal.v3_5.logical.plans.LogicalPlan
import org.neo4j.cypher.result.QueryResult.QueryResultVisitor
import org.neo4j.graphdb.Notification
import org.neo4j.values.virtual.MapValue
import org.opencypher.v9_0.ast.semantics.SemanticTable
import org.opencypher.v9_0.frontend.PlannerName

import org.opencypher.v9_0.frontend.phases.InternalNotificationLogger
import org.opencypher.v9_0.util.TaskCloser



object MorselRuntime extends CypherRuntime[EnterpriseRuntimeContext] {
  override def compileToExecutable(state: LogicalPlanState, context: EnterpriseRuntimeContext): ExecutionPlan_V35 = {
      val (logicalPlan,physicalPlan) = rewritePlan(context, state.logicalPlan, state.semanticTable())
      val converters: ExpressionConverters = new ExpressionConverters(
        new CompiledExpressionConverter(context.log, physicalPlan.slotConfigurations(state.logicalPlan.id)),
        MorselExpressionConverters,
        SlottedExpressionConverters,
        CommunityExpressionConverter)
      val operatorBuilder = new PipelineBuilder(physicalPlan, converters, context.readOnly)

      val operators = operatorBuilder.create(logicalPlan)
      val dispatcher =
        if (context.debugOptions.contains("singlethreaded")) new SingleThreadedExecutor()
        else context.dispatcher
      val fieldNames = state.statement().returnColumns.toArray

      context.notificationLogger.log(
        ExperimentalFeatureNotification("use the morsel runtime at your own peril, " +
                                      "not recommended to be run on production systems"))

      VectorizedExecutionPlan(state.plannerName,
                              operators,
                              physicalPlan.slotConfigurations,
                              logicalPlan,
                              fieldNames,
                              dispatcher,
                              context.notificationLogger,
                              context.readOnly,
                              state.cardinalities)
  }

  private def rewritePlan(context: EnterpriseRuntimeContext, beforeRewrite: LogicalPlan, semanticTable: SemanticTable) = {
    val physicalPlan: PhysicalPlan = SlotAllocation.allocateSlots(beforeRewrite, semanticTable)
    val slottedRewriter = new SlottedRewriter(context.tokenContext)
    val logicalPlan = slottedRewriter(beforeRewrite, physicalPlan.slotConfigurations)
    (logicalPlan, physicalPlan)
  }

  case class VectorizedExecutionPlan(plannerUsed: PlannerName,
                                     operators: Pipeline,
                                     slots: SlotConfigurations,
                                     logicalPlan: LogicalPlan,
                                     fieldNames: Array[String],
                                     dispatcher: Dispatcher,
                                     notificationLogger: InternalNotificationLogger,
                                     readOnly: Boolean,
                                     cardinalities: Cardinalities) extends ExecutionPlan_V35 {
    override def run(queryContext: QueryContext, planType: ExecutionMode, params: MapValue): InternalExecutionResult = {
      val taskCloser = new TaskCloser
      taskCloser.addTask(queryContext.transactionalContext.close)
      taskCloser.addTask(queryContext.resources.close)
      val planDescription =
        () => LogicalPlan2PlanDescription(logicalPlan, plannerUsed, readOnly, cardinalities)
          .addArgument(Runtime(MorselRuntimeName.toTextOutput))
          .addArgument(RuntimeImpl(MorselRuntimeName.name))

      if (planType == ExplainMode) {
        //close all statements
        taskCloser.close(success = true)
        ExplainExecutionResult(fieldNames, planDescription(), READ_ONLY,
          notificationLogger.notifications.map(asKernelNotification(notificationLogger.offset)))
      } else new VectorizedOperatorExecutionResult(operators, logicalPlan, planDescription, queryContext,
        params, fieldNames, taskCloser, dispatcher)
    }

    override def runtimeName: RuntimeName = MorselRuntimeName
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
}
