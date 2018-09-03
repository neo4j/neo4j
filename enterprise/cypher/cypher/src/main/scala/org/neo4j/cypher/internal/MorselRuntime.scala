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
import org.neo4j.cypher.internal.compatibility.v3_5.runtime.executionplan.{ExecutionPlan => ExecutionPlan_V35}
import org.neo4j.cypher.internal.compiler.v3_5.ExperimentalFeatureNotification
import org.neo4j.cypher.internal.compiler.v3_5.phases.LogicalPlanState
import org.neo4j.cypher.internal.runtime._
import org.neo4j.cypher.internal.runtime.interpreted.commands.convert.{CommunityExpressionConverter, ExpressionConverters}
import org.neo4j.cypher.internal.runtime.parallel.SchedulerTracer
import org.neo4j.cypher.internal.runtime.planDescription.Argument
import org.neo4j.cypher.internal.runtime.slotted.expressions.{CompiledExpressionConverter, SlottedExpressionConverters}
import org.neo4j.cypher.internal.runtime.vectorized.expressions.MorselExpressionConverters
import org.neo4j.cypher.internal.runtime.vectorized.{Dispatcher, Pipeline, PipelineBuilder}
import org.neo4j.cypher.internal.v3_5.logical.plans.LogicalPlan
import org.neo4j.cypher.result.QueryResult.QueryResultVisitor
import org.neo4j.cypher.result.RuntimeResult.ConsumptionState
import org.neo4j.cypher.result.{QueryProfile, RuntimeResult}
import org.neo4j.graphdb.ResourceIterator
import org.neo4j.values.virtual.MapValue
import org.opencypher.v9_0.ast.semantics.SemanticTable

object MorselRuntime extends CypherRuntime[EnterpriseRuntimeContext] {
  override def compileToExecutable(state: LogicalPlanState, context: EnterpriseRuntimeContext): ExecutionPlan_V35 = {
      val (logicalPlan,physicalPlan) = rewritePlan(context, state.logicalPlan, state.semanticTable())
    val converters: ExpressionConverters = new ExpressionConverters(
        new CompiledExpressionConverter(context.log, physicalPlan),
        MorselExpressionConverters,
        SlottedExpressionConverters(physicalPlan),
        CommunityExpressionConverter)
      val operatorBuilder = new PipelineBuilder(physicalPlan, converters, context.readOnly)

      val operators = operatorBuilder.create(logicalPlan)
      val dispatcher = context.runtimeEnvironment.getDispatcher(context.debugOptions)
      val tracer = context.runtimeEnvironment.tracer
      val fieldNames = state.statement().returnColumns.toArray

      context.notificationLogger.log(
        ExperimentalFeatureNotification("use the morsel runtime at your own peril, " +
                                        "not recommended to be run on production systems"))

    VectorizedExecutionPlan(operators,
                            physicalPlan.slotConfigurations,
                            logicalPlan,
                            fieldNames,
                            dispatcher,
                            tracer)
  }

  private def rewritePlan(context: EnterpriseRuntimeContext, beforeRewrite: LogicalPlan, semanticTable: SemanticTable) = {
    val physicalPlan: PhysicalPlan = SlotAllocation.allocateSlots(beforeRewrite, semanticTable)
    val slottedRewriter = new SlottedRewriter(context.tokenContext)
    val logicalPlan = slottedRewriter(beforeRewrite, physicalPlan.slotConfigurations)
    (logicalPlan, physicalPlan)
  }

  case class VectorizedExecutionPlan(operators: Pipeline,
                                     slots: SlotConfigurations,
                                     logicalPlan: LogicalPlan,
                                     fieldNames: Array[String],
                                     dispatcher: Dispatcher,
                                     schedulerTracer: SchedulerTracer) extends ExecutionPlan_V35 {

    override def run(queryContext: QueryContext,
                     doProfile: Boolean,
                     params: MapValue): RuntimeResult = {

      new VectorizedRuntimeResult(operators,
                                  logicalPlan,
                                  queryContext,
                                  params,
                                  fieldNames,
                                  dispatcher,
                                  schedulerTracer)
    }

    override def runtimeName: RuntimeName = MorselRuntimeName

    override def metadata: Seq[Argument] = Nil
  }

  class VectorizedRuntimeResult(operators: Pipeline,
                                logicalPlan: LogicalPlan,
                                queryContext: QueryContext,
                                params: MapValue,
                                override val fieldNames: Array[String],
                                dispatcher: Dispatcher,
                                schedulerTracer: SchedulerTracer) extends RuntimeResult {

    private var resultRequested = false

    override def accept[E <: Exception](visitor: QueryResultVisitor[E]): Unit = {
      dispatcher.execute(operators, queryContext, params, schedulerTracer)(visitor)
      resultRequested = true
    }

    override def queryStatistics(): runtime.QueryStatistics = queryContext.getOptStatistics.getOrElse(QueryStatistics())

    override def isIterable: Boolean = false

    override def asIterator(): ResourceIterator[java.util.Map[String, AnyRef]] =
      throw new UnsupportedOperationException("The Morsel runtime is not iterable")

    override def consumptionState: RuntimeResult.ConsumptionState =
      if (!resultRequested) ConsumptionState.NOT_STARTED
      else ConsumptionState.EXHAUSTED

    override def close(): Unit = {}

    override def queryProfile(): QueryProfile = QueryProfile.NONE
  }
}
