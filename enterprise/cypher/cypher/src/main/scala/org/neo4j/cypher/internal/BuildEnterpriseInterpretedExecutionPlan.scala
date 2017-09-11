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
package org.neo4j.cypher.internal

import org.neo4j.cypher.internal.compatibility.v3_3.runtime._
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.commands.convert.{CommunityExpressionConverter, ExpressionConverters}
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.compiled.EnterpriseRuntimeContext
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.executionplan._
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.phases.CompilationState
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.pipes.Pipe
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.slotted.SlottedPipeBuilder
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.slotted.expressions.SlottedExpressionConverters
import org.neo4j.cypher.internal.compiler.v3_3.CypherCompilerConfiguration
import org.neo4j.cypher.internal.compiler.v3_3.phases.{CompilationContains, LogicalPlanState}
import org.neo4j.cypher.internal.compiler.v3_3.planner.CantCompileQueryException
import org.neo4j.cypher.internal.compiler.v3_3.spi.{GraphStatistics, PlanContext}
import org.neo4j.cypher.internal.frontend.v3_3.notification.InternalNotification
import org.neo4j.cypher.internal.frontend.v3_3.phases.CompilationPhaseTracer.CompilationPhase.PIPE_BUILDING
import org.neo4j.cypher.internal.frontend.v3_3.phases.{CompilationPhaseTracer, Monitors, Phase}
import org.neo4j.cypher.internal.frontend.v3_3.{CypherException, PlannerName}
import org.neo4j.cypher.internal.spi.v3_3.QueryContext
import org.neo4j.cypher.internal.v3_3.logical.plans.{IndexUsage, LogicalPlan, LogicalPlanId}
import org.neo4j.values.virtual.MapValue

object BuildEnterpriseInterpretedExecutionPlan extends Phase[EnterpriseRuntimeContext, LogicalPlanState, CompilationState] {
  override def phase: CompilationPhaseTracer.CompilationPhase = PIPE_BUILDING

  override def description = "create interpreted execution plan"

  override def postConditions = Set(CompilationContains[ExecutionPlan])

  override def process(from: LogicalPlanState, context: EnterpriseRuntimeContext): CompilationState = {
    createSlottedRuntimeExecPlan(from, context)
  }

  private def createSlottedRuntimeExecPlan(from: LogicalPlanState, context: EnterpriseRuntimeContext) = {
    val runtimeSuccessRateMonitor = context.monitors.newMonitor[NewRuntimeSuccessRateMonitor]()
    try {
      val (logicalPlan, pipelines) = rewritePlan(context, from.logicalPlan)
      val converters = new ExpressionConverters(SlottedExpressionConverters, CommunityExpressionConverter)
      val pipeBuilderFactory = EnterprisePipeBuilderFactory(pipelines)
      val executionPlanBuilder = new PipeExecutionPlanBuilder(context.clock, context.monitors,
                                                              expressionConverters = converters,
                                                              pipeBuilderFactory = pipeBuilderFactory)
      val pipeBuildContext = PipeExecutionBuilderContext(context.metrics.cardinality, from.semanticTable(),
                                                         from.plannerName)
      val pipeInfo = executionPlanBuilder
        .build(from.periodicCommit, logicalPlan)(pipeBuildContext, context.planContext)
      val PipeInfo(pipe: Pipe, updating, periodicCommitInfo, fp, planner) = pipeInfo
      val columns = from.statement().returnColumns
      val resultBuilderFactory = DefaultExecutionResultBuilderFactory(pipeInfo, columns, logicalPlan)
      val func = BuildInterpretedExecutionPlan.getExecutionPlanFunction(periodicCommitInfo, from.queryText, updating,
                                                                        resultBuilderFactory,
                                                                        context.notificationLogger,
                                                                        SlottedRuntimeName)
      val fingerprint = context.createFingerprintReference(fp)
      val periodicCommit = periodicCommitInfo.isDefined
      val indexes = logicalPlan.indexUsage
      val execPlan = SlottedExecutionPlan(fingerprint, periodicCommit, planner, indexes, func, pipe, context.config)
      new CompilationState(from, Some(execPlan))
    } catch {
      case e: CypherException =>
        runtimeSuccessRateMonitor.unableToHandlePlan(from.logicalPlan, new CantCompileQueryException(cause = e))
        new CompilationState(from, None)
    }
  }

  private def rewritePlan(context: EnterpriseRuntimeContext, beforeRewrite: LogicalPlan) = {
    val pipelines: Map[LogicalPlanId, PipelineInformation] = SlotAllocation.allocateSlots(beforeRewrite)
    val slottedRewriter = new SlottedRewriter(context.planContext)
    val logicalPlan = slottedRewriter(beforeRewrite, pipelines)
    (logicalPlan, pipelines)
  }

  case class SlottedExecutionPlan(fingerprint: PlanFingerprintReference,
                                  isPeriodicCommit: Boolean,
                                  plannerUsed: PlannerName,
                                  override val plannedIndexUsage: Seq[IndexUsage],
                                  runFunction: (QueryContext, ExecutionMode, MapValue) => InternalExecutionResult,
                                  pipe: Pipe,
                                  config: CypherCompilerConfiguration) extends executionplan.ExecutionPlan {

    override def run(queryContext: QueryContext, planType: ExecutionMode,
                     params: MapValue): InternalExecutionResult =
      runFunction(queryContext, planType, params)

    override def isStale(lastTxId: () => Long, statistics: GraphStatistics): Boolean = fingerprint
      .isStale(lastTxId, statistics)

    override def runtimeUsed: RuntimeName = SlottedRuntimeName

    override def notifications(planContext: PlanContext): Seq[InternalNotification] =
      BuildInterpretedExecutionPlan.checkForNotifications(pipe, planContext, config)
  }

  case class EnterprisePipeBuilderFactory(pipelineInformation: Map[LogicalPlanId, PipelineInformation])
    extends PipeBuilderFactory {
    def apply(monitors: Monitors, recurse: LogicalPlan => Pipe, readOnly: Boolean,
              expressionConverters: ExpressionConverters)
             (implicit context: PipeExecutionBuilderContext, planContext: PlanContext): PipeBuilder = {

      val expressionToExpression = recursePipes(recurse, planContext) _

      val fallback = CommunityPipeBuilder(monitors, recurse, readOnly, expressionConverters, expressionToExpression)

      new SlottedPipeBuilder(fallback, expressionConverters, monitors, pipelineInformation, readOnly,
        expressionToExpression)
    }
  }

}
