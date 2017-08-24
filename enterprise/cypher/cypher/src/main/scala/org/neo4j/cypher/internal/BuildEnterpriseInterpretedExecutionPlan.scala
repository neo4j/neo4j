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
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.interpreted.EnterprisePipeBuilder
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.interpreted.expressions.EnterpriseExpressionConverters
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.phases.CompilationState
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.pipes.Pipe
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.planDescription.{Id, LogicalPlanIdentificationBuilder}
import org.neo4j.cypher.internal.compiler.v3_3.CypherCompilerConfiguration
import org.neo4j.cypher.internal.compiler.v3_3.phases.{CompilationContains, LogicalPlanState}
import org.neo4j.cypher.internal.compiler.v3_3.planner.CantCompileQueryException
import org.neo4j.cypher.internal.compiler.v3_3.planner.logical.plans.{IndexUsage, LogicalPlan}
import org.neo4j.cypher.internal.compiler.v3_3.spi.{GraphStatistics, PlanContext}
import org.neo4j.cypher.internal.frontend.v3_3.notification.InternalNotification
import org.neo4j.cypher.internal.frontend.v3_3.phases.CompilationPhaseTracer.CompilationPhase.PIPE_BUILDING
import org.neo4j.cypher.internal.frontend.v3_3.phases.{CompilationPhaseTracer, Monitors, Phase}
import org.neo4j.cypher.internal.frontend.v3_3.{CypherException, PlannerName}
import org.neo4j.cypher.internal.spi.v3_3.QueryContext
import org.neo4j.values.AnyValue

object BuildEnterpriseInterpretedExecutionPlan extends Phase[EnterpriseRuntimeContext, LogicalPlanState, CompilationState] {
  override def phase: CompilationPhaseTracer.CompilationPhase = PIPE_BUILDING

  override def description = "create interpreted execution plan"

  override def postConditions = Set(CompilationContains[ExecutionPlan])

  override def process(from: LogicalPlanState, context: EnterpriseRuntimeContext): CompilationState = {
    createRegisteredRuntimeExecPlan(from, context)
  }

  private def createRegisteredRuntimeExecPlan(from: LogicalPlanState, context: EnterpriseRuntimeContext) = {
    val runtimeSuccessRateMonitor = context.monitors.newMonitor[NewRuntimeSuccessRateMonitor]()
    try {
      val (logicalPlan, pipelines) = rewritePlan(context, from.logicalPlan)
      val idMap = LogicalPlanIdentificationBuilder(logicalPlan)
      val converters = new ExpressionConverters(EnterpriseExpressionConverters, CommunityExpressionConverter)
      val executionPlanBuilder = new PipeExecutionPlanBuilder(context.clock, context.monitors,
                                                              expressionConverters = converters,
                                                              pipeBuilderFactory = EnterprisePipeBuilderFactory(
                                                                pipelines))
      val pipeBuildContext = PipeExecutionBuilderContext(context.metrics.cardinality, from.semanticTable(),
                                                         from.plannerName)
      val pipeInfo = executionPlanBuilder
        .build(from.periodicCommit, logicalPlan, idMap)(pipeBuildContext, context.planContext)
      val PipeInfo(pipe: Pipe, updating, periodicCommitInfo, fp, planner) = pipeInfo
      val columns = from.statement().returnColumns
      val resultBuilderFactory = DefaultExecutionResultBuilderFactory(pipeInfo, columns, logicalPlan, idMap)
      val func = BuildInterpretedExecutionPlan.getExecutionPlanFunction(periodicCommitInfo, from.queryText, updating,
                                                                        resultBuilderFactory,
                                                                        context.notificationLogger,
                                                                        EnterpriseInterpretedRuntimeName)
      val fingerprint = context.createFingerprintReference(fp)
      val periodicCommit = periodicCommitInfo.isDefined
      val indexes = logicalPlan.indexUsage
      val execPlan = RegisteredExecutionPlan(fingerprint, periodicCommit, planner, indexes, func, pipe, context.config)
      new CompilationState(from, Some(execPlan))
    } catch {
      case e: CypherException =>
        runtimeSuccessRateMonitor.unableToHandlePlan(from.logicalPlan, new CantCompileQueryException(cause = e))
        new CompilationState(from, None)
    }
  }

  private def rewritePlan(context: EnterpriseRuntimeContext, beforeRewrite: LogicalPlan) = {
    val beforePipelines: Map[LogicalPlan, PipelineInformation] = RegisterAllocation.allocateRegisters(beforeRewrite)
    val registeredRewriter = new RegisteredRewriter(context.planContext)
    val (logicalPlan, pipelines) = registeredRewriter(beforeRewrite, beforePipelines)
    (logicalPlan, pipelines)
  }

  case class RegisteredExecutionPlan(fingerprint: PlanFingerprintReference,
                                     isPeriodicCommit: Boolean,
                                     plannerUsed: PlannerName,
                                     override val plannedIndexUsage: Seq[IndexUsage],
                                     runFunction: (QueryContext, ExecutionMode, Map[String, AnyValue]) => InternalExecutionResult,
                                     pipe: Pipe,
                                     config: CypherCompilerConfiguration) extends executionplan.ExecutionPlan {

    override def run(queryContext: QueryContext, planType: ExecutionMode,
                     params: Map[String, AnyValue]): InternalExecutionResult =
      runFunction(queryContext, planType, params)

    override def isStale(lastTxId: () => Long, statistics: GraphStatistics): Boolean = fingerprint
      .isStale(lastTxId, statistics)

    override def runtimeUsed: RuntimeName = EnterpriseInterpretedRuntimeName

    override def notifications(planContext: PlanContext): Seq[InternalNotification] =
      BuildInterpretedExecutionPlan.checkForNotifications(pipe, planContext, config)
  }

  case class EnterprisePipeBuilderFactory(pipelineInformation: Map[LogicalPlan, PipelineInformation])
    extends PipeBuilderFactory {
    def apply(monitors: Monitors, recurse: LogicalPlan => Pipe, readOnly: Boolean, idMap: Map[LogicalPlan, Id],
              expressionConverters: ExpressionConverters)
             (implicit context: PipeExecutionBuilderContext, planContext: PlanContext): PipeBuilder = {

      val expressionToExpression = recursePipes(recurse, planContext) _

      val fallback = CommunityPipeBuilder(monitors, recurse, readOnly, idMap, expressionConverters,
                                          expressionToExpression)

      new EnterprisePipeBuilder(fallback, expressionConverters, idMap, monitors, pipelineInformation, readOnly,
        expressionToExpression)
    }
  }

}
