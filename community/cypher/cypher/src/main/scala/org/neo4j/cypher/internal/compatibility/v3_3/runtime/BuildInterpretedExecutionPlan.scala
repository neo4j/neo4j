/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.compatibility.v3_3.runtime

import org.neo4j.cypher.internal.InternalExecutionResult
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.commands.convert.{CommunityExpressionConverter, ExpressionConverters}
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.executionplan._
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.phases.CompilationState
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.pipes.Pipe
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.profiler.Profiler
import org.neo4j.cypher.internal.compiler.v3_4.CypherCompilerConfiguration
import org.neo4j.cypher.internal.compiler.v3_4.phases._
import org.neo4j.cypher.internal.compiler.v3_4.spi.{GraphStatistics, PlanContext}
import org.neo4j.cypher.internal.frontend.v3_4.notification.InternalNotification
import org.neo4j.cypher.internal.frontend.v3_4.phases.CompilationPhaseTracer.CompilationPhase.PIPE_BUILDING
import org.neo4j.cypher.internal.frontend.v3_4.phases.{InternalNotificationLogger, Phase}
import org.neo4j.cypher.internal.frontend.v3_4.{PeriodicCommitInOpenTransactionException, PlannerName}
import org.neo4j.cypher.internal.spi.v3_3.{QueryContext, UpdateCountingQueryContext}
import org.neo4j.cypher.internal.v3_4.logical.plans.IndexUsage
import org.neo4j.values.virtual.MapValue

object BuildInterpretedExecutionPlan extends Phase[CommunityRuntimeContext, LogicalPlanState, CompilationState] {
  override def phase = PIPE_BUILDING

  override def description = "create interpreted execution plan"

  override def postConditions = Set(CompilationContains[ExecutionPlan])

  override def process(from: LogicalPlanState, context: CommunityRuntimeContext): CompilationState = {
    val logicalPlan = from.logicalPlan
    val converters = new ExpressionConverters(CommunityExpressionConverter)
    val executionPlanBuilder = new PipeExecutionPlanBuilder(context.clock, context.monitors,
      expressionConverters = converters, pipeBuilderFactory = CommunityPipeBuilderFactory)
    val pipeBuildContext = PipeExecutionBuilderContext(context.metrics.cardinality, from.semanticTable(), from.plannerName)
    val pipeInfo = executionPlanBuilder.build(from.periodicCommit, logicalPlan)(pipeBuildContext, context.planContext)
    val PipeInfo(pipe, updating, periodicCommitInfo, fp, planner) = pipeInfo
    val columns = from.statement().returnColumns
    val resultBuilderFactory = DefaultExecutionResultBuilderFactory(pipeInfo, columns, logicalPlan)
    val func = getExecutionPlanFunction(periodicCommitInfo, from.queryText, updating, resultBuilderFactory,
                                        context.notificationLogger, InterpretedRuntimeName)
    val execPlan = new ExecutionPlan {
      private val fingerprint = context.createFingerprintReference(fp)

      override def run(queryContext: QueryContext, planType: ExecutionMode, params: MapValue): InternalExecutionResult =
        func(queryContext, planType, params)

      override def isPeriodicCommit: Boolean = periodicCommitInfo.isDefined

      override def plannerUsed: PlannerName = planner

      override def isStale(lastTxId: () => Long, statistics: GraphStatistics): Boolean = fingerprint.isStale(lastTxId, statistics)

      override def runtimeUsed = InterpretedRuntimeName

      override def notifications(planContext: PlanContext): Seq[InternalNotification] = checkForNotifications(pipe, planContext, context.config)

      override def plannedIndexUsage: Seq[IndexUsage] = logicalPlan.indexUsage
    }

    new CompilationState(from, Some(execPlan))
  }

  def checkForNotifications(pipe: Pipe, planContext: PlanContext, config: CypherCompilerConfiguration): Seq[InternalNotification] = {
    val notificationCheckers = Seq(checkForEagerLoadCsv,
      CheckForLoadCsvAndMatchOnLargeLabel(planContext, config.nonIndexedLabelWarningThreshold))

    notificationCheckers.flatMap(_ (pipe))
  }

  def getExecutionPlanFunction(periodicCommit: Option[PeriodicCommitInfo],
                                       queryId: AnyRef,
                                       updating: Boolean,
                                       resultBuilderFactory: ExecutionResultBuilderFactory,
                                       notificationLogger: InternalNotificationLogger,
                                        runtimeName: RuntimeName):
  (QueryContext, ExecutionMode, MapValue) => InternalExecutionResult =
    (queryContext: QueryContext, planType: ExecutionMode, params: MapValue) => {
      val builder = resultBuilderFactory.create()

      val profiling = planType == ProfileMode
      val builderContext = if (updating || profiling) new UpdateCountingQueryContext(queryContext) else queryContext

      builder.setQueryContext(builderContext)

      if (periodicCommit.isDefined) {
        if (!builderContext.transactionalContext.isTopLevelTx)
          throw new PeriodicCommitInOpenTransactionException()
        builder.setLoadCsvPeriodicCommitObserver(periodicCommit.get.batchRowCount)
      }

      if (profiling)
        builder.setPipeDecorator(new Profiler(queryContext.transactionalContext.databaseInfo))

      builder.build(queryId, planType, params, notificationLogger, runtimeName)
    }
}
