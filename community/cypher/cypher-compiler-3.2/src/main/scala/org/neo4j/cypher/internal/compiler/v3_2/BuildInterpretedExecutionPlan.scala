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
package org.neo4j.cypher.internal.compiler.v3_2

import org.neo4j.cypher.internal.compiler.v3_2.CompilationPhaseTracer.CompilationPhase.PIPE_BUILDING
import org.neo4j.cypher.internal.compiler.v3_2.executionplan.{PipeInfo, _}
import org.neo4j.cypher.internal.compiler.v3_2.phases._
import org.neo4j.cypher.internal.compiler.v3_2.pipes.Pipe
import org.neo4j.cypher.internal.compiler.v3_2.planner.execution.{PipeExecutionBuilderContext, PipeExecutionPlanBuilder}
import org.neo4j.cypher.internal.compiler.v3_2.planner.logical.LogicalPlanIdentificationBuilder
import org.neo4j.cypher.internal.compiler.v3_2.profiler.Profiler
import org.neo4j.cypher.internal.compiler.v3_2.spi.{GraphStatistics, PlanContext, QueryContext, UpdateCountingQueryContext}
import org.neo4j.cypher.internal.frontend.v3_2.PeriodicCommitInOpenTransactionException
import org.neo4j.cypher.internal.frontend.v3_2.notification.InternalNotification

object BuildInterpretedExecutionPlan extends Phase {
  override def phase = PIPE_BUILDING

  override def description = "create interpreted execution plan"

  override def postConditions = Set(Contains[ExecutionPlan])

  override def process(from: CompilationState, context: Context): CompilationState = {
    val logicalPlan = from.logicalPlan
    val idMap = LogicalPlanIdentificationBuilder(logicalPlan)
    val executionPlanBuilder = new PipeExecutionPlanBuilder(context.clock, context.monitors)
    val pipeBuildContext = PipeExecutionBuilderContext(context.metrics.cardinality, from.semanticTable, from.plannerName)
    val pipeInfo = executionPlanBuilder.build(from.periodicCommit, logicalPlan, idMap)(pipeBuildContext, context.planContext)
    val PipeInfo(pipe, updating, periodicCommitInfo, fp, planner) = pipeInfo
    val columns = from.statement.returnColumns
    val resultBuilderFactory = DefaultExecutionResultBuilderFactory(pipeInfo, columns, context.typeConverter, logicalPlan, idMap)
    val func = getExecutionPlanFunction(periodicCommitInfo, from.queryText, updating, resultBuilderFactory, context.notificationLogger)
    val execPlan = new ExecutionPlan {
      private val fingerprint = context.createFingerprintReference(fp)

      override def run(queryContext: QueryContext, planType: ExecutionMode, params: Map[String, Any]) =
        func(queryContext, planType, params)

      override def isPeriodicCommit = periodicCommitInfo.isDefined

      override def plannerUsed = planner

      override def isStale(lastTxId: () => Long, statistics: GraphStatistics) = fingerprint.isStale(lastTxId, statistics)

      override def runtimeUsed = InterpretedRuntimeName

      override def notifications(planContext: PlanContext) = checkForNotifications(pipe, planContext, context.config)

      override def plannedIndexUsage = logicalPlan.indexUsage
    }

    from.copy(maybeExecutionPlan = Some(execPlan))
  }

  private def checkForNotifications(pipe: Pipe, planContext: PlanContext, config: CypherCompilerConfiguration): Seq[InternalNotification] = {
    val notificationCheckers = Seq(checkForEagerLoadCsv,
      CheckForLoadCsvAndMatchOnLargeLabel(planContext, config.nonIndexedLabelWarningThreshold))

    notificationCheckers.flatMap(_ (pipe))
  }

  private def getExecutionPlanFunction(periodicCommit: Option[PeriodicCommitInfo],
                                       queryId: AnyRef,
                                       updating: Boolean,
                                       resultBuilderFactory: ExecutionResultBuilderFactory,
                                       notificationLogger: InternalNotificationLogger):
  (QueryContext, ExecutionMode, Map[String, Any]) => InternalExecutionResult =
    (queryContext: QueryContext, planType: ExecutionMode, params: Map[String, Any]) => {
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
        builder.setPipeDecorator(new Profiler())

      builder.build(queryId, planType, params, notificationLogger)
    }
}
