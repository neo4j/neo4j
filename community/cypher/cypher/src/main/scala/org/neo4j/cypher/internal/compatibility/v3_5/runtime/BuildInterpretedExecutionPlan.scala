/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.compatibility.v3_5.runtime

import org.neo4j.cypher.internal.compatibility.v3_5.runtime.executionplan._
import org.neo4j.cypher.internal.compatibility.v3_5.runtime.phases.CompilationState
import org.neo4j.cypher.internal.compatibility.v3_5.runtime.profiler.Profiler
import org.neo4j.cypher.internal.compiler.v3_5.phases._
import org.neo4j.cypher.internal.frontend.v3_5.PlannerName
import org.neo4j.cypher.internal.frontend.v3_5.phases.CompilationPhaseTracer.CompilationPhase.PIPE_BUILDING
import org.neo4j.cypher.internal.frontend.v3_5.phases.{InternalNotificationLogger, Phase}
import org.neo4j.cypher.internal.planner.v3_5.spi.GraphStatistics
import org.neo4j.cypher.internal.planner.v3_5.spi.PlanningAttributes.Cardinalities
import org.neo4j.cypher.internal.runtime.interpreted.UpdateCountingQueryContext
import org.neo4j.cypher.internal.runtime.interpreted.commands.convert.{CommunityExpressionConverter, ExpressionConverters}
import org.neo4j.cypher.internal.runtime.interpreted.pipes.PipeExecutionBuilderContext
import org.neo4j.cypher.internal.runtime.{ExecutionMode, InternalExecutionResult, ProfileMode, QueryContext}
import org.neo4j.cypher.internal.util.v3_5.PeriodicCommitInOpenTransactionException
import org.neo4j.cypher.internal.v3_5.logical.plans.{IndexUsage, LogicalPlan}
import org.neo4j.values.virtual.MapValue

import scala.util.Success

object BuildInterpretedExecutionPlan extends Phase[CommunityRuntimeContext, LogicalPlanState, CompilationState] {
  override def phase = PIPE_BUILDING

  override def description = "create interpreted execution plan"

  override def postConditions = Set(CompilationContains[ExecutionPlan])

  override def process(from: LogicalPlanState, context: CommunityRuntimeContext): CompilationState = {
    val readOnly = from.solveds(from.logicalPlan.id).readOnly
    val cardinalities = from.cardinalities
    val logicalPlan = from.logicalPlan
    val converters = new ExpressionConverters(CommunityExpressionConverter)
    val executionPlanBuilder = new PipeExecutionPlanBuilder(expressionConverters = converters,
                                                            pipeBuilderFactory = InterpretedPipeBuilderFactory)
    val pipeBuildContext = PipeExecutionBuilderContext(from.semanticTable(), readOnly, cardinalities)
    val pipe = executionPlanBuilder.build(logicalPlan)(pipeBuildContext, context.planContext)
    val periodicCommitInfo = from.periodicCommit.map(x => PeriodicCommitInfo(x.batchSize))
    val columns = from.statement().returnColumns
    val resultBuilderFactory = InterpretedExecutionResultBuilderFactory(pipe, readOnly, columns, logicalPlan)
    val func = getExecutionPlanFunction(periodicCommitInfo,
                                        resultBuilderFactory,
                                        context.notificationLogger,
                                        from.plannerName,
                                        InterpretedRuntimeName,
                                        readOnly,
                                        cardinalities)

    val fingerprint = PlanFingerprint.take(context.clock, context.planContext.txIdProvider, context.planContext.statistics)
    val execPlan: ExecutionPlan = new InterpretedExecutionPlan(func,
                                                               logicalPlan,
                                                               periodicCommitInfo.isDefined,
                                                               from.plannerName,
                                                               context.createFingerprintReference(fingerprint))

    new CompilationState(from, Success(execPlan))
  }

  def getExecutionPlanFunction(periodicCommit: Option[PeriodicCommitInfo],
                               resultBuilderFactory: ExecutionResultBuilderFactory,
                               notificationLogger: InternalNotificationLogger,
                               plannerName: PlannerName,
                               runtimeName: RuntimeName,
                               readOnly: Boolean,
                               cardinalities: Cardinalities):
  (QueryContext, ExecutionMode, MapValue) => InternalExecutionResult =
    (queryContext: QueryContext, planType: ExecutionMode, params: MapValue) => {
      val builder = resultBuilderFactory.create()

      val profiling = planType == ProfileMode
      val builderContext = if (!readOnly || profiling) new UpdateCountingQueryContext(queryContext) else queryContext

      builder.setQueryContext(builderContext)

      if (periodicCommit.isDefined) {
        if (!builderContext.transactionalContext.isTopLevelTx)
          throw new PeriodicCommitInOpenTransactionException()
        builder.setLoadCsvPeriodicCommitObserver(periodicCommit.get.batchRowCount)
      }

      if (profiling)
        builder.setPipeDecorator(new Profiler(queryContext.transactionalContext.databaseInfo))

      builder.build(planType, params, notificationLogger, plannerName, runtimeName, readOnly, cardinalities)
    }

  /**
    * Executable plan for a single cypher query. Warning, this class will get cached! Do not leak transaction objects
    * or other resources in here.
    */
  class InterpretedExecutionPlan(val executionPlanFunc: (QueryContext, ExecutionMode, MapValue) => InternalExecutionResult,
                                 val logicalPlan: LogicalPlan,
                                 override val isPeriodicCommit: Boolean,
                                 override val plannerUsed: PlannerName,
                                 val fingerprint: PlanFingerprintReference) extends ExecutionPlan {

    override def run(queryContext: QueryContext, planType: ExecutionMode, params: MapValue): InternalExecutionResult =
      executionPlanFunc(queryContext, planType, params)

    override def checkPlanResusability(lastTxId: () => Long, statistics: GraphStatistics) = fingerprint.checkPlanReusability(lastTxId, statistics)

    override def runtimeUsed: RuntimeName = InterpretedRuntimeName

    override def plannedIndexUsage: Seq[IndexUsage] = logicalPlan.indexUsage
  }
}
