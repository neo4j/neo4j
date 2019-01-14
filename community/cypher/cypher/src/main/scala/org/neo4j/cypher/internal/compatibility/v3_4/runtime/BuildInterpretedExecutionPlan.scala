/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.compatibility.v3_4.runtime

import org.neo4j.cypher.internal.compatibility.v3_4.runtime.executionplan._
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.phases.CompilationState
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.profiler.Profiler
import org.neo4j.cypher.internal.compiler.v3_4.phases._
import org.neo4j.cypher.internal.frontend.v3_4.PlannerName
import org.neo4j.cypher.internal.frontend.v3_4.phases.CompilationPhaseTracer.CompilationPhase.PIPE_BUILDING
import org.neo4j.cypher.internal.frontend.v3_4.phases.{InternalNotificationLogger, Phase}
import org.neo4j.cypher.internal.planner.v3_4.spi.GraphStatistics
import org.neo4j.cypher.internal.planner.v3_4.spi.PlanningAttributes.{Cardinalities, ReadOnlies}
import org.neo4j.cypher.internal.runtime.interpreted.UpdateCountingQueryContext
import org.neo4j.cypher.internal.runtime.interpreted.commands.convert.{CommunityExpressionConverter, ExpressionConverters}
import org.neo4j.cypher.internal.runtime.{ExecutionMode, InternalExecutionResult, ProfileMode, QueryContext}
import org.neo4j.cypher.internal.util.v3_4.PeriodicCommitInOpenTransactionException
import org.neo4j.cypher.internal.v3_4.logical.plans.{IndexUsage, LogicalPlan}
import org.neo4j.values.virtual.MapValue

import scala.util.Success

object BuildInterpretedExecutionPlan extends Phase[CommunityRuntimeContext, LogicalPlanState, CompilationState] {
  override def phase = PIPE_BUILDING

  override def description = "create interpreted execution plan"

  override def postConditions = Set(CompilationContains[ExecutionPlan])

  override def process(from: LogicalPlanState, context: CommunityRuntimeContext): CompilationState = {
    val readOnlies = new ReadOnlies
    from.solveds.mapTo(readOnlies, _.readOnly)
    val cardinalities = from.cardinalities
    val logicalPlan = from.logicalPlan
    val converters = new ExpressionConverters(CommunityExpressionConverter)
    val executionPlanBuilder = new PipeExecutionPlanBuilder(context.clock, context.monitors,
      expressionConverters = converters, pipeBuilderFactory = CommunityPipeBuilderFactory)
    val pipeBuildContext = PipeExecutionBuilderContext(context.metrics.cardinality, from.semanticTable(), from.plannerName, readOnlies, cardinalities)
    val pipeInfo = executionPlanBuilder.build(from.periodicCommit, logicalPlan)(pipeBuildContext, context.planContext)
    val PipeInfo(pipe, updating, periodicCommitInfo, fp, planner) = pipeInfo
    val columns = from.statement().returnColumns
    val resultBuilderFactory = InterpretedExecutionResultBuilderFactory(pipeInfo, columns, logicalPlan, context.config.lenientCreateRelationship)
    val func = getExecutionPlanFunction(periodicCommitInfo, updating, resultBuilderFactory,
                                        context.notificationLogger, InterpretedRuntimeName, readOnlies, cardinalities)

    val execPlan: ExecutionPlan = new InterpretedExecutionPlan(func,
      logicalPlan,
      periodicCommitInfo.isDefined,
      planner,
      context.createFingerprintReference(fp))

    new CompilationState(from, Success(execPlan))
  }

  def getExecutionPlanFunction(periodicCommit: Option[PeriodicCommitInfo],
                               updating: Boolean,
                               resultBuilderFactory: ExecutionResultBuilderFactory,
                               notificationLogger: InternalNotificationLogger,
                               runtimeName: RuntimeName,
                               readOnlies: ReadOnlies,
                               cardinalities: Cardinalities):
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

      builder.build(planType, params, notificationLogger, runtimeName, readOnlies, cardinalities)
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
