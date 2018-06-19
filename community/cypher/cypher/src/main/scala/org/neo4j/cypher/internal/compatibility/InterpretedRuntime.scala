/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.cypher.internal.compatibility

import org.neo4j.cypher.internal.compatibility.v3_5.runtime._
import org.neo4j.cypher.internal.compatibility.v3_5.runtime.executionplan._
import org.neo4j.cypher.internal.compatibility.v3_5.runtime.profiler.{InterpretedProfilerInformation, Profiler}
import org.neo4j.cypher.internal.compiler.v3_5.phases.LogicalPlanState
import org.neo4j.cypher.internal.planner.v3_5.spi.PlanningAttributes.Cardinalities
import org.neo4j.cypher.internal.runtime.interpreted.UpdateCountingQueryContext
import org.neo4j.cypher.internal.runtime.interpreted.commands.convert.{CommunityExpressionConverter, ExpressionConverters}
import org.neo4j.cypher.internal.runtime.interpreted.pipes.PipeExecutionBuilderContext
import org.neo4j.cypher.internal.runtime.{ExecutionMode, InternalExecutionResult, ProfileMode, QueryContext}
import org.neo4j.cypher.internal.v3_5.logical.plans.{IndexUsage, LogicalPlan}
import org.neo4j.values.virtual.MapValue
import org.opencypher.v9_0.frontend.PlannerName
import org.opencypher.v9_0.frontend.phases.InternalNotificationLogger
import org.opencypher.v9_0.util.PeriodicCommitInOpenTransactionException

object InterpretedRuntime extends CypherRuntime[RuntimeContext] {
  override def compileToExecutable(state: LogicalPlanState, context: RuntimeContext): ExecutionPlan = {
    val cardinalities = state.cardinalities
    val logicalPlan = state.logicalPlan
    val converters = new ExpressionConverters(CommunityExpressionConverter)
    val executionPlanBuilder = new PipeExecutionPlanBuilder(
      expressionConverters = converters,
      pipeBuilderFactory = InterpretedPipeBuilderFactory)
    val pipeBuildContext = PipeExecutionBuilderContext(state.semanticTable(), context.readOnly, cardinalities)
    val pipe = executionPlanBuilder.build(logicalPlan)(pipeBuildContext, context.tokenContext)
    val periodicCommitInfo = state.periodicCommit.map(x => PeriodicCommitInfo(x.batchSize))
    val columns = state.statement().returnColumns
    val resultBuilderFactory = InterpretedExecutionResultBuilderFactory(pipe, context.readOnly, columns, logicalPlan)

    new InterpretedExecutionPlan(periodicCommitInfo,
                                 resultBuilderFactory,
                                 context.notificationLogger,
                                 state.plannerName,
                                 InterpretedRuntimeName,
                                 context.readOnly,
                                 cardinalities,
                                 logicalPlan.indexUsage)
  }

  /**
    * Executable plan for a single cypher query. Warning, this class will get cached! Do not leak transaction objects
    * or other resources in here.
    */
  class InterpretedExecutionPlan(periodicCommit: Option[PeriodicCommitInfo],
                                 resultBuilderFactory: ExecutionResultBuilderFactory,
                                 notificationLogger: InternalNotificationLogger,
                                 override val plannerUsed: PlannerName,
                                 override val runtimeUsed: RuntimeName,
                                 readOnly: Boolean,
                                 cardinalities: Cardinalities,
                                 override val plannedIndexUsage: Seq[IndexUsage]) extends ExecutionPlan {

    override def run(queryContext: QueryContext, planType: ExecutionMode, params: MapValue): InternalExecutionResult = {
      val builder = resultBuilderFactory.create()

      val profileInformation = new InterpretedProfilerInformation
      val profiling = planType == ProfileMode
      val builderContext = if (!readOnly || profiling) new UpdateCountingQueryContext(queryContext) else queryContext

      builder.setQueryContext(builderContext)

      if (periodicCommit.isDefined) {
        if (!builderContext.transactionalContext.isTopLevelTx)
          throw new PeriodicCommitInOpenTransactionException()
        builder.setLoadCsvPeriodicCommitObserver(periodicCommit.get.batchRowCount)
      }

      if (profiling)
        builder.setPipeDecorator(new Profiler(queryContext.transactionalContext.databaseInfo, profileInformation))

      builder.build(planType, params, notificationLogger, plannerUsed, InterpretedRuntimeName, readOnly, cardinalities)
    }

    override def isPeriodicCommit: Boolean = periodicCommit.isDefined
  }
}
