/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
import org.neo4j.cypher.internal.compatibility.v3_5.runtime.profiler.{InterpretedProfileInformation, Profiler}
import org.neo4j.cypher.internal.compiler.v3_5.phases.LogicalPlanState
import org.neo4j.cypher.internal.runtime.{ExecutionMode, ExplainMode, ProfileMode, QueryContext}
import org.neo4j.cypher.internal.runtime.interpreted.UpdateCountingQueryContext
import org.neo4j.cypher.internal.runtime.interpreted.commands.convert.{CommunityExpressionConverter, ExpressionConverters}
import org.neo4j.cypher.internal.runtime.interpreted.pipes.PipeExecutionBuilderContext
import org.neo4j.cypher.internal.runtime.planDescription.Argument
import org.neo4j.cypher.result.RuntimeResult
import org.neo4j.values.virtual.MapValue
import org.neo4j.cypher.internal.v3_5.util.{InternalNotification, PeriodicCommitInOpenTransactionException}

object InterpretedRuntime extends CypherRuntime[RuntimeContext] {
  override def compileToExecutable(state: LogicalPlanState, context: RuntimeContext): ExecutionPlan = {
    val cardinalities = state.planningAttributes.cardinalities
    val logicalPlan = state.logicalPlan
    val converters = new ExpressionConverters(CommunityExpressionConverter(context.tokenContext))
    val executionPlanBuilder = new PipeExecutionPlanBuilder(
      expressionConverters = converters,
      pipeBuilderFactory = InterpretedPipeBuilderFactory)
    val pipeBuildContext = PipeExecutionBuilderContext(state.semanticTable(), context.readOnly)
    val pipe = executionPlanBuilder.build(logicalPlan)(pipeBuildContext, context.tokenContext)
    val periodicCommitInfo = state.periodicCommit.map(x => PeriodicCommitInfo(x.batchSize))
    val columns = state.statement().returnColumns
    val resultBuilderFactory = InterpretedExecutionResultBuilderFactory(pipe,
                                                                        context.readOnly,
                                                                        columns,
                                                                        logicalPlan,
                                                                        context.config.lenientCreateRelationship)

    new InterpretedExecutionPlan(periodicCommitInfo,
                                 resultBuilderFactory,
                                 InterpretedRuntimeName,
                                 context.readOnly)
  }

  /**
    * Executable plan for a single cypher query. Warning, this class will get cached! Do not leak transaction objects
    * or other resources in here.
    */
  class InterpretedExecutionPlan(periodicCommit: Option[PeriodicCommitInfo],
                                 resultBuilderFactory: ExecutionResultBuilderFactory,
                                 override val runtimeName: RuntimeName,
                                 readOnly: Boolean) extends ExecutionPlan {

    override def run(queryContext: QueryContext, planType: ExecutionMode, params: MapValue): RuntimeResult = {
      val doProfile = planType == ProfileMode
      val builderContext = if (!readOnly || doProfile) new UpdateCountingQueryContext(queryContext) else queryContext
      val builder = resultBuilderFactory.create(builderContext)

      val profileInformation = new InterpretedProfileInformation

      if (periodicCommit.isDefined && planType != ExplainMode) {
        if (!builderContext.transactionalContext.isTopLevelTx)
          throw new PeriodicCommitInOpenTransactionException()
        builder.setLoadCsvPeriodicCommitObserver(periodicCommit.get.batchRowCount)
      }

      if (doProfile)
        builder.setPipeDecorator(new Profiler(queryContext.transactionalContext.databaseInfo, profileInformation))

      builder.build(params,
                    readOnly,
                    profileInformation)
    }

    override def metadata: Seq[Argument] = Nil

    override def notifications: Set[InternalNotification] = Set.empty
  }
}
