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
package org.neo4j.cypher.internal.compatibility

import org.neo4j.cypher.internal.compatibility.v4_0.runtime._
import org.neo4j.cypher.internal.compatibility.v4_0.runtime.executionplan._
import org.neo4j.cypher.internal.compatibility.v4_0.runtime.profiler.InterpretedProfileInformation
import org.neo4j.cypher.internal.compatibility.v4_0.runtime.profiler.Profiler
import org.neo4j.cypher.internal.runtime.interpreted.commands.convert.CommunityExpressionConverter
import org.neo4j.cypher.internal.runtime.interpreted.commands.convert.ExpressionConverters
import org.neo4j.cypher.internal.runtime.interpreted.pipes.NestedPipeExpressions
import org.neo4j.cypher.internal.runtime.interpreted.pipes.PipeTreeBuilder
import org.neo4j.cypher.internal.runtime.interpreted.InterpretedPipeMapper
import org.neo4j.cypher.internal.runtime.interpreted.UpdateCountingQueryContext
import org.neo4j.cypher.internal.runtime.planDescription.Argument
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.QueryIndexes
import org.neo4j.cypher.internal.v4_0.util.InternalNotification
import org.neo4j.cypher.internal.v4_0.util.PeriodicCommitInOpenTransactionException
import org.neo4j.cypher.result.RuntimeResult
import org.neo4j.values.virtual.MapValue

object InterpretedRuntime extends CypherRuntime[RuntimeContext] {
  override def compileToExecutable(query: LogicalQuery, context: RuntimeContext): ExecutionPlan = {
    val logicalPlan = query.logicalPlan
    val converters = new ExpressionConverters(CommunityExpressionConverter(context.tokenContext))
    val queryIndexes = new QueryIndexes(context.schemaRead)
    val pipeMapper = InterpretedPipeMapper(query.readOnly, converters, context.tokenContext, queryIndexes)(query.semanticTable)
    val pipeTreeBuilder = PipeTreeBuilder(pipeMapper)
    val logicalPlanWithConvertedNestedPlans = NestedPipeExpressions.build(pipeTreeBuilder, logicalPlan)
    val pipe = pipeTreeBuilder.build(logicalPlanWithConvertedNestedPlans)
    val periodicCommitInfo = query.periodicCommit.map(x => PeriodicCommitInfo(x.batchSize))
    val columns = query.resultColumns
    val resultBuilderFactory = InterpretedExecutionResultBuilderFactory(pipe,
                                                                        queryIndexes,
                                                                        query.readOnly,
                                                                        columns,
                                                                        logicalPlan,
                                                                        context.config.lenientCreateRelationship)

    new InterpretedExecutionPlan(periodicCommitInfo,
                                 resultBuilderFactory,
                                 InterpretedRuntimeName,
                                 query.readOnly)
  }

  /**
    * Executable plan for a single cypher query. Warning, this class will get cached! Do not leak transaction objects
    * or other resources in here.
    */
  class InterpretedExecutionPlan(periodicCommit: Option[PeriodicCommitInfo],
                                 resultBuilderFactory: ExecutionResultBuilderFactory,
                                 override val runtimeName: RuntimeName,
                                 readOnly: Boolean) extends ExecutionPlan {

    override def run(queryContext: QueryContext,
                     doProfile: Boolean,
                     params: MapValue,
                     prePopulateResults: Boolean): RuntimeResult = {
      val builderContext = if (!readOnly || doProfile) new UpdateCountingQueryContext(queryContext) else queryContext
      val builder = resultBuilderFactory.create(builderContext)

      val profileInformation = new InterpretedProfileInformation

      if (periodicCommit.isDefined) {
        if (!builderContext.transactionalContext.isTopLevelTx)
          throw new PeriodicCommitInOpenTransactionException()
        builder.setLoadCsvPeriodicCommitObserver(periodicCommit.get.batchRowCount)
      }

      if (doProfile)
        builder.addProfileDecorator(new Profiler(queryContext.transactionalContext.databaseInfo, profileInformation))

      builder.build(params,
                    readOnly,
                    profileInformation,
                    prePopulateResults)
    }

    override def metadata: Seq[Argument] = Nil

    override def notifications: Set[InternalNotification] = Set.empty
  }
}
