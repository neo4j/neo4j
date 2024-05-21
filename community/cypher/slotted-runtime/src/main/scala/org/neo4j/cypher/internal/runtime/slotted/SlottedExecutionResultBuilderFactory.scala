/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.runtime.slotted

import org.neo4j.cypher.internal.config.MemoryTrackingController
import org.neo4j.cypher.internal.logical.plans.TransactionConcurrency
import org.neo4j.cypher.internal.runtime.ExpressionCursors
import org.neo4j.cypher.internal.runtime.InputDataStream
import org.neo4j.cypher.internal.runtime.ParameterMapping
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.QueryIndexes
import org.neo4j.cypher.internal.runtime.QuerySelectivityTrackers
import org.neo4j.cypher.internal.runtime.createParameterArray
import org.neo4j.cypher.internal.runtime.interpreted.BaseExecutionResultBuilderFactory
import org.neo4j.cypher.internal.runtime.interpreted.ExecutionResultBuilder
import org.neo4j.cypher.internal.runtime.interpreted.pipes.Pipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState.createDefaultInCache
import org.neo4j.cypher.internal.runtime.interpreted.profiler.InterpretedProfileInformation
import org.neo4j.kernel.impl.query.QuerySubscriber
import org.neo4j.memory.MemoryTracker
import org.neo4j.values.AnyValue
import org.neo4j.values.virtual.MapValue

class SlottedExecutionResultBuilderFactory(
  pipe: Pipe,
  queryIndexes: QueryIndexes,
  querySelectivityTrackers: QuerySelectivityTrackers,
  nExpressionSlots: Int,
  columns: Seq[String],
  parameterMapping: ParameterMapping,
  lenientCreateRelationship: Boolean,
  memoryTrackingController: MemoryTrackingController,
  hasLoadCSV: Boolean,
  startsTransactions: Option[TransactionConcurrency]
) extends BaseExecutionResultBuilderFactory(pipe, columns, hasLoadCSV, startsTransactions) {

  override def create(queryContext: QueryContext): ExecutionResultBuilder = SlottedExecutionResultBuilder(queryContext)

  case class SlottedExecutionResultBuilder(queryContext: QueryContext) extends BaseExecutionResultBuilder {

    val transactionMemoryTracker: MemoryTracker = queryContext.transactionalContext.memoryTracker

    val cursors = new ExpressionCursors(
      queryContext.transactionalContext.cursors,
      queryContext.transactionalContext.cursorContext,
      transactionMemoryTracker
    )
    queryContext.resources.trace(cursors)

    override protected def createQueryState(
      params: MapValue,
      prePopulateResults: Boolean,
      input: InputDataStream,
      subscriber: QuerySubscriber,
      doProfile: Boolean,
      profileInformation: InterpretedProfileInformation
    ): QueryState = {
      val queryMemoryTracker = createQueryMemoryTracker(memoryTrackingController, doProfile, queryContext)
      QueryState(
        queryContext,
        externalResource,
        createParameterArray(params, parameterMapping),
        cursors,
        queryIndexes.initiateLabelAndSchemaIndexes(queryContext),
        querySelectivityTrackers.initializeTrackers(),
        queryIndexes.initiateNodeTokenIndex(queryContext),
        queryIndexes.initiateRelationshipTokenIndex(queryContext),
        new Array[AnyValue](nExpressionSlots),
        subscriber,
        queryMemoryTracker,
        pipeDecorator,
        initialContext = None,
        cachedIn = createDefaultInCache(),
        lenientCreateRelationship = lenientCreateRelationship,
        prePopulateResults = prePopulateResults,
        input = input,
        profileInformation,
        transactionWorkerExecutor = transactionWorkerExecutor
      )
    }
  }
}
