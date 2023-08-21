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
package org.neo4j.cypher.internal.runtime.interpreted

import org.neo4j.cypher.internal.config.MemoryTrackingController
import org.neo4j.cypher.internal.runtime.InputDataStream
import org.neo4j.cypher.internal.runtime.ParameterMapping
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.QueryIndexes
import org.neo4j.cypher.internal.runtime.createParameterArray
import org.neo4j.cypher.internal.runtime.interpreted.pipes.ExternalCSVResource
import org.neo4j.cypher.internal.runtime.interpreted.pipes.LinenumberPipeDecorator
import org.neo4j.cypher.internal.runtime.interpreted.pipes.NullPipeDecorator
import org.neo4j.cypher.internal.runtime.interpreted.pipes.Pipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.PipeDecorator
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState.createDefaultInCache
import org.neo4j.cypher.result.QueryProfile
import org.neo4j.cypher.result.RuntimeResult
import org.neo4j.kernel.impl.query.QuerySubscriber
import org.neo4j.values.AnyValue
import org.neo4j.values.virtual.MapValue

abstract class BaseExecutionResultBuilderFactory(
  pipe: Pipe,
  columns: Seq[String],
  hasLoadCSV: Boolean,
  startsTransactions: Boolean
) extends ExecutionResultBuilderFactory {

  abstract class BaseExecutionResultBuilder() extends ExecutionResultBuilder {
    protected var externalResource: ExternalCSVResource = new CSVResources(queryContext.resources)
    protected var pipeDecorator: PipeDecorator = if (hasLoadCSV) new LinenumberPipeDecorator() else NullPipeDecorator

    protected def createQueryState(
      params: MapValue,
      prePopulateResults: Boolean,
      input: InputDataStream,
      subscriber: QuerySubscriber,
      doProfile: Boolean
    ): QueryState

    def queryContext: QueryContext

    def addProfileDecorator(profileDecorator: PipeDecorator): Unit = pipeDecorator match {
      case decorator: LinenumberPipeDecorator => decorator.setInnerDecorator(profileDecorator)
      case _                                  => pipeDecorator = profileDecorator
    }

    override def build(
      params: MapValue,
      queryProfile: QueryProfile,
      prePopulateResults: Boolean,
      input: InputDataStream,
      subscriber: QuerySubscriber,
      doProfile: Boolean
    ): RuntimeResult = {
      val state = createQueryState(params, prePopulateResults, input, subscriber, doProfile)
      new PipeExecutionResult(pipe, columns.toArray, state, queryProfile, subscriber, startsTransactions)
    }
  }

}

case class InterpretedExecutionResultBuilderFactory(
  pipe: Pipe,
  queryIndexes: QueryIndexes,
  nExpressionSlots: Int,
  parameterMapping: ParameterMapping,
  columns: Seq[String],
  lenientCreateRelationship: Boolean,
  memoryTrackingController: MemoryTrackingController,
  hasLoadCSV: Boolean,
  startsTransactions: Boolean
) extends BaseExecutionResultBuilderFactory(pipe, columns, hasLoadCSV, startsTransactions) {

  override def create(queryContext: QueryContext): ExecutionResultBuilder =
    InterpretedExecutionResultBuilder(queryContext: QueryContext)

  case class InterpretedExecutionResultBuilder(queryContext: QueryContext) extends BaseExecutionResultBuilder {

    override def createQueryState(
      params: MapValue,
      prePopulateResults: Boolean,
      input: InputDataStream,
      subscriber: QuerySubscriber,
      doProfile: Boolean
    ): QueryState = {
      val cursors = queryContext.createExpressionCursors()

      QueryState(
        queryContext,
        externalResource,
        createParameterArray(params, parameterMapping),
        cursors,
        queryIndexes.initiateLabelAndSchemaIndexes(queryContext),
        queryIndexes.initiateNodeTokenIndex(queryContext),
        queryIndexes.initiateRelationshipTokenIndex(queryContext),
        new Array[AnyValue](nExpressionSlots),
        subscriber,
        memoryTrackingController,
        doProfile,
        pipeDecorator,
        initialContext = None,
        cachedIn = createDefaultInCache(),
        lenientCreateRelationship = lenientCreateRelationship,
        prePopulateResults = prePopulateResults,
        input = input
      )
    }
  }
}
