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

import org.neo4j.cypher.internal.config.CUSTOM_MEMORY_TRACKING
import org.neo4j.cypher.internal.config.MEMORY_TRACKING
import org.neo4j.cypher.internal.config.MemoryTrackingController
import org.neo4j.cypher.internal.config.NO_TRACKING
import org.neo4j.cypher.internal.logical.plans.TransactionConcurrency
import org.neo4j.cypher.internal.logical.plans.TransactionConcurrency.Concurrent
import org.neo4j.cypher.internal.runtime.InputDataStream
import org.neo4j.cypher.internal.runtime.ParameterMapping
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.QueryIndexes
import org.neo4j.cypher.internal.runtime.QuerySelectivityTrackers
import org.neo4j.cypher.internal.runtime.createParameterArray
import org.neo4j.cypher.internal.runtime.interpreted.pipes.ExternalCSVResource
import org.neo4j.cypher.internal.runtime.interpreted.pipes.LinenumberPipeDecorator
import org.neo4j.cypher.internal.runtime.interpreted.pipes.NullPipeDecorator
import org.neo4j.cypher.internal.runtime.interpreted.pipes.Pipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.PipeDecorator
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState.createDefaultInCache
import org.neo4j.cypher.internal.runtime.interpreted.profiler.InterpretedProfileInformation
import org.neo4j.cypher.internal.runtime.memory.CustomTrackingQueryMemoryTracker
import org.neo4j.cypher.internal.runtime.memory.NoOpQueryMemoryTracker
import org.neo4j.cypher.internal.runtime.memory.ParallelTrackingQueryMemoryTracker
import org.neo4j.cypher.internal.runtime.memory.ProfilingParallelTrackingQueryMemoryTracker
import org.neo4j.cypher.internal.runtime.memory.QueryMemoryTracker
import org.neo4j.cypher.internal.runtime.memory.TrackingQueryMemoryTracker
import org.neo4j.cypher.internal.runtime.memory.TransactionWorkerThreadDelegatingMemoryTracker
import org.neo4j.cypher.result.RuntimeResult
import org.neo4j.internal.kernel.api.DefaultCloseListenable
import org.neo4j.kernel.impl.query.QuerySubscriber
import org.neo4j.scheduler.CallableExecutor
import org.neo4j.scheduler.Group
import org.neo4j.values.AnyValue
import org.neo4j.values.virtual.MapValue

abstract class BaseExecutionResultBuilderFactory(
  pipe: Pipe,
  columns: Seq[String],
  hasLoadCSV: Boolean,
  startsTransactions: Option[TransactionConcurrency]
) extends ExecutionResultBuilderFactory {

  abstract class BaseExecutionResultBuilder() extends ExecutionResultBuilder {
    protected var externalResource: ExternalCSVResource = new CSVResources(queryContext.resources)
    protected var pipeDecorator: PipeDecorator = if (hasLoadCSV) new LinenumberPipeDecorator() else NullPipeDecorator

    protected val transactionWorkerExecutor: Option[CallableExecutor] = startsTransactions match {
      case Some(Concurrent(_)) =>
        Some(queryContext.jobScheduler.executor(Group.CYPHER_TRANSACTION_WORKER))
      case _ => None
    }

    protected def createQueryMemoryTracker(
      memoryTrackingController: MemoryTrackingController,
      profile: Boolean,
      queryContext: QueryContext
    ): QueryMemoryTracker = {
      (memoryTrackingController.memoryTracking, startsTransactions) match {
        case (NO_TRACKING, _) => NoOpQueryMemoryTracker
        case (MEMORY_TRACKING, Some(Concurrent(_))) =>
          val delegateFactory = () => {
            new TransactionWorkerThreadDelegatingMemoryTracker
          }
          val mainThreadMemoryTracker = queryContext.transactionalContext.createExecutionContextMemoryTracker()
          val mt = if (profile) {
            new ProfilingParallelTrackingQueryMemoryTracker(delegateFactory)
          } else {
            new ParallelTrackingQueryMemoryTracker(delegateFactory)
          }
          // mainThreadMemoryTracker should be closed together with the query context
          queryContext.resources.trace(DefaultCloseListenable.wrap(mainThreadMemoryTracker))
          mt.setInitializationMemoryTracker(mainThreadMemoryTracker)
          mt
        case (MEMORY_TRACKING, _)                   => new TrackingQueryMemoryTracker
        case (CUSTOM_MEMORY_TRACKING(decorator), _) => new CustomTrackingQueryMemoryTracker(decorator)
      }
    }

    protected def createQueryState(
      params: MapValue,
      prePopulateResults: Boolean,
      input: InputDataStream,
      subscriber: QuerySubscriber,
      doProfile: Boolean,
      profileInformation: InterpretedProfileInformation
    ): QueryState

    def queryContext: QueryContext

    def addProfileDecorator(profileDecorator: PipeDecorator): Unit = pipeDecorator match {
      case decorator: LinenumberPipeDecorator => decorator.setInnerDecorator(profileDecorator)
      case _                                  => pipeDecorator = profileDecorator
    }

    override def build(
      params: MapValue,
      queryProfile: InterpretedProfileInformation,
      prePopulateResults: Boolean,
      input: InputDataStream,
      subscriber: QuerySubscriber,
      doProfile: Boolean
    ): RuntimeResult = {
      val state = createQueryState(params, prePopulateResults, input, subscriber, doProfile, queryProfile)
      new PipeExecutionResult(pipe, columns.toArray, state, queryProfile, subscriber, startsTransactions.isDefined)
    }
  }

}

case class InterpretedExecutionResultBuilderFactory(
  pipe: Pipe,
  queryIndexes: QueryIndexes,
  querySelectivityTrackers: QuerySelectivityTrackers,
  nExpressionSlots: Int,
  parameterMapping: ParameterMapping,
  columns: Seq[String],
  lenientCreateRelationship: Boolean,
  memoryTrackingController: MemoryTrackingController,
  hasLoadCSV: Boolean,
  startsTransactions: Option[TransactionConcurrency]
) extends BaseExecutionResultBuilderFactory(pipe, columns, hasLoadCSV, startsTransactions) {

  override def create(queryContext: QueryContext): ExecutionResultBuilder =
    InterpretedExecutionResultBuilder(queryContext: QueryContext)

  case class InterpretedExecutionResultBuilder(queryContext: QueryContext) extends BaseExecutionResultBuilder {

    override def createQueryState(
      params: MapValue,
      prePopulateResults: Boolean,
      input: InputDataStream,
      subscriber: QuerySubscriber,
      doProfile: Boolean,
      profileInformation: InterpretedProfileInformation
    ): QueryState = {
      val cursors = queryContext.createExpressionCursors()
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
        if (doProfile) profileInformation else null,
        transactionWorkerExecutor
      )
    }
  }
}
