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
package org.neo4j.cypher.internal.runtime.interpreted.pipes

import org.neo4j.cypher.internal.GQLExceptionsHelper.requireImplicitTransaction
import org.neo4j.cypher.internal.macros.AssertMacros
import org.neo4j.cypher.internal.notification.AggregationSkippedNull
import org.neo4j.cypher.internal.notification.InternalNotification
import org.neo4j.cypher.internal.planner.spi.IndexComparatorFactory
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.InputDataStream
import org.neo4j.cypher.internal.runtime.MapCypherRow
import org.neo4j.cypher.internal.runtime.NoInput
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.QueryStatistics
import org.neo4j.cypher.internal.runtime.ReadableRow
import org.neo4j.cypher.internal.runtime.RuntimeNotifier
import org.neo4j.cypher.internal.runtime.SelectivityTrackerStorage
import org.neo4j.cypher.internal.runtime.cursors.ExpressionCursors
import org.neo4j.cypher.internal.runtime.interpreted.CSVResources
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState.createDefaultInCache
import org.neo4j.cypher.internal.runtime.interpreted.profiler.InterpretedProfileInformation
import org.neo4j.cypher.internal.runtime.interpreted.profiler.Profiler
import org.neo4j.cypher.internal.runtime.memory.MemoryTrackerForOperatorProvider
import org.neo4j.cypher.internal.runtime.memory.QueryMemoryTracker
import org.neo4j.exceptions.InternalException
import org.neo4j.internal.kernel
import org.neo4j.internal.kernel.api.IndexReadSession
import org.neo4j.internal.kernel.api.TokenReadSession
import org.neo4j.io.IOUtils.closeAll
import org.neo4j.kernel.api.KernelTransaction
import org.neo4j.kernel.impl.query.QuerySubscriber
import org.neo4j.scheduler.CallableExecutor
import org.neo4j.values.AnyValue
import org.neo4j.values.utils.InCache

import java.util

class QueryState(
  val query: QueryContext,
  val resources: ExternalCSVResource,
  val params: Array[AnyValue],
  val cursors: ExpressionCursors,
  val queryIndexes: Array[IndexReadSession],
  val selectivityTrackerStorage: SelectivityTrackerStorage,
  val nodeLabelTokenReadSession: Option[TokenReadSession],
  val relTypeTokenReadSession: Option[TokenReadSession],
  val expressionVariables: Array[AnyValue],
  val subscriber: QuerySubscriber,
  val queryMemoryTracker: QueryMemoryTracker,
  val memoryTrackerForOperatorProvider: MemoryTrackerForOperatorProvider,
  val decorator: PipeDecorator = NullPipeDecorator,
  val initialContext: Option[CypherRow] = None,
  val cachedIn: InCache = createDefaultInCache(),
  val indexComparatorFactory: IndexComparatorFactory,
  val lenientCreateRelationship: Boolean = false,
  val prePopulateResults: Boolean = false,
  val input: InputDataStream = NoInput,
  val profileInformation: InterpretedProfileInformation = null,
  val transactionWorkerExecutor: Option[CallableExecutor] = None,
  val notifications: util.Set[InternalNotification] = new util.HashSet[InternalNotification](),
  val warnOnAggregationSkipNull: Boolean = false
) extends AutoCloseable with RuntimeNotifier {

  private var _rowFactory: CypherRowFactory = _
  private var _closed = false

  // NOTE: used as a simple cache to avoid flooding the map with adding the same object,
  //       assigning it as AggregationSkippedNull when we shouldn't produce this error is a somewhat
  //       silly micro optimization to avoid an extra equality check.
  private[this] var lastCachedNotification: InternalNotification =
    if (!warnOnAggregationSkipNull) AggregationSkippedNull else null

  def newRow(rowFactory: CypherRowFactory): CypherRow = {
    initialContext match {
      case Some(init) => rowFactory.copyWith(init)
      case None       => rowFactory.newRow()
    }
  }

  def newRuntimeNotification(notification: InternalNotification): Unit = {
    if (notification ne lastCachedNotification) {
      if (warnOnAggregationSkipNull || (notification ne AggregationSkippedNull)) {
        notifications.add(notification)
      }
      lastCachedNotification = notification
    }
  }

  /**
   * When running on the RHS of an Apply, this method will fill the new row with argument data
   */
  def newRowWithArgument(rowFactory: CypherRowFactory): CypherRow = {
    initialContext match {
      case Some(init) => rowFactory.copyArgumentOf(init)
      case None       => rowFactory.newRow()
    }
  }

  def getStatistics: QueryStatistics = query.getOptStatistics.getOrElse(QueryState.defaultStatistics)

  def withDecorator(decorator: PipeDecorator): QueryState =
    new QueryState(
      query,
      resources,
      params,
      cursors,
      queryIndexes,
      selectivityTrackerStorage,
      nodeLabelTokenReadSession,
      relTypeTokenReadSession,
      expressionVariables,
      subscriber,
      queryMemoryTracker,
      memoryTrackerForOperatorProvider,
      decorator,
      initialContext,
      cachedIn,
      indexComparatorFactory,
      lenientCreateRelationship,
      prePopulateResults,
      input,
      profileInformation,
      transactionWorkerExecutor,
      notifications
    )

  def withInitialContext(initialContext: CypherRow): QueryState =
    new QueryState(
      query,
      resources,
      params,
      cursors,
      queryIndexes,
      selectivityTrackerStorage,
      nodeLabelTokenReadSession,
      relTypeTokenReadSession,
      expressionVariables,
      subscriber,
      queryMemoryTracker,
      memoryTrackerForOperatorProvider,
      decorator,
      Some(initialContext),
      cachedIn,
      indexComparatorFactory,
      lenientCreateRelationship,
      prePopulateResults,
      input,
      profileInformation,
      transactionWorkerExecutor,
      notifications
    )

  def withInitialContextAndDecorator(initialContext: CypherRow, newDecorator: PipeDecorator): QueryState =
    new QueryState(
      query,
      resources,
      params,
      cursors,
      queryIndexes,
      selectivityTrackerStorage,
      nodeLabelTokenReadSession,
      relTypeTokenReadSession,
      expressionVariables,
      subscriber,
      queryMemoryTracker,
      memoryTrackerForOperatorProvider,
      newDecorator,
      Some(initialContext),
      cachedIn,
      indexComparatorFactory,
      lenientCreateRelationship,
      prePopulateResults,
      input,
      profileInformation,
      transactionWorkerExecutor,
      notifications
    )

  def withQueryContext(query: QueryContext): QueryState =
    new QueryState(
      query,
      resources,
      params,
      cursors,
      queryIndexes,
      selectivityTrackerStorage,
      nodeLabelTokenReadSession,
      relTypeTokenReadSession,
      expressionVariables,
      subscriber,
      queryMemoryTracker,
      memoryTrackerForOperatorProvider,
      decorator,
      initialContext,
      cachedIn,
      indexComparatorFactory,
      lenientCreateRelationship,
      prePopulateResults,
      input,
      profileInformation,
      transactionWorkerExecutor,
      notifications
    )

  def withNewTransaction(concurrentAccess: Boolean): QueryState = {
    if (query.getTransactionType != KernelTransaction.Type.IMPLICIT) {
      throw requireImplicitTransaction(
        "A query with 'CALL { ... } IN TRANSACTIONS' can only be executed in an implicit transaction, " + "but tried to execute in an explicit transaction."
      )
    }
    val newQuery = query.contextWithNewTransaction()

    val newCursors = newQuery.createExpressionCursors()

    // This method is not supported when we run with PERIODIC COMMIT, so we assert that we do not have such resources.
    AssertMacros.checkOnlyWhenAssertionsAreEnabled(resources.isInstanceOf[CSVResources])
    val newResources = new CSVResources(newQuery.resources)

    // IndexReadSession and TokenReadSession are bound to the outer transaction.
    // They use a ValueIndexReader / TokenIndexReader that is cached and closed together with the transaction.
    // But apart from that they seem to be safe to be used from different transactions from the same thread.
    // Nevertheless we create new sessions here to protect against future modifications of IndexReadSession and TokenReadSession that
    // would actually break from two different transaction.
    // When concurrentAccess is false, an optimization could be to only create new sessions for those indexes that
    // are actually used in the new transaction.
    val newQueryIndexes = queryIndexes.map(i => newQuery.transactionalContext.dataRead.indexReadSession(i.reference()))
    val newNodeLabelTokenReadSession =
      nodeLabelTokenReadSession.map(t => newQuery.transactionalContext.dataRead.tokenReadSession(t.reference()))
    val newRelTypeTokenReadSession =
      relTypeTokenReadSession.map(t => newQuery.transactionalContext.dataRead.tokenReadSession(t.reference()))

    // Reusing the expressionVariables should work when concurrentAccess is false
    val newExpressionVariables =
      if (concurrentAccess) {
        new Array[AnyValue](expressionVariables.length)
      } else {
        expressionVariables
      }

    // Reusing the decorator should work when concurrentAccess is false
    val (newDecorator, newProfileInformation) = maybeCreateNewProfileDecorator(decorator, concurrentAccess)

    // Reusing the IN cache should work when concurrentAccess is false
    val newCachedIn =
      if (concurrentAccess) {
        createDefaultInCache()
      } else {
        cachedIn
      }

    QueryState(
      newQuery,
      newResources,
      params,
      newCursors,
      newQueryIndexes,
      selectivityTrackerStorage,
      newNodeLabelTokenReadSession,
      newRelTypeTokenReadSession,
      newExpressionVariables,
      subscriber,
      queryMemoryTracker,
      newDecorator,
      initialContext,
      newCachedIn,
      indexComparatorFactory,
      lenientCreateRelationship,
      prePopulateResults,
      input,
      newProfileInformation,
      transactionWorkerExecutor,
      notifications,
      warnOnAggregationSkipNull
    )
  }

  def withNewCursors(cursors: ExpressionCursors): QueryState = {
    new QueryState(
      query,
      resources,
      params,
      cursors,
      queryIndexes,
      selectivityTrackerStorage,
      nodeLabelTokenReadSession,
      relTypeTokenReadSession,
      expressionVariables,
      subscriber,
      queryMemoryTracker,
      memoryTrackerForOperatorProvider,
      decorator,
      initialContext,
      cachedIn,
      indexComparatorFactory,
      lenientCreateRelationship,
      prePopulateResults,
      input,
      profileInformation,
      transactionWorkerExecutor,
      notifications
    )
  }

  private def maybeCreateNewProfileDecorator(
    decorator: PipeDecorator,
    concurrentAccess: Boolean
  ): (PipeDecorator, InterpretedProfileInformation) = {
    var linenumberDecorator: LinenumberPipeDecorator = null
    var profiler: Profiler = null
    if (concurrentAccess) {
      decorator match {
        case d: LinenumberPipeDecorator =>
          linenumberDecorator = d
          val inner = d.getInnerDecorator
          inner match {
            case p: Profiler =>
              profiler = p
            case _ =>
            // Do nothing
          }
        case p: Profiler =>
          profiler = p
        case _ =>
        // Do nothing
      }
    }
    if (profiler != null) {
      val newProfileInformation = new InterpretedProfileInformation
      val newProfiler = profiler.withProfileInformation(newProfileInformation)
      if (linenumberDecorator != null) {
        (new LinenumberPipeDecorator(newProfiler), newProfileInformation)
      } else {
        (newProfiler, newProfileInformation)
      }
    } else {
      (decorator, profileInformation)
    }
  }

  def setExecutionContextFactory(rowFactory: CypherRowFactory): Unit = {
    _rowFactory = rowFactory
  }

  def rowFactory: CypherRowFactory = _rowFactory

  def kernelQueryContext: kernel.api.QueryContext = query.transactionalContext.kernelQueryContext

  override def close(): Unit = {
    if (!_closed) {
      closeAll(cursors, cachedIn, query)
      _closed = true
    }
  }

  override def toString: String = {
    s"${this.getClass.getSimpleName}(interpreted, doProfile=${profileInformation != null}, numberOfParams=${params.length}, prePopulateResults=$prePopulateResults)"
  }
}

object QueryState {

  val defaultStatistics: QueryStatistics = QueryStatistics()

  val inCacheMaxSize: Int = 16

  def createDefaultInCache(): InCache = new InCache(inCacheMaxSize)

  def apply(
    query: QueryContext,
    resources: ExternalCSVResource,
    params: Array[AnyValue],
    cursors: ExpressionCursors,
    queryIndexes: Array[IndexReadSession],
    selectivityTrackerStorage: SelectivityTrackerStorage,
    nodeLabelTokenReadSession: Option[TokenReadSession],
    relTypeTokenReadSession: Option[TokenReadSession],
    expressionVariables: Array[AnyValue],
    subscriber: QuerySubscriber,
    queryHeapHighWatermarkTracker: QueryMemoryTracker,
    decorator: PipeDecorator,
    initialContext: Option[CypherRow],
    cachedIn: InCache,
    indexComparatorFactory: IndexComparatorFactory,
    lenientCreateRelationship: Boolean,
    prePopulateResults: Boolean,
    input: InputDataStream,
    profileInformation: InterpretedProfileInformation,
    transactionWorkerExecutor: Option[CallableExecutor],
    notifications: util.Set[InternalNotification],
    warnOnAggregationSkipNull: Boolean
  ): QueryState = {
    val memoryTrackerForOperatorProvider =
      queryHeapHighWatermarkTracker.newMemoryTrackerForOperatorProvider(query.transactionalContext.memoryTracker)
    new QueryState(
      query,
      resources,
      params,
      cursors,
      queryIndexes,
      selectivityTrackerStorage,
      nodeLabelTokenReadSession,
      relTypeTokenReadSession,
      expressionVariables,
      subscriber,
      queryHeapHighWatermarkTracker,
      memoryTrackerForOperatorProvider,
      decorator,
      initialContext,
      cachedIn,
      indexComparatorFactory,
      lenientCreateRelationship,
      prePopulateResults,
      input,
      profileInformation,
      transactionWorkerExecutor,
      notifications,
      warnOnAggregationSkipNull
    )
  }
}

trait CypherRowFactory {

  def newRow(): CypherRow

  def copyArgumentOf(row: ReadableRow): CypherRow

  def copyWith(row: ReadableRow): CypherRow

  def copyWith(row: ReadableRow, key: String, value: AnyValue): CypherRow

  def copyWith(row: ReadableRow, key1: String, value1: AnyValue, key2: String, value2: AnyValue): CypherRow

  def copyWith(
    row: ReadableRow,
    key1: String,
    value1: AnyValue,
    key2: String,
    value2: AnyValue,
    key3: String,
    value3: AnyValue
  ): CypherRow
}

case class CommunityCypherRowFactory() extends CypherRowFactory {

  override def newRow(): CypherRow = CypherRow.empty

  override def copyArgumentOf(row: ReadableRow): CypherRow = copyWith(row)

  // Not using polymorphism here, instead cast since the cost of being megamorhpic is too high
  override def copyWith(row: ReadableRow): CypherRow = row match {
    case context: MapCypherRow =>
      context.createClone()

    case x =>
      throw InternalException.internalError(getClass.getSimpleName, s"Unexpected row type $x")
  }

  // Not using polymorphism here, instead cast since the cost of being megamorhpic is too high
  override def copyWith(row: ReadableRow, key: String, value: AnyValue): CypherRow = row match {
    case context: MapCypherRow =>
      context.copyWith(key, value)

    case x =>
      throw InternalException.internalError(getClass.getSimpleName, s"Unexpected row type $x")
  }

  // Not using polymorphism here, instead cast since the cost of being megamorhpic is too high
  override def copyWith(row: ReadableRow, key1: String, value1: AnyValue, key2: String, value2: AnyValue): CypherRow =
    row match {
      case context: MapCypherRow =>
        context.copyWith(key1, value1, key2, value2)

      case x =>
        throw InternalException.internalError(getClass.getSimpleName, s"Unexpected row type $x")
    }

  // Not using polymorphism here, instead cast since the cost of being megamorhpic is too high
  override def copyWith(
    row: ReadableRow,
    key1: String,
    value1: AnyValue,
    key2: String,
    value2: AnyValue,
    key3: String,
    value3: AnyValue
  ): CypherRow = row match {
    case context: MapCypherRow =>
      context.copyWith(key1, value1, key2, value2, key3, value3)

    case x =>
      throw InternalException.internalError(getClass.getSimpleName, s"Unexpected row type $x")
  }
}
