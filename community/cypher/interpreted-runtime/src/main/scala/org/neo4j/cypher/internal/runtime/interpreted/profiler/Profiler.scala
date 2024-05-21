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
package org.neo4j.cypher.internal.runtime.interpreted.profiler

import org.neo4j.common.Edition
import org.neo4j.cypher.internal.profiling.OperatorProfileEvent
import org.neo4j.cypher.internal.runtime.ClosingIterator
import org.neo4j.cypher.internal.runtime.ClosingLongIterator
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.ExpressionCursors
import org.neo4j.cypher.internal.runtime.NodeOperations
import org.neo4j.cypher.internal.runtime.NodeReadOperations
import org.neo4j.cypher.internal.runtime.Operations
import org.neo4j.cypher.internal.runtime.PrimitiveLongHelper
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.ReadOperations
import org.neo4j.cypher.internal.runtime.RelationshipIterator
import org.neo4j.cypher.internal.runtime.RelationshipOperations
import org.neo4j.cypher.internal.runtime.RelationshipReadOperations
import org.neo4j.cypher.internal.runtime.interpreted.DelegatingOperations
import org.neo4j.cypher.internal.runtime.interpreted.DelegatingQueryContext
import org.neo4j.cypher.internal.runtime.interpreted.DelegatingReadOperations
import org.neo4j.cypher.internal.runtime.interpreted.pipes.PipeDecorator
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.cypher.result.OperatorProfile
import org.neo4j.internal.kernel.api.Cursor
import org.neo4j.internal.kernel.api.DefaultCloseListenable
import org.neo4j.internal.kernel.api.KernelReadTracer
import org.neo4j.internal.kernel.api.NodeCursor
import org.neo4j.internal.kernel.api.NodeLabelIndexCursor
import org.neo4j.internal.kernel.api.NodeValueIndexCursor
import org.neo4j.internal.kernel.api.PropertyCursor
import org.neo4j.internal.kernel.api.RelationshipScanCursor
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor
import org.neo4j.internal.kernel.api.RelationshipTypeIndexCursor
import org.neo4j.internal.kernel.api.RelationshipValueIndexCursor
import org.neo4j.kernel.impl.factory.DbmsInfo
import org.neo4j.kernel.impl.query.statistic.StatisticProvider
import org.neo4j.storageengine.api.PropertySelection
import org.neo4j.storageengine.api.Reference
import org.neo4j.storageengine.api.RelationshipVisitor
import org.neo4j.values.storable.Value

import scala.collection.mutable

class Profiler(val dbmsInfo: DbmsInfo, stats: InterpretedProfileInformation) extends PipeDecorator {
  outerProfiler =>

  def withProfileInformation(profileInformation: InterpretedProfileInformation): Profiler =
    new Profiler(dbmsInfo, profileInformation)

  private case class StackEntry(planId: Id, transactionBoundStatisticProvider: StatisticProvider)

  private var planIdStack: List[StackEntry] = Nil

  private val lastObservedStats =
    mutable.Map[StatisticProvider, PageCacheStats]().withDefaultValue(PageCacheStats(0, 0))

  private def startAccountingPageCacheStatsFor(statisticProvider: StatisticProvider, planId: Id): Unit = {
    // The current top of the stack hands over control to the plan with the provided planId.
    // Account any statistic updates that happened until now towards the previousId.
    planIdStack.headOption.foreach {
      case StackEntry(previousId, previousStatisticProvider) =>
        val currentStatsOfPreviousPlan =
          PageCacheStats(previousStatisticProvider.getPageCacheHits, previousStatisticProvider.getPageCacheMisses)
        stats.pageCacheMap(previousId) += (currentStatsOfPreviousPlan - lastObservedStats(previousStatisticProvider))
    }

    val currentStats = PageCacheStats(statisticProvider.getPageCacheHits, statisticProvider.getPageCacheMisses)

    planIdStack ::= StackEntry(planId, statisticProvider)
    lastObservedStats.update(statisticProvider, currentStats)
  }

  private def stopAccountingPageCacheStatsFor(statisticProvider: StatisticProvider, planId: Id): Unit = {
    val head :: rest = planIdStack
    require(
      head.planId == planId,
      s"We messed up accounting the page cache statistics. Expected to pop $planId but popped ${head.planId}. Remaining stack: $planIdStack"
    )

    val currentStats = PageCacheStats(statisticProvider.getPageCacheHits, statisticProvider.getPageCacheMisses)
    stats.pageCacheMap(planId) += (currentStats - lastObservedStats(statisticProvider))

    planIdStack = rest
    // To not leave any memory leak where we reference a transactionBoundStatisticProvider of an already committed
    // transaction, we have to clean up transactionBoundStatisticProvider whenever control goes back to another transaction.
    if (rest.isEmpty || rest.head.transactionBoundStatisticProvider != head.transactionBoundStatisticProvider) {
      lastObservedStats.remove(head.transactionBoundStatisticProvider)
    }
    lastObservedStats.update(statisticProvider, currentStats)
  }

  override def decorate(planId: Id, state: QueryState, iter: ClosingIterator[CypherRow]): ClosingIterator[CypherRow] = {
    val oldCount = stats.rowMap.get(planId).map(_.count).getOrElse(0L)

    val transactionalContext = state.query.transactionalContext

    val resultIter =
      new ProfilingIterator(
        iter,
        oldCount,
        if (trackPageCacheStats)
          () => startAccountingPageCacheStatsFor(transactionalContext.kernelStatisticProvider, planId)
        else () => (),
        if (trackPageCacheStats)
          () => stopAccountingPageCacheStatsFor(transactionalContext.kernelStatisticProvider, planId)
        else () => ()
      )

    stats.rowMap(planId) = resultIter
    resultIter
  }

  override def decorate(planId: Id, state: QueryState): QueryState = {
    stats.setQueryMemoryTracker(state.queryMemoryTracker)
    val counter = stats.dbHitsMap.getOrElseUpdate(planId, Counter())
    val decoratedContext = state.query match {
      case p: ProfilingPipeQueryContext => new ProfilingPipeQueryContext(p.inner, counter)
      case _                            => new ProfilingPipeQueryContext(state.query, counter)
    }

    if (trackPageCacheStats) {
      startAccountingPageCacheStatsFor(decoratedContext.transactionalContext.kernelStatisticProvider, planId)
    }
    state.withQueryContext(decoratedContext)
  }

  override def afterCreateResults(planId: Id, state: QueryState): Unit = {
    if (trackPageCacheStats) {
      stopAccountingPageCacheStatsFor(state.query.transactionalContext.kernelStatisticProvider, planId)
    }
  }

  private def trackPageCacheStats = {
    dbmsInfo.edition != Edition.COMMUNITY
  }

  override def innerDecorator(outerPlanId: Id): PipeDecorator = new PipeDecorator {
    innerProfiler =>
    override def innerDecorator(planId: Id): PipeDecorator = innerProfiler

    override def decorate(planId: Id, state: QueryState): QueryState =
      outerProfiler.decorate(outerPlanId, state)

    override def decorate(planId: Id, state: QueryState, iter: ClosingIterator[CypherRow]): ClosingIterator[CypherRow] =
      iter

    override def afterCreateResults(planId: Id, state: QueryState): Unit =
      outerProfiler.afterCreateResults(outerPlanId, state)
  }
}

trait Counter {
  protected var _count = 0L
  def count: Long = _count

  def increment(): Unit = {
    if (count != OperatorProfile.NO_DATA)
      _count += 1L
  }

  def increment(hits: Long): Unit = {
    if (count != OperatorProfile.NO_DATA)
      _count += hits
  }

  def invalidate(): Unit = {
    _count = OperatorProfile.NO_DATA
  }
}

object Counter {

  class DefaultCounter extends Counter

  def apply(): Counter = {
    new DefaultCounter
  }
}

final class ProfilingPipeQueryContext(inner: QueryContext, counter: Counter)
    extends DelegatingQueryContext(inner) {
  self =>

  def count: Long = counter.count

  override protected def singleDbHit[A](value: A): A = {
    counter.increment()
    value
  }

  override protected def unknownDbHits[A](value: A): A = {
    counter.invalidate()
    value
  }

  override protected def manyDbHits[A](value: ClosingIterator[A]): ClosingIterator[A] = {
    counter.increment()
    value.map {
      v =>
        counter.increment()
        v
    }
  }

  override protected def manyDbHits(value: ClosingLongIterator): ClosingLongIterator = {
    counter.increment()
    PrimitiveLongHelper.mapPrimitive(
      value,
      { x =>
        counter.increment()
        x
      }
    )
  }

  override protected def manyDbHits(inner: RelationshipIterator): RelationshipIterator = new RelationshipIterator {
    counter.increment()

    override def relationshipVisit[EXCEPTION <: Exception](
      relationshipId: Long,
      visitor: RelationshipVisitor[EXCEPTION]
    ): Boolean =
      inner.relationshipVisit(relationshipId, visitor)

    override def next(): Long = {
      counter.increment()
      inner.next()
    }

    override def hasNext: Boolean = inner.hasNext

    override def startNodeId(): Long = inner.startNodeId()

    override def endNodeId(): Long = inner.endNodeId()

    override def typeId(): Int = inner.typeId()
  }

  override protected def manyDbHitsCliRi(inner: ClosingLongIterator with RelationshipIterator)
    : ClosingLongIterator with RelationshipIterator =
    new ClosingLongIterator with RelationshipIterator {
      counter.increment()

      override def relationshipVisit[EXCEPTION <: Exception](
        relationshipId: Long,
        visitor: RelationshipVisitor[EXCEPTION]
      ): Boolean =
        inner.relationshipVisit(relationshipId, visitor)

      override def next(): Long = {
        counter.increment()
        inner.next()
      }

      override def startNodeId(): Long = inner.startNodeId()

      override def endNodeId(): Long = inner.endNodeId()

      override def typeId(): Int = inner.typeId()

      override def close(): Unit = inner.close()

      override protected[this] def innerHasNext: Boolean = inner.hasNext
    }

  override protected def manyDbHits(nodeCursor: NodeCursor): NodeCursor = {

    val tracer = new PipeTracer
    nodeCursor.setTracer(tracer)
    nodeCursor
  }

  override protected def manyDbHits(nodeCursor: NodeLabelIndexCursor): NodeLabelIndexCursor = {
    val tracer = new PipeTracer
    nodeCursor.setTracer(tracer)
    nodeCursor
  }

  override protected def manyDbHits(relCursor: RelationshipTypeIndexCursor): RelationshipTypeIndexCursor = {
    val tracer = new PipeTracer
    relCursor.setTracer(tracer)
    relCursor
  }

  override protected def manyDbHits(propertyCursor: PropertyCursor): PropertyCursor = {
    val tracer = new PipeTracer
    propertyCursor.setTracer(tracer)
    propertyCursor
  }

  override protected def manyDbHits(relationshipScanCursor: RelationshipScanCursor): RelationshipScanCursor = {
    val tracer = new PipeTracer
    relationshipScanCursor.setTracer(tracer)
    relationshipScanCursor
  }

  override protected def manyDbHits(inner: NodeValueIndexCursor): NodeValueIndexCursor =
    new ProfilingCursor(inner) with NodeValueIndexCursor {

      override def numberOfProperties(): Int = inner.numberOfProperties()

      override def hasValue: Boolean = inner.hasValue

      override def propertyValue(offset: Int): Value = inner.propertyValue(offset)

      override def node(cursor: NodeCursor): Unit = inner.node(cursor)

      override def nodeReference(): Long = inner.nodeReference()

      override def score(): Float = inner.score()
    }

  override protected def manyDbHits(inner: RelationshipValueIndexCursor): RelationshipValueIndexCursor =
    new ProfilingCursor(inner) with RelationshipValueIndexCursor {
      override def numberOfProperties(): Int = inner.numberOfProperties()
      override def hasValue: Boolean = inner.hasValue
      override def propertyValue(offset: Int): Value = inner.propertyValue(offset)
      override def source(cursor: NodeCursor): Unit = inner.source(cursor)
      override def target(cursor: NodeCursor): Unit = inner.target(cursor)
      override def `type`(): Int = inner.`type`()
      override def sourceNodeReference(): Long = inner.sourceNodeReference()
      override def targetNodeReference(): Long = inner.targetNodeReference()
      override def relationshipReference(): Long = inner.relationshipReference()
      override def score(): Float = inner.score()

      override def properties(cursor: PropertyCursor, selection: PropertySelection): Unit =
        inner.properties(cursor, selection)
      override def propertiesReference(): Reference = inner.propertiesReference()
      override def readFromStore(): Boolean = inner.readFromStore()
    }

  override protected def manyDbHits(inner: RelationshipTraversalCursor): RelationshipTraversalCursor = {
    val tracer = new PipeTracer
    inner.setTracer(tracer)
    inner
  }

  override protected def manyDbHits(count: Int): Int = {
    counter.increment(count)
    count
  }

  override def createExpressionCursors(): ExpressionCursors = {
    val expressionCursors = super.createExpressionCursors()
    val tracer = new PipeTracer
    expressionCursors.setKernelTracer(tracer)
    expressionCursors
  }

  abstract class ProfilingCursor(inner: Cursor) extends DefaultCloseListenable with Cursor {

    override def next(): Boolean = {
      counter.increment()
      inner.next()
    }

    override def closeInternal(): Unit = inner.close()

    override def isClosed: Boolean = inner.isClosed

    override def setTracer(tracer: KernelReadTracer): Unit = inner.setTracer(tracer)

    override def removeTracer(): Unit = inner.removeTracer()
  }

  class PipeTracer() extends OperatorProfileEvent {

    override def dbHit(): Unit = {
      counter.increment()
    }

    override def dbHits(hits: Long): Unit = {
      counter.increment(hits)
    }

    override def row(): Unit = {}

    override def row(hasRow: Boolean): Unit = {}

    override def rows(n: Long): Unit = {}

    override def close(): Unit = {}
  }

  class ProfilerReadOperations[T, CURSOR](inner: ReadOperations[T, CURSOR])
      extends DelegatingReadOperations[T, CURSOR](inner) {
    override protected def singleDbHit[A](value: A): A = self.singleDbHit(value)
    override protected def manyDbHits[A](value: ClosingIterator[A]): ClosingIterator[A] = self.manyDbHits(value)
    override protected def manyDbHits[A](value: ClosingLongIterator): ClosingLongIterator = self.manyDbHits(value)
  }

  class ProfilerOperations[T, CURSOR](inner: Operations[T, CURSOR]) extends DelegatingOperations[T, CURSOR](inner) {
    override protected def singleDbHit[A](value: A): A = self.singleDbHit(value)
    override protected def manyDbHits[A](value: ClosingIterator[A]): ClosingIterator[A] = self.manyDbHits(value)
    override protected def manyDbHits[A](value: ClosingLongIterator): ClosingLongIterator = self.manyDbHits(value)
  }

  override val nodeReadOps: NodeReadOperations = new ProfilerReadOperations(inner.nodeReadOps) with NodeReadOperations

  override val relationshipReadOps: RelationshipReadOperations =
    new ProfilerReadOperations(inner.relationshipReadOps) with RelationshipReadOperations
  override val nodeWriteOps: NodeOperations = new ProfilerOperations(inner.nodeWriteOps) with NodeOperations

  override val relationshipWriteOps: RelationshipOperations =
    new ProfilerOperations(inner.relationshipWriteOps) with RelationshipOperations
}

class ProfilingIterator(
  inner: ClosingIterator[CypherRow],
  startValue: Long,
  startAccounting: () => Unit,
  stopAccounting: () => Unit
) extends ClosingIterator[CypherRow]
    with Counter {

  _count = startValue

  override protected[this] def closeMore(): Unit = inner.close()

  def innerHasNext: Boolean = {
    startAccounting()
    val hasNext = inner.hasNext
    stopAccounting()
    hasNext
  }

  def next(): CypherRow = {
    increment()
    startAccounting()
    val result = inner.next()
    stopAccounting()
    result
  }
}

object ProfilingIterator {
  def empty: ProfilingIterator = new ProfilingIterator(ClosingIterator.empty, 0L, () => (), () => ())
}
