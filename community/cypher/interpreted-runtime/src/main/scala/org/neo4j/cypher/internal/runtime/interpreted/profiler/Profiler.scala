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
package org.neo4j.cypher.internal.runtime.interpreted.profiler

import org.eclipse.collections.api.iterator.LongIterator
import org.neo4j.common.Edition
import org.neo4j.cypher.internal.profiling.KernelStatisticProvider
import org.neo4j.cypher.internal.runtime._
import org.neo4j.cypher.internal.runtime.interpreted.pipes.{Pipe, PipeDecorator, QueryState}
import org.neo4j.cypher.internal.runtime.interpreted.{DelegatingOperations, DelegatingQueryContext}
import org.neo4j.cypher.internal.v4_0.util.AssertionRunner
import org.neo4j.cypher.internal.v4_0.util.attribution.Id
import org.neo4j.cypher.result.OperatorProfile
import org.neo4j.internal.kernel.api.{QueryContext => _, _}
import org.neo4j.kernel.impl.factory.DatabaseInfo
import org.neo4j.storageengine.api.RelationshipVisitor
import org.neo4j.values.storable.Value

class Profiler(databaseInfo: DatabaseInfo,
               stats: InterpretedProfileInformation) extends PipeDecorator {
  outerProfiler =>

  private var parentPipe: Option[Pipe] = None
  private var planIdStack: List[Id] = Nil
  private var lastObservedStats = PageCacheStats(0, 0)

  private def startAccountingPageCacheStatsFor(statisticProvider: KernelStatisticProvider, id: Id): Unit = {
    val currentStats = PageCacheStats(statisticProvider.getPageCacheHits, statisticProvider.getPageCacheMisses)

    planIdStack.headOption.foreach {
      previousId =>
        stats.pageCacheMap(previousId) += (currentStats - lastObservedStats)
    }

    planIdStack ::= id
    lastObservedStats = currentStats
  }

  private def stopAccountingPageCacheStatsFor(statisticProvider: KernelStatisticProvider, id: Id): Unit = {
    val head :: rest = planIdStack
    if (AssertionRunner.isAssertionsEnabled) {
      if (head != id) {
        throw new IllegalStateException(s"We messed up accounting the page cache statistics. Expected to pop $id but popped $head. Remaining stack: $planIdStack")
      }
    }

    val currentStats = PageCacheStats(statisticProvider.getPageCacheHits, statisticProvider.getPageCacheMisses)
    stats.pageCacheMap(id) += (currentStats - lastObservedStats)

    planIdStack = rest
    lastObservedStats = currentStats
  }

  private def updatePageCacheStatistics(pipeId: Id, f: (KernelStatisticProvider, Id) => Unit): Unit = {
    val context = stats.dbHitsMap(pipeId)
    val statisticProvider = context.transactionalContext.kernelStatisticProvider
    f(statisticProvider, pipeId)
  }


  def decorate(pipe: Pipe, iter: Iterator[ExecutionContext]): Iterator[ExecutionContext] = {
    val oldCount = stats.rowMap.get(pipe.id).map(_.count).getOrElse(0L)

    val resultIter =
      new ProfilingIterator(
        iter,
        oldCount,
        pipe.id,
        if (trackPageCacheStats) updatePageCacheStatistics(_, startAccountingPageCacheStatsFor) else _ => (),
        if (trackPageCacheStats) updatePageCacheStatistics(_, stopAccountingPageCacheStatsFor) else _ => ())

    stats.rowMap(pipe.id) = resultIter
    resultIter
  }

  def decorate(pipe: Pipe, state: QueryState): QueryState = {
    val decoratedContext = stats.dbHitsMap.getOrElseUpdate(pipe.id, state.query match {
      case p: ProfilingPipeQueryContext => new ProfilingPipeQueryContext(p.inner, pipe)
      case _ => new ProfilingPipeQueryContext(state.query, pipe)
    })

    if (trackPageCacheStats) {
      startAccountingPageCacheStatsFor(decoratedContext.transactionalContext.kernelStatisticProvider, pipe.id)
    }
    state.withQueryContext(decoratedContext)
  }

  override def afterCreateResults(pipe: Pipe, state: QueryState): Unit = {
    if (trackPageCacheStats) {
      stopAccountingPageCacheStatsFor(state.query.transactionalContext.kernelStatisticProvider, pipe.id)
    }
  }

  private def trackPageCacheStats = {
    databaseInfo.edition != Edition.COMMUNITY
  }

  def innerDecorator(owningPipe: Pipe): PipeDecorator = new PipeDecorator {
    innerProfiler =>

    def innerDecorator(pipe: Pipe): PipeDecorator = innerProfiler

    def decorate(pipe: Pipe, state: QueryState): QueryState =
      outerProfiler.decorate(owningPipe, state)

    def decorate(pipe: Pipe, iter: Iterator[ExecutionContext]): Iterator[ExecutionContext] = iter

    override def afterCreateResults(pipe: Pipe, state: QueryState): Unit = outerProfiler.afterCreateResults(owningPipe, state)
  }

  def registerParentPipe(pipe: Pipe): Unit =
    parentPipe = Some(pipe)
}

trait Counter {
  protected var _count = 0L
  def count: Long = _count

  def increment(): Unit = {
    if (count != OperatorProfile.NO_DATA)
      _count += 1L
  }

  def invalidate(): Unit = {
    _count = OperatorProfile.NO_DATA
  }
}

final class ProfilingPipeQueryContext(inner: QueryContext, val p: Pipe)
  extends DelegatingQueryContext(inner) with Counter {
  self =>

  override protected def singleDbHit[A](value: A): A = {
    increment()
    value
  }

  override protected def unknownDbHits[A](value: A): A = {
    invalidate()
    value
  }

  override protected def manyDbHits[A](value: Iterator[A]): Iterator[A] = {
    increment()
    value.map {
      v =>
        increment()
        v
    }
  }

  override protected def manyDbHits(value: LongIterator): LongIterator = {
    increment()
    PrimitiveLongHelper.mapPrimitive(value, { x =>
      increment()
      x
    })
  }

  override protected def manyDbHits(inner: RelationshipIterator): RelationshipIterator = new RelationshipIterator {
    increment()
    override def relationshipVisit[EXCEPTION <: Exception](relationshipId: Long, visitor: RelationshipVisitor[EXCEPTION]): Boolean =
      inner.relationshipVisit(relationshipId, visitor)

    override def next(): Long = {
      increment()
      inner.next()
    }

    override def hasNext: Boolean = inner.hasNext
  }

  override protected def manyDbHits(inner: NodeValueIndexCursor): NodeValueIndexCursor = new NodeValueIndexCursor {

    override def numberOfProperties(): Int = inner.numberOfProperties()

    override def propertyKey(offset: Int): Int = inner.propertyKey(offset)

    override def hasValue: Boolean = inner.hasValue

    override def propertyValue(offset: Int): Value = inner.propertyValue(offset)

    override def node(cursor: NodeCursor): Unit = inner.node(cursor)

    override def nodeReference(): Long = inner.nodeReference()

    override def next(): Boolean = {
      increment()
      inner.next()
    }

    override def close(): Unit = {
      closeInternal()
      // We do not call getCloseListener.onClosed(inner) here since
      // that will already happen in closeInternal.
    }

    override def closeInternal(): Unit = inner.close()

    override def isClosed: Boolean = inner.isClosed

    override def score(): Float = inner.score()

    override def setTracer(tracer: KernelReadTracer): Unit = inner.setTracer(tracer)

    override def setCloseListener(closeListener: CloseListener): Unit = inner.setCloseListener(closeListener)

    override def removeTracer(): Unit = inner.removeTracer()

    override def getCloseListener: CloseListener = inner.getCloseListener

    override def setToken(token: Int): Unit = inner.setToken(token)

    override def getToken: Int = inner.getToken
  }

  class ProfilerOperations[T, CURSOR](inner: Operations[T, CURSOR]) extends DelegatingOperations[T, CURSOR](inner) {
    override protected def singleDbHit[A](value: A): A = self.singleDbHit(value)
    override protected def manyDbHits[A](value: Iterator[A]): Iterator[A] = self.manyDbHits(value)

    override protected def manyDbHits[A](value: LongIterator): LongIterator = self.manyDbHits(value)
  }

  override val nodeOps: NodeOperations = new ProfilerOperations(inner.nodeOps) with NodeOperations
  override val relationshipOps: RelationshipOperations = new ProfilerOperations(inner.relationshipOps) with RelationshipOperations
}

class ProfilingIterator(inner: Iterator[ExecutionContext],
                        startValue: Long,
                        pipeId: Id,
                        startAccouting: Id => Unit,
                        stopAccounting: Id => Unit) extends Iterator[ExecutionContext]
  with Counter {

  _count = startValue

  def hasNext: Boolean = {
    startAccouting(pipeId)
    val hasNext = inner.hasNext
    stopAccounting(pipeId)
    hasNext
  }

  def next(): ExecutionContext = {
    increment()
    startAccouting(pipeId)
    val result = inner.next()
    stopAccounting(pipeId)
    result
  }
}
