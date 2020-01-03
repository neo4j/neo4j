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
package org.neo4j.cypher.internal.compatibility.v3_5.runtime.profiler

import org.eclipse.collections.api.iterator.LongIterator
import org.neo4j.cypher.internal.compatibility.v3_5.runtime.helpers.PrimitiveLongHelper
import org.neo4j.cypher.internal.runtime.interpreted.pipes.{Pipe, PipeDecorator, QueryState}
import org.neo4j.cypher.internal.runtime.interpreted.{DelegatingOperations, DelegatingQueryContext, ExecutionContext}
import org.neo4j.cypher.internal.runtime.{Operations, QueryContext}
import org.neo4j.internal.kernel.api.{NodeCursor, NodeValueIndexCursor}
import org.neo4j.internal.kernel.api.helpers.RelationshipSelectionCursor
import org.neo4j.kernel.impl.api.store.RelationshipIterator
import org.neo4j.kernel.impl.factory.{DatabaseInfo, Edition}
import org.neo4j.storageengine.api.RelationshipVisitor
import org.neo4j.values.storable.Value
import org.neo4j.values.virtual.{NodeValue, RelationshipValue}
import org.neo4j.cypher.internal.v3_5.util.attribution.Id

class Profiler(databaseInfo: DatabaseInfo,
               stats: InterpretedProfileInformation) extends PipeDecorator {
  outerProfiler =>

  private var parentPipe: Option[Pipe] = None

  def decorate(pipe: Pipe, iter: Iterator[ExecutionContext]): Iterator[ExecutionContext] = {
    val oldCount = stats.rowMap.get(pipe.id).map(_.count).getOrElse(0L)
    val resultIter =
      new ProfilingIterator(iter, oldCount, pipe.id,
        if (trackPageCacheStats) updatePageCacheStatistics
        else _ => Unit
      )

    stats.rowMap(pipe.id) = resultIter
    resultIter
  }

  def decorate(pipe: Pipe, state: QueryState): QueryState = {
    val decoratedContext = stats.dbHitsMap.getOrElseUpdate(pipe.id, state.query match {
      case p: ProfilingPipeQueryContext => new ProfilingPipeQueryContext(p.inner, pipe)
      case _ => new ProfilingPipeQueryContext(state.query, pipe)
    })

    if (trackPageCacheStats) {
      val statisticProvider = decoratedContext.transactionalContext.kernelStatisticProvider
      stats.pageCacheMap(pipe.id) = PageCacheStats(statisticProvider.getPageCacheHits, statisticProvider.getPageCacheMisses)
    }
    state.withQueryContext(decoratedContext)
  }

  private def updatePageCacheStatistics(pipeId: Id): Unit = {
    val context = stats.dbHitsMap(pipeId)
    val statisticProvider = context.transactionalContext.kernelStatisticProvider
    val currentStat = stats.pageCacheMap(pipeId)
    stats.pageCacheMap(pipeId) =
      PageCacheStats(
        statisticProvider.getPageCacheHits - currentStat.hits,
        statisticProvider.getPageCacheMisses - currentStat.misses
      )
  }

  private def trackPageCacheStats = {
    databaseInfo.edition != Edition.community
  }

  def innerDecorator(owningPipe: Pipe): PipeDecorator = new PipeDecorator {
    innerProfiler =>

    def innerDecorator(pipe: Pipe): PipeDecorator = innerProfiler

    def decorate(pipe: Pipe, state: QueryState): QueryState =
      outerProfiler.decorate(owningPipe, state)

    def decorate(pipe: Pipe, iter: Iterator[ExecutionContext]): Iterator[ExecutionContext] = iter
  }

  def registerParentPipe(pipe: Pipe): Unit =
    parentPipe = Some(pipe)
}

trait Counter {
  protected var _count = 0L
  def count: Long = _count

  def increment() {
    _count += 1L
  }
}

final class ProfilingPipeQueryContext(inner: QueryContext, val p: Pipe)
  extends DelegatingQueryContext(inner) with Counter {
  self =>

  override def createNewQueryContext() = new ProfilingPipeQueryContext(inner.createNewQueryContext(), p)

  override protected def singleDbHit[A](value: A): A = {
    increment()
    value
  }

  override protected def manyDbHits[A](value: Iterator[A]): Iterator[A] = {
    increment()
    value.map {
      (v) =>
        increment()
        v
    }
  }

  override protected def manyDbHits[A](value: LongIterator): LongIterator = {
    increment()
    PrimitiveLongHelper.mapPrimitive(value, { x =>
      increment()
      x
    })
  }

  override protected def manyDbHits[A](inner: RelationshipIterator): RelationshipIterator = new RelationshipIterator {
    increment()
    override def relationshipVisit[EXCEPTION <: Exception](relationshipId: Long, visitor: RelationshipVisitor[EXCEPTION]): Boolean =
      inner.relationshipVisit(relationshipId, visitor)

    override def next(): Long = {
      increment()
      inner.next()
    }

    override def hasNext: Boolean = inner.hasNext
  }

  override protected def manyDbHits[A](inner: RelationshipSelectionCursor): RelationshipSelectionCursor = new RelationshipSelectionCursor {
    override def next(): Boolean = {
      increment()
      inner.next()
    }

    override def close(): Unit = inner.close()

    override def relationshipReference(): Long = inner.relationshipReference()

    override def `type`(): Int = inner.`type`()

    override def otherNodeReference(): Long = inner.otherNodeReference()

    override def sourceNodeReference(): Long = inner.sourceNodeReference()

    override def targetNodeReference(): Long = inner.targetNodeReference()

    override def propertiesReference(): Long = inner.propertiesReference()
  }

  override protected def manyDbHits[A](inner: NodeValueIndexCursor): NodeValueIndexCursor = new NodeValueIndexCursor {

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

    override def close(): Unit = inner.close()

    override def isClosed: Boolean = inner.isClosed
  }

  class ProfilerOperations[T](inner: Operations[T]) extends DelegatingOperations[T](inner) {
    override protected def singleDbHit[A](value: A): A = self.singleDbHit(value)
    override protected def manyDbHits[A](value: Iterator[A]): Iterator[A] = self.manyDbHits(value)

    override protected def manyDbHits[A](value: LongIterator): LongIterator = self.manyDbHits(value)
  }

  override def nodeOps: Operations[NodeValue] = new ProfilerOperations(inner.nodeOps)
  override def relationshipOps: Operations[RelationshipValue] = new ProfilerOperations(inner.relationshipOps)
}

class ProfilingIterator(inner: Iterator[ExecutionContext], startValue: Long, pipeId: Id,
                        updatePageCacheStatistics: Id => Unit) extends Iterator[ExecutionContext]
  with Counter {

  _count = startValue
  private var updatedStatistics = false

  def hasNext: Boolean = {
    val hasNext = inner.hasNext
    if (!hasNext && !updatedStatistics) {
      updatePageCacheStatistics(pipeId)
      updatedStatistics = true
    }
    hasNext
  }

  def next(): ExecutionContext = {
    increment()
    inner.next()
  }
}
