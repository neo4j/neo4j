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
package org.neo4j.cypher.internal.compatibility.v3_4.runtime.profiler

import org.neo4j.collection.primitive.PrimitiveLongIterator
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.helpers.PrimitiveLongHelper
import org.neo4j.cypher.internal.runtime.interpreted.pipes.{Pipe, PipeDecorator, QueryState}
import org.neo4j.cypher.internal.runtime.interpreted.{DelegatingOperations, DelegatingQueryContext, ExecutionContext}
import org.neo4j.cypher.internal.runtime.planDescription.InternalPlanDescription
import org.neo4j.cypher.internal.runtime.planDescription.InternalPlanDescription.Arguments
import org.neo4j.cypher.internal.runtime.{Operations, QueryContext}
import org.neo4j.cypher.internal.util.v3_4.attribution.Id
import org.neo4j.helpers.MathUtil
import org.neo4j.internal.kernel.api.helpers.RelationshipSelectionCursor
import org.neo4j.kernel.impl.api.RelationshipVisitor
import org.neo4j.kernel.impl.api.store.RelationshipIterator
import org.neo4j.kernel.impl.factory.{DatabaseInfo, Edition}
import org.neo4j.values.virtual.{NodeValue, RelationshipValue}

import scala.collection.mutable

class Profiler(databaseInfo: DatabaseInfo = DatabaseInfo.COMMUNITY) extends PipeDecorator {
  outerProfiler =>

  val pageCacheStats: mutable.Map[Id, (Long, Long)] = mutable.Map.empty
  val dbHitsStats: mutable.Map[Id, ProfilingPipeQueryContext] = mutable.Map.empty
  val rowStats: mutable.Map[Id, ProfilingIterator] = mutable.Map.empty
  private var parentPipe: Option[Pipe] = None


  def decorate(pipe: Pipe, iter: Iterator[ExecutionContext]): Iterator[ExecutionContext] = {
    val oldCount = rowStats.get(pipe.id).map(_.count).getOrElse(0L)
    val resultIter = new ProfilingIterator(iter, oldCount, pipe.id, if (trackPageCacheStats) updatePageCacheStatistics
    else {
      (_) => Unit})

    rowStats(pipe.id) = resultIter
    resultIter
  }

  def decorate(pipe: Pipe, state: QueryState): QueryState = {
    val decoratedContext = dbHitsStats.getOrElseUpdate(pipe.id, state.query match {
      case p: ProfilingPipeQueryContext => new ProfilingPipeQueryContext(p.inner, pipe)
      case _ => new ProfilingPipeQueryContext(state.query, pipe)
    })

    if (trackPageCacheStats) {
      val statisticProvider = decoratedContext.transactionalContext.kernelStatisticProvider
      pageCacheStats(pipe.id) = (statisticProvider.getPageCacheHits, statisticProvider.getPageCacheMisses)
    }
    state.withQueryContext(decoratedContext)
  }

  private def updatePageCacheStatistics(pipeId: Id) = {
    val context = dbHitsStats(pipeId)
    val statisticProvider = context.transactionalContext.kernelStatisticProvider
    val currentStat = pageCacheStats(pipeId)
    pageCacheStats(pipeId) = (statisticProvider.getPageCacheHits - currentStat._1, statisticProvider.getPageCacheMisses - currentStat._2)
  }

  private def trackPageCacheStats = {
    databaseInfo.edition != Edition.community
  }

  def decorate(plan: () => InternalPlanDescription, verifyProfileReady: () => Unit): () => InternalPlanDescription = {
    () => {
      verifyProfileReady()
      plan() map {
        input: InternalPlanDescription =>
          val rows = rowStats.get(input.id).map(_.count).getOrElse(0L)
          val dbHits = dbHitsStats.get(input.id).map(_.count).getOrElse(0L)
          val (hits: Long, misses: Long) = pageCacheStats.getOrElse(input.id, (0L, 0L))
          val hitRatio = MathUtil.portion(hits, misses)

          input
            .addArgument(Arguments.Rows(rows))
            .addArgument(Arguments.DbHits(dbHits))
            .addArgument(Arguments.PageCacheHits(hits))
            .addArgument(Arguments.PageCacheMisses(misses))
            .addArgument(Arguments.PageCacheHitRatio(hitRatio))
      }
    }
  }

  def innerDecorator(owningPipe: Pipe): PipeDecorator = new PipeDecorator {
    innerProfiler =>

    def innerDecorator(pipe: Pipe): PipeDecorator = innerProfiler

    def decorate(pipe: Pipe, state: QueryState): QueryState =
      outerProfiler.decorate(owningPipe, state)

    def decorate(pipe: Pipe, iter: Iterator[ExecutionContext]): Iterator[ExecutionContext] = iter

    def decorate(plan: () => InternalPlanDescription, verifyProfileReady: () => Unit): () => InternalPlanDescription =
      outerProfiler.decorate(plan, verifyProfileReady)
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

  override protected def manyDbHits[A](value: PrimitiveLongIterator): PrimitiveLongIterator = {
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

  class ProfilerOperations[T](inner: Operations[T]) extends DelegatingOperations[T](inner) {
    override protected def singleDbHit[A](value: A): A = self.singleDbHit(value)
    override protected def manyDbHits[A](value: Iterator[A]): Iterator[A] = self.manyDbHits(value)

    override protected def manyDbHits[A](value: PrimitiveLongIterator): PrimitiveLongIterator = self.manyDbHits(value)
  }

  override def nodeOps: Operations[NodeValue] = new ProfilerOperations(inner.nodeOps)
  override def relationshipOps: Operations[RelationshipValue] = new ProfilerOperations(inner.relationshipOps)
}

class ProfilingIterator(inner: Iterator[ExecutionContext], startValue: Long, pipeId: Id,
                        updatePageCacheStatistics: Id => Unit) extends Iterator[ExecutionContext]
  with Counter {

  _count = startValue

  def hasNext: Boolean = {
    val hasNext = inner.hasNext
    if (!hasNext) {
      updatePageCacheStatistics(pipeId)
    }
    hasNext
  }

  def next(): ExecutionContext = {
    increment()
    inner.next()
  }
}
