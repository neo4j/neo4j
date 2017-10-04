/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.ExecutionContext
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.helpers.PrimitiveLongHelper
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.pipes.{Pipe, PipeDecorator, QueryState}
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.planDescription.InternalPlanDescription
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.planDescription.InternalPlanDescription.Arguments
import org.neo4j.cypher.internal.spi.v3_4.{DelegatingOperations, DelegatingQueryContext, Operations, QueryContext}
import org.neo4j.cypher.internal.v3_4.logical.plans.LogicalPlanId
import org.neo4j.graphdb.{Node, PropertyContainer, Relationship}
import org.neo4j.helpers.MathUtil
import org.neo4j.kernel.impl.api.RelationshipVisitor
import org.neo4j.kernel.impl.api.store.RelationshipIterator
import org.neo4j.kernel.impl.factory.{DatabaseInfo, Edition}

import scala.collection.mutable

class Profiler(databaseInfo: DatabaseInfo = DatabaseInfo.COMMUNITY) extends PipeDecorator {
  outerProfiler =>

  val pageCacheStats: mutable.Map[LogicalPlanId, (Long, Long)] = mutable.Map.empty
  val dbHitsStats: mutable.Map[LogicalPlanId, ProfilingPipeQueryContext] = mutable.Map.empty
  val rowStats: mutable.Map[LogicalPlanId, ProfilingIterator] = mutable.Map.empty
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

  private def updatePageCacheStatistics(pipeId: LogicalPlanId) = {
    val context = dbHitsStats(pipeId)
    val statisticProvider = context.transactionalContext.kernelStatisticProvider
    val currentStat = pageCacheStats(pipeId)
    pageCacheStats(pipeId) = (statisticProvider.getPageCacheHits - currentStat._1, statisticProvider.getPageCacheMisses - currentStat._2)
  }

  private def trackPageCacheStats = {
    databaseInfo.edition != Edition.community
  }

  def decorate(plan: InternalPlanDescription, verifyProfileReady: () => Unit): InternalPlanDescription = {
    verifyProfileReady()

    plan map {
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

  def innerDecorator(owningPipe: Pipe): PipeDecorator = new PipeDecorator {
    innerProfiler =>

    def innerDecorator(pipe: Pipe): PipeDecorator = innerProfiler

    def decorate(pipe: Pipe, state: QueryState): QueryState =
      outerProfiler.decorate(owningPipe, state)

    def decorate(pipe: Pipe, iter: Iterator[ExecutionContext]): Iterator[ExecutionContext] = iter

    def decorate(plan: InternalPlanDescription, verifyProfileReady: () => Unit): InternalPlanDescription =
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

  class ProfilerOperations[T <: PropertyContainer](inner: Operations[T]) extends DelegatingOperations[T](inner) {
    override protected def singleDbHit[A](value: A): A = self.singleDbHit(value)
    override protected def manyDbHits[A](value: Iterator[A]): Iterator[A] = self.manyDbHits(value)

    override protected def manyDbHits[A](value: PrimitiveLongIterator): PrimitiveLongIterator = self.manyDbHits(value)
  }

  override def nodeOps: Operations[Node] = new ProfilerOperations(inner.nodeOps)
  override def relationshipOps: Operations[Relationship] = new ProfilerOperations(inner.relationshipOps)
}

class ProfilingIterator(inner: Iterator[ExecutionContext], startValue: Long, pipeId: LogicalPlanId,
                        updatePageCacheStatistics: LogicalPlanId => Unit) extends Iterator[ExecutionContext]
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
