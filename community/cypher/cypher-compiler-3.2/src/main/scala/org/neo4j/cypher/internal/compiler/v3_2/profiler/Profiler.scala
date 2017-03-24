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
package org.neo4j.cypher.internal.compiler.v3_2.profiler

import org.neo4j.cypher.internal.compiler.v3_2._
import org.neo4j.cypher.internal.compiler.v3_2.pipes.{Pipe, PipeDecorator, QueryState}
import org.neo4j.cypher.internal.compiler.v3_2.planDescription.InternalPlanDescription.Arguments
import org.neo4j.cypher.internal.compiler.v3_2.planDescription.{Id, InternalPlanDescription}
import org.neo4j.cypher.internal.compiler.v3_2.spi._
import org.neo4j.cypher.internal.frontend.v3_2.ProfilerStatisticsNotReadyException
import org.neo4j.graphdb.{Node, PropertyContainer, Relationship}

import scala.collection.mutable

class Profiler extends PipeDecorator {
  outerProfiler =>

  val pageCacheStats: mutable.Map[Id, (Long, Long)] = mutable.Map.empty
  val dbHitsStats: mutable.Map[Id, ProfilingPipeQueryContext] = mutable.Map.empty
  val rowStats: mutable.Map[Id, ProfilingIterator] = mutable.Map.empty
  private var parentPipe: Option[Pipe] = None


  def decorate(pipe: Pipe, iter: Iterator[ExecutionContext]): Iterator[ExecutionContext] = {
    val oldCount = rowStats.get(pipe.id).map(_.count).getOrElse(0L)
    val context = dbHitsStats(pipe.id)

    val resultIter = new ProfilingIterator(iter, oldCount, context, pipe.id, pageCacheStats)

    rowStats(pipe.id) = resultIter
    resultIter
  }

  def decorate(pipe: Pipe, state: QueryState): QueryState = {
    val decoratedContext = dbHitsStats.getOrElseUpdate(pipe.id, state.query match {
      case p: ProfilingPipeQueryContext => new ProfilingPipeQueryContext(p.inner, pipe)
      case _ => new ProfilingPipeQueryContext(state.query, pipe)
    })

    val statisticProvider = decoratedContext.kernelStatisticProvider()
    pageCacheStats(pipe.id) = (statisticProvider.getPageCacheHits, statisticProvider.getPageCacheMisses)
    state.withQueryContext(decoratedContext)
  }


  def decorate(plan: InternalPlanDescription, isProfileReady: => Boolean): InternalPlanDescription = {
    if (!isProfileReady)
      throw new ProfilerStatisticsNotReadyException()

    plan map {
      input: InternalPlanDescription =>
        val rows = rowStats.get(input.id).map(_.count).getOrElse(0L)
        val dbHits = dbHitsStats.get(input.id).map(_.count).getOrElse(0L)
        val pageCacheStatistic: (Long, Long) = pageCacheStats.getOrElse(input.id, (0, 0))

        input
          .addArgument(Arguments.Rows(rows))
          .addArgument(Arguments.DbHits(dbHits))
          .addArgument(Arguments.PageCacheHits(pageCacheStatistic._1))
          .addArgument(Arguments.PageCacheMisses(pageCacheStatistic._2))
    }
  }

  def innerDecorator(owningPipe: Pipe): PipeDecorator = new PipeDecorator {
    innerProfiler =>

    def innerDecorator(pipe: Pipe): PipeDecorator = innerProfiler

    def decorate(pipe: Pipe, state: QueryState): QueryState =
      outerProfiler.decorate(owningPipe, state)

    def decorate(pipe: Pipe, iter: Iterator[ExecutionContext]): Iterator[ExecutionContext] = iter

    def decorate(plan: InternalPlanDescription, isProfileReady: => Boolean): InternalPlanDescription =
      outerProfiler.decorate(plan, isProfileReady)
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

  class ProfilerOperations[T <: PropertyContainer](inner: Operations[T]) extends DelegatingOperations[T](inner) {
    override protected def singleDbHit[A](value: A): A = self.singleDbHit(value)
    override protected def manyDbHits[A](value: Iterator[A]): Iterator[A] = self.manyDbHits(value)
  }

  override def nodeOps: Operations[Node] = new ProfilerOperations(inner.nodeOps)
  override def relationshipOps: Operations[Relationship] = new ProfilerOperations(inner.relationshipOps)
}

class ProfilingIterator(inner: Iterator[ExecutionContext], startValue: Long, queryContext: ProfilingPipeQueryContext,
                        pipeId: Id, pageCacheStats:mutable.Map[Id, (Long, Long)]) extends
  Iterator[ExecutionContext] with Counter {

  _count = startValue

  def hasNext: Boolean = {
    val hasNext = inner.hasNext
    if (!hasNext) {
      val statisticProvider = queryContext.kernelStatisticProvider()
      val currentStat = pageCacheStats(pipeId)
      pageCacheStats(pipeId) = (statisticProvider.getPageCacheHits - currentStat._1,
                                statisticProvider.getPageCacheMisses - currentStat._2)
    }
    hasNext
  }

  def next(): ExecutionContext = {
    increment()
    inner.next()
  }
}
