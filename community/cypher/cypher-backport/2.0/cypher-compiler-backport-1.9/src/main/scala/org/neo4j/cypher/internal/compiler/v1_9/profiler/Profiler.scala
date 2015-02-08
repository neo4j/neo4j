/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v1_9.profiler

import org.neo4j.cypher.internal.compiler.v1_9.pipes.{QueryState, Pipe, PipeDecorator}
import org.neo4j.cypher.internal.compiler.v1_9.ExecutionContext
import org.neo4j.cypher.internal.compiler.v1_9.spi.{DelegatingOperations, Operations, QueryContext, DelegatingQueryContext}
import collection.mutable
import org.neo4j.cypher.ProfilerStatisticsNotReadyException
import org.neo4j.graphdb.{PropertyContainer, Direction, Relationship, Node}
import org.neo4j.cypher.internal.compiler.v1_9.data.PrimVal
import org.neo4j.cypher.internal.compiler.v1_9.executionplan.PlanDescription

class Profiler extends PipeDecorator {

  val contextStats: mutable.Map[Pipe, ProfilingQueryContext] = mutable.Map.empty
  val iterStats: mutable.Map[Pipe, ProfilingIterator] = mutable.Map.empty


  def decorate(pipe: Pipe, iter: Iterator[ExecutionContext]): Iterator[ExecutionContext] = {
    val resultIter = new ProfilingIterator(iter)

    assert(!iterStats.contains(pipe), "Can't profile the same iterator twice")

    iterStats(pipe) = resultIter

    resultIter
  }

  def decorate(pipe: Pipe, state: QueryState): QueryState = {
    assert(!contextStats.contains(pipe), "Can't profile the same pipe twice")

    val decoratedContext = state.query match {
      case p: ProfilingQueryContext => new ProfilingQueryContext(p.inner, pipe)
      case _                        => new ProfilingQueryContext(state.query, pipe)
    }

    contextStats(pipe) = decoratedContext
    state.copy(query = decoratedContext)
  }

  def decorate(plan: PlanDescription, isProfileReady: => Boolean): PlanDescription = plan.mapArgs {
    p: PlanDescription =>
      val iteratorStats: ProfilingIterator = iterStats(p.pipe)

      if ( ! isProfileReady )
        throw new ProfilerStatisticsNotReadyException()

      val newArgs = p.args :+ "_rows" -> PrimVal(iteratorStats.count)

      contextStats.get(p.pipe) match {
        case Some(stats) => newArgs :+ "_db_hits" -> PrimVal(stats.count)
        case None        => newArgs
      }

  }
}

trait Counter {
  private var _count = 0L

  def count = _count

  def increment() {
    _count += 1L
  }
}


class ProfilingQueryContext(val inner: QueryContext, val p: Pipe) extends DelegatingQueryContext(inner) with Counter {

  class ProfilerOperations[T <: PropertyContainer](inner: Operations[T]) extends DelegatingOperations[T](inner) {
    override def delete(obj: T) {
      increment()
      inner.delete(obj)
    }

    override def getById(id: Long): T = {
      increment()
      inner.getById(id)
    }

    override def getProperty(obj: T, propertyKey: String): Any = {
      increment()
      inner.getProperty(obj, propertyKey)
    }

    override def hasProperty(obj: T, propertyKey: String): Boolean = {
      increment()
      inner.hasProperty(obj, propertyKey)
    }

    override def propertyKeys(obj: T): Iterable[String] = {
      increment()
      inner.propertyKeys(obj)
    }

    override def removeProperty(obj: T, propertyKey: String) {
      increment()
      inner.removeProperty(obj, propertyKey)
    }

    override def setProperty(obj: T, propertyKey: String, value: Any) {
      increment()
      inner.setProperty(obj, propertyKey, value)
    }

    override def indexGet(name: String, key: String, value: Any): Iterator[T] = countItems(inner.indexGet(name, key, value))

    override def indexQuery(name: String, query: Any): Iterator[T] = countItems(inner.indexQuery(name, query))

    override def all: Iterator[T] = countItems(inner.all)

    private def countItems(in: Iterator[T]): Iterator[T] = in.map {
      t =>
        increment()
        t
    }

    override def isDeleted(obj: T): Boolean = {
      increment()
      inner.isDeleted(obj)
    }
  }

  override def createNode(): Node = {
    increment()
    inner.createNode()
  }

  override def createRelationship(start: Node, end: Node, relType: String): Relationship = {
    increment()
    inner.createRelationship(start, end, relType)
  }

  override def getRelationshipsFor(node: Node, dir: Direction, types: Seq[String]): Iterable[Relationship] = {
    val result: Iterable[Relationship] = inner.getRelationshipsFor(node, dir, types)

    result.view.map {
      rel =>
        increment()
        rel
    }
  }

  override def nodeOps: Operations[Node] = new ProfilerOperations(inner.nodeOps)

  override def relationshipOps: Operations[Relationship] = new ProfilerOperations(inner.relationshipOps)
}

class ProfilingIterator(inner: Iterator[ExecutionContext]) extends Iterator[ExecutionContext] with Counter {

  def hasNext: Boolean = inner.hasNext

  def next(): ExecutionContext = {
    increment()
    inner.next()
  }
}
