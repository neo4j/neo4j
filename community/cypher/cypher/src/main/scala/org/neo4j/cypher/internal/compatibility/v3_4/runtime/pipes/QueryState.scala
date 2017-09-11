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
package org.neo4j.cypher.internal.compatibility.v3_4.runtime.pipes

import java.util.UUID

import org.neo4j.collection.primitive.PrimitiveLongSet
import org.neo4j.cypher.internal.QueryStatistics
import org.neo4j.cypher.internal.apa.v3_4.ParameterNotFoundException
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.ExecutionContext
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.commands.expressions.PathValueBuilder
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.commands.predicates.{InCheckContainer, SingleThreadedLRUCache}
import org.neo4j.cypher.internal.spi.v3_4.QueryContext
import org.neo4j.values.AnyValue
import org.neo4j.values.virtual.MapValue

import scala.collection.mutable

class QueryState(val query: QueryContext,
                 val resources: ExternalCSVResource,
                 val params: MapValue,
                 val decorator: PipeDecorator = NullPipeDecorator,
                 val timeReader: TimeReader = new TimeReader,
                 var initialContext: Option[ExecutionContext] = None,
                 val queryId: AnyRef = UUID.randomUUID().toString,
                 val triadicState: mutable.Map[String, PrimitiveLongSet] = mutable.Map.empty,
                 val repeatableReads: mutable.Map[Pipe, Seq[ExecutionContext]] = mutable.Map.empty,
                 val cachedIn: SingleThreadedLRUCache[Any, InCheckContainer] =
                   new SingleThreadedLRUCache(maxSize = 16)) {
  private var _pathValueBuilder: PathValueBuilder = _

  def createOrGetInitialContext(): ExecutionContext = initialContext.getOrElse(ExecutionContext.empty)

  def clearPathValueBuilder: PathValueBuilder = {
    if (_pathValueBuilder == null) {
      _pathValueBuilder = new PathValueBuilder()
    }
    _pathValueBuilder.clear()
  }

  def readTimeStamp(): Long = timeReader.getTime

  def  getParam(key: String): AnyValue = {
    if (!params.containsKey(key)) throw new ParameterNotFoundException("Expected a parameter named " + key)
    params.get(key)
  }

  def getStatistics: QueryStatistics = query.getOptStatistics.getOrElse(QueryState.defaultStatistics)

  def withDecorator(decorator: PipeDecorator) =
    new QueryState(query, resources, params, decorator, timeReader, initialContext, queryId, triadicState, repeatableReads, cachedIn)

  def withInitialContext(initialContext: ExecutionContext) =
    new QueryState(query, resources, params, decorator, timeReader, Some(initialContext), queryId, triadicState, repeatableReads, cachedIn)

  /**
    * When running on the RHS of an Apply, this method will fill an execution context with argument data
    * @param ctx ExecutionContext to fill with data
    */
  def copyArgumentStateTo(ctx: ExecutionContext, nLongs: Int, nRefs: Int): Unit = initialContext.foreach(initData => ctx.copyFrom(initData, nLongs, nRefs))

  def copyArgumentStateTo(ctx: ExecutionContext): Unit = initialContext.foreach(initData => initData.copyTo(ctx))

  def withQueryContext(query: QueryContext) =
    new QueryState(query, resources, params, decorator, timeReader, initialContext, queryId, triadicState, repeatableReads, cachedIn)
}

object QueryState {
  val defaultStatistics = QueryStatistics()
}

class TimeReader {
  lazy val getTime: Long = System.currentTimeMillis()
}
