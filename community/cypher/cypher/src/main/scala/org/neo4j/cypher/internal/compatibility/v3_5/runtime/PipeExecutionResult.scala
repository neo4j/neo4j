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
package org.neo4j.cypher.internal.compatibility.v3_5.runtime

import java.util

import org.neo4j.cypher.internal.runtime._
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.result.QueryResult.QueryResultVisitor
import org.neo4j.cypher.result.RuntimeResult.ConsumptionState
import org.neo4j.cypher.result.{QueryProfile, RuntimeResult}
import org.neo4j.graphdb.ResourceIterator
import org.neo4j.values.AnyValue

class PipeExecutionResult(val result: IteratorBasedResult,
                          val fieldNames: Array[String],
                          val state: QueryState,
                          override val queryProfile: QueryProfile)
  extends RuntimeResult {

  self =>

  private val query = state.query
  private var resultRequested = false

  val javaValues = new RuntimeJavaValueConverter(isGraphKernelResultValue)
  def isIterable: Boolean = true

  def asIterator: ResourceIterator[java.util.Map[String, AnyRef]] = {
    resultRequested = true
    new WrappingResourceIterator[util.Map[String, AnyRef]] {
      private val inner = result.mapIterator
      def hasNext: Boolean = inner.hasNext
      def next(): util.Map[String, AnyRef] = {
        val scalaRow: collection.Map[String, AnyValue] = inner.next()
        val javaRow = new util.HashMap[String, AnyRef](scalaRow.size)
        for (kv <- scalaRow)
          javaRow.put(kv._1, query.asObject(kv._2))
        javaRow
      }
    }
  }

  override def queryStatistics(): QueryStatistics = state.getStatistics

  override def close(): Unit = {}

  private trait WrappingResourceIterator[T] extends ResourceIterator[T] {
    def remove() { throw new UnsupportedOperationException("remove") }
    def close() { self.close() }
  }

  override def accept[EX <: Exception](visitor: QueryResultVisitor[EX]): Unit = {
    resultRequested = true
    val maybeRecordIterator = result.recordIterator
    if (maybeRecordIterator.isDefined)
      javaValues.feedQueryResultRecordIteratorToVisitable(maybeRecordIterator.get).accept(visitor)
    else
      javaValues.feedIteratorToVisitable(result.mapIterator.map(r => fieldNames.map(r))).accept(visitor)
  }

  override def consumptionState: RuntimeResult.ConsumptionState =
    if (!resultRequested) ConsumptionState.NOT_STARTED
    else if (result.mapIterator.hasNext) ConsumptionState.HAS_MORE
    else ConsumptionState.EXHAUSTED
}
