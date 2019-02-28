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
package org.neo4j.cypher.internal.procs

import java.util

import org.neo4j.cypher.internal.runtime._
import org.neo4j.cypher.internal.v4_0.util.InternalException
import org.neo4j.cypher.internal.v4_0.util.symbols.CypherType
import org.neo4j.cypher.result.QueryResult.{QueryResultVisitor, Record}
import org.neo4j.cypher.result.RuntimeResult.ConsumptionState
import org.neo4j.cypher.result.{OperatorProfile, QueryProfile, RuntimeResult}
import org.neo4j.graphdb.ResourceIterator
import org.neo4j.internal.kernel.api.procs.QualifiedName
import org.neo4j.kernel.impl.query.QuerySubscriber
import org.neo4j.values.AnyValue

/**
  * Result of calling a procedure.
  *
  * @param context                           The QueryContext used to communicate with the kernel.
  * @param name                              The name of the procedure.
  * @param id                                The id of the procedure.
  * @param callMode                          The call mode of the procedure.
  * @param args                              The argument to the procedure.
  * @param indexResultNameMappings           Describes how values at output row indices are mapped onto result columns.
  */
class ProcedureCallRuntimeResult(context: QueryContext,
                                 name: QualifiedName,
                                 id: Int,
                                 callMode: ProcedureCallMode,
                                 args: Seq[AnyValue],
                                 indexResultNameMappings: IndexedSeq[(Int, String, CypherType)],
                                 profile: Boolean,
                                 subscriber: QuerySubscriber) extends RuntimeResult {

  self =>

  private val counter = Counter()

  override val fieldNames: Array[String] = indexResultNameMappings.map(_._2).toArray

  private final val executionResults: Iterator[Array[AnyValue]] = executeCall
  private var resultRequested = false
  private var reactiveIterator: ReactiveIterator = _

  // The signature mode is taking care of eagerization
  protected def executeCall: Iterator[Array[AnyValue]] = {
    val iterator = callMode.callProcedure(context, id, args)

    if (profile)
      counter.track(iterator)
    else
      iterator
  }

  override def asIterator(): ResourceIterator[util.Map[String, AnyRef]] = {
    resultRequested = true
    new ResourceIterator[util.Map[String, AnyRef]]() {
      override def next(): util.Map[String, AnyRef] =
        resultAsMap(executionResults.next())

      override def hasNext: Boolean = {
        val moreToCome = executionResults.hasNext
        if (!moreToCome) {
          close()
        }
        moreToCome
      }

      override def close(): Unit = self.close()
    }
  }

  override def accept[EX <: Exception](visitor: QueryResultVisitor[EX]): Unit = {
    resultRequested = true
    executionResults.foreach { res =>
      val fieldArray = new Array[AnyValue](indexResultNameMappings.size)
      for (i <- indexResultNameMappings.indices) {
        val mapping = indexResultNameMappings(i)
        val pos = mapping._1
        fieldArray(i) = res(pos)
      }
      visitor.visit(new Record {
        override def fields(): Array[AnyValue] = fieldArray
      })
    }
    close()
  }

  override def queryStatistics(): QueryStatistics = context.getOptStatistics.getOrElse(QueryStatistics())

  private def resultAsMap(rowData: Array[AnyValue]): util.Map[String, AnyRef] = {
    val mapData = new util.HashMap[String, AnyRef](rowData.length)
    indexResultNameMappings.foreach { entry => mapData.put(entry._2, context.asObject(rowData(entry._1))) }
    mapData
  }

  override def isIterable: Boolean = true

  override def consumptionState: RuntimeResult.ConsumptionState =
    if (!resultRequested) ConsumptionState.NOT_STARTED
    else if (executionResults.hasNext) ConsumptionState.HAS_MORE
    else ConsumptionState.EXHAUSTED

  override def close(): Unit = {}

  override def queryProfile(): QueryProfile = StandaloneProcedureCallProfile(counter.counted)

  override def request(numberOfRecords: Long): Unit = {
    resultRequested = true
    if (reactiveIterator == null) {
      reactiveIterator = new ReactiveIterator(executionResults, this, indexResultNameMappings.map(_._1).toArray)
    }
    reactiveIterator.addDemand(numberOfRecords)
  }

  override def cancel(): Unit = if (reactiveIterator != null) {
    reactiveIterator.cancel()
  }

  override def await(): Boolean = {
    if (reactiveIterator == null) {
      throw new InternalException("Call to await before calling request");
    }
    reactiveIterator.await(subscriber)
  }
}

case class StandaloneProcedureCallProfile(rowCount: Long) extends QueryProfile with OperatorProfile {

  override def operatorProfile(operatorId: Int): OperatorProfile = this

  override def time(): Long = OperatorProfile.NO_DATA

  override def dbHits(): Long = 1 // for unclear reasons

  override def rows(): Long = rowCount

  override def pageCacheHits(): Long = OperatorProfile.NO_DATA

  override def pageCacheMisses(): Long = OperatorProfile.NO_DATA
}
