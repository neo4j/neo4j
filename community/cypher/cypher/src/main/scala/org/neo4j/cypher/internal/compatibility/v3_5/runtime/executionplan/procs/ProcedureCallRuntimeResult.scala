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
package org.neo4j.cypher.internal.compatibility.v3_5.runtime.executionplan.procs

import java.time._
import java.time.temporal.TemporalAmount
import java.util

import org.neo4j.cypher.internal.runtime._
import org.neo4j.cypher.internal.v3_5.logical.plans.QualifiedName
import org.neo4j.cypher.internal.v3_5.util.symbols.{CypherType, _}
import org.neo4j.cypher.result.QueryResult.{QueryResultVisitor, Record}
import org.neo4j.cypher.result.RuntimeResult.ConsumptionState
import org.neo4j.cypher.result.{OperatorProfile, QueryProfile, RuntimeResult}
import org.neo4j.graphdb.ResourceIterator
import org.neo4j.graphdb.spatial.{Geometry, Point}
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext
import org.neo4j.kernel.impl.util.ValueUtils
import org.neo4j.kernel.impl.util.ValueUtils._
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values._
import org.neo4j.values.storable._

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
                                 id: Option[Int],
                                 callMode: ProcedureCallMode,
                                 args: Seq[Any],
                                 indexResultNameMappings: IndexedSeq[(Int, String, CypherType)],
                                 profile: Boolean,
                                 procedureCallContext : ProcedureCallContext) extends RuntimeResult {

  self =>

  private val counter = Counter()

  override val fieldNames: Array[String] = indexResultNameMappings.map(_._2).toArray

  private final val executionResults: Iterator[Array[AnyRef]] = executeCall
  private var resultRequested = false

  // The signature mode is taking care of eagerization
  protected def executeCall: Iterator[Array[AnyRef]] = {
    val iterator =
      if (id.nonEmpty) callMode.callProcedure(context, id.get, args, procedureCallContext)
      else callMode.callProcedure(context, name, args, procedureCallContext)

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

  private def transform[T](value: AnyRef, f: T => AnyValue): AnyValue = {
    if (value == null) NO_VALUE
    else f(value.asInstanceOf[T])
  }

  override def accept[EX <: Exception](visitor: QueryResultVisitor[EX]): Unit = {
    resultRequested = true
    executionResults.foreach { res =>
      val fieldArray = new Array[AnyValue](indexResultNameMappings.size)
      for (i <- indexResultNameMappings.indices) {
        val mapping = indexResultNameMappings(i)
        val pos = mapping._1
        fieldArray(i) = mapping._3 match {
          case CTNode => transform(res(pos), fromNodeProxy)
          case CTRelationship => transform(res(pos), fromRelationshipProxy)
          case CTPath => transform(res(pos), fromPath)
          case CTInteger => transform(res(pos), longValue)
          case CTFloat => transform(res(pos), doubleValue)
          case CTNumber => transform(res(pos), numberValue)
          case CTString => transform(res(pos), stringValue)
          case CTBoolean => transform(res(pos), booleanValue)
          case CTPoint => transform(res(pos), (p: Point) => asPointValue(p))
          case CTGeometry => transform(res(pos), (g: Geometry) => asGeometryValue(g))
          case CTDateTime => transform(res(pos), (g: ZonedDateTime) => DateTimeValue.datetime(g))
          case CTLocalDateTime => transform(res(pos), (g: LocalDateTime) => LocalDateTimeValue.localDateTime(g))
          case CTDate => transform(res(pos), (g: LocalDate) => DateValue.date(g))
          case CTTime => transform(res(pos), (g: OffsetTime) => TimeValue.time(g))
          case CTLocalTime => transform(res(pos), (g: LocalTime) => LocalTimeValue.localTime(g))
          case CTDuration => transform(res(pos), (g: TemporalAmount) => Values.durationValue(g))
          case CTMap => transform(res(pos), asMapValue)
          case ListType(_) => transform(res(pos), asListValue)
          case CTAny => transform(res(pos), ValueUtils.of)
        }
      }
      visitor.visit(new Record {
        override def fields(): Array[AnyValue] = fieldArray
      })
    }
    close()
  }

  override def queryStatistics(): QueryStatistics = context.getOptStatistics.getOrElse(QueryStatistics())

  private def resultAsMap(rowData: Array[AnyRef]): util.Map[String, AnyRef] = {
    val mapData = new util.HashMap[String, AnyRef](rowData.length)
    indexResultNameMappings.foreach { entry => mapData.put(entry._2, rowData(entry._1)) }
    mapData
  }

  override def isIterable: Boolean = true

  override def consumptionState: RuntimeResult.ConsumptionState =
    if (!resultRequested) ConsumptionState.NOT_STARTED
    else if (executionResults.hasNext) ConsumptionState.HAS_MORE
    else ConsumptionState.EXHAUSTED

  override def close(): Unit = {}

  override def queryProfile(): QueryProfile = StandaloneProcedureCallProfile(counter.counted)
}

case class StandaloneProcedureCallProfile(rowCount: Long) extends QueryProfile with OperatorProfile {

  override def operatorProfile(operatorId: Int): OperatorProfile = this

  override def time(): Long = OperatorProfile.NO_DATA

  override def dbHits(): Long = 1 // for unclear reasons

  override def rows(): Long = rowCount

  override def pageCacheHits(): Long = OperatorProfile.NO_DATA

  override def pageCacheMisses(): Long = OperatorProfile.NO_DATA
}
