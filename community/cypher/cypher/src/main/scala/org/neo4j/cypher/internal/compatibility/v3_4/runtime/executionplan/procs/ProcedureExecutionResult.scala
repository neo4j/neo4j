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
package org.neo4j.cypher.internal.compatibility.v3_4.runtime.executionplan.procs

import java.util

import org.neo4j.cypher.internal.compatibility.v3_4.runtime._
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.executionplan.{InternalQueryType, ProcedureCallMode, StandardInternalExecutionResult}
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.planDescription.InternalPlanDescription
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.planDescription.InternalPlanDescription.Arguments.{Runtime, RuntimeImpl}
import org.neo4j.cypher.internal.frontend.v3_4.ProfilerStatisticsNotReadyException
import org.neo4j.cypher.internal.frontend.v3_4.symbols.{CypherType, _}
import org.neo4j.cypher.internal.spi.v3_4.QueryContext
import org.neo4j.cypher.internal.v3_4.logical.plans.QualifiedName
import org.neo4j.cypher.internal.{InternalExecutionResult, QueryStatistics}
import org.neo4j.cypher.result.QueryResult.{QueryResultVisitor, Record}
import org.neo4j.graphdb.Notification
import org.neo4j.graphdb.spatial.{Geometry, Point}
import org.neo4j.helpers.ValueUtils
import org.neo4j.helpers.ValueUtils._
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values
import org.neo4j.values.storable.Values.{of => _, _}

/**
  * Execution result of a Procedure
  *
  * @param context                           The QueryContext used to communicate with the kernel.
  * @param taskCloser                        called when done with the result, cleans up resources.
  * @param name                              The name of the procedure.
  * @param callMode                          The call mode of the procedure.
  * @param args                              The argument to the procedure.
  * @param indexResultNameMappings           Describes how values at output row indices are mapped onto result columns.
  * @param executionPlanDescriptionGenerator Generator for the plan description of the result.
  * @param executionMode                     The execution mode.
  */
class ProcedureExecutionResult(context: QueryContext,
                               taskCloser: TaskCloser,
                               name: QualifiedName,
                               callMode: ProcedureCallMode,
                               args: Seq[Any],
                               indexResultNameMappings: IndexedSeq[(Int, String, CypherType)],
                               executionPlanDescriptionGenerator: () => InternalPlanDescription,
                               val executionMode: ExecutionMode)
  extends StandardInternalExecutionResult(context, ProcedureRuntimeName, Some(taskCloser)) {

  override val fieldNames: Array[String] = indexResultNameMappings.map(_._2).toArray

  private final val executionResults = executeCall

  // The signature mode is taking care of eagerization
  protected def executeCall: Iterator[Array[AnyRef]] = callMode.callProcedure(context, name, args)

  override protected def createInner = new util.Iterator[util.Map[String, Any]]() {
    override def next(): util.Map[String, Any] =
      resultAsMap(executionResults.next()) //TODO!!!¡¡¡¡
    /*
        override def next(): util.Map[String, Any] =
          try { resultAsMap( executionResults.next( ) ) }
          catch { case e: NoSuchElementException => success(); throw e }

     */

    override def hasNext: Boolean = {
      val moreToCome = executionResults.hasNext
      if (!moreToCome) {
        close()
      }
      moreToCome
    }
  }

  private def transform[T](value: AnyRef, f: T => AnyValue): AnyValue = {
    if (value == null) Values.NO_VALUE
    else f(value.asInstanceOf[T])
  }

  override def accept[EX <: Exception](visitor: QueryResultVisitor[EX]): Unit = {
    executionResults.foreach { res =>
      val fieldArray = new Array[AnyValue](indexResultNameMappings.size)
      for (i <- indexResultNameMappings.indices) {
        val mapping = indexResultNameMappings(i)
        val pos = mapping._1
        fieldArray(i) = mapping._3 match {
          case CTNode => transform(res(pos), fromNodeProxy)
          case CTRelationship => transform(res(pos), fromRelationshipProxy)
          case CTPath => transform(res(pos), asPathValue)
          case CTInteger => transform(res(pos), longValue)
          case CTFloat => transform(res(pos), doubleValue)
          case CTNumber => transform(res(pos), numberValue)
          case CTString => transform(res(pos), stringValue)
          case CTBoolean => transform(res(pos), booleanValue)
          case CTPoint => transform(res(pos), (p: Point) => asPointValue(p))
          case CTGeometry => transform(res(pos), (g: Geometry) => asPointValue(g))
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

  // TODO Look into having the kernel track updates, rather than cypher middle-layers, only sensible way I can think
  //      of to get accurate stats for procedure code
  override def queryStatistics(): QueryStatistics = context.getOptStatistics.getOrElse(QueryStatistics())

  override def queryType: InternalQueryType = callMode.queryType

  private def resultAsMap(rowData: Array[AnyRef]): util.Map[String, Any] = {
    val mapData = new util.HashMap[String, Any](rowData.length)
    indexResultNameMappings.foreach { entry => mapData.put(entry._2, rowData(entry._1)) }
    mapData
  }

  private def resultAsRefMap(rowData: Array[AnyRef]): util.Map[String, AnyRef] = {
    val mapData = new util.HashMap[String, AnyRef](rowData.length)
    indexResultNameMappings.foreach { entry => mapData.put(entry._2, rowData(entry._1)) }
    mapData
  }

  override def executionPlanDescription(): InternalPlanDescription = executionMode match {
    case ProfileMode if executionResults.hasNext =>
      completed(success = false)
      throw new ProfilerStatisticsNotReadyException()
    case _ => executionPlanDescriptionGenerator()
      .addArgument(Runtime(ProcedureRuntimeName.toTextOutput))
      .addArgument(RuntimeImpl(ProcedureRuntimeName.toTextOutput))
  }

  override def withNotifications(notification: Notification*): InternalExecutionResult = this
}
