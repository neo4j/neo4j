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
package org.neo4j.cypher.internal

import org.neo4j.cypher.internal.javacompat.ExecutionResult
import org.neo4j.cypher.internal.runtime._
import org.neo4j.cypher.internal.runtime.planDescription.InternalPlanDescription
import org.neo4j.cypher.result.QueryResult.QueryResultVisitor
import org.neo4j.cypher.result.{QueryResult, RuntimeResult}
import org.neo4j.graphdb.Result.{ResultRow, ResultVisitor}
import org.neo4j.graphdb.{Notification, Result}

import scala.collection.mutable.ArrayBuffer

trait RewindableExecutionResult {
  def columns: Array[String]
  protected def result: Seq[Map[String, AnyRef]]
  def executionMode: ExecutionMode
  protected def planDescription: InternalPlanDescription
  protected def statistics: QueryStatistics
  def notifications: Iterable[Notification]

  def columnAs[T](column: String): Iterator[T] = result.iterator.map(row => row(column).asInstanceOf[T])
  def toList: List[Map[String, AnyRef]] = result.toList
  def toSet: Set[Map[String, AnyRef]] = result.toSet
  def size: Long = result.size
  def head(str: String): AnyRef = result.head(str)

  def single: Map[String, AnyRef] =
    if (result.size == 1)
      result.head
    else
      throw new IllegalStateException(s"Result should have one row, but had ${result.size}")

  def executionPlanDescription(): InternalPlanDescription = planDescription

  def executionPlanString(): String = planDescription.toString

  def queryStatistics(): QueryStatistics = statistics

  def isEmpty: Boolean = result.isEmpty
}

/**
  * Helper class to ease asserting on cypher results from scala.
  */
class RewindableExecutionResultImplementation(val columns: Array[String],
                                              protected val result: Seq[Map[String, AnyRef]],
                                              val executionMode: ExecutionMode,
                                              protected val planDescription: InternalPlanDescription,
                                              protected val statistics: QueryStatistics,
                                              val notifications: Iterable[Notification]) extends RewindableExecutionResult

object RewindableExecutionResult {

  val scalaValues = new RuntimeScalaValueConverter(isGraphKernelResultValue)

  def apply(in: Result): RewindableExecutionResult = {
    val internal = in.asInstanceOf[ExecutionResult].internalExecutionResult
    apply(internal)
  }

  def apply(runtimeResult: RuntimeResult, queryContext: QueryContext): RewindableExecutionResult = {
    val result = new ArrayBuffer[Map[String, AnyRef]]()
    val columns = runtimeResult.fieldNames()

    runtimeResult.accept(new QueryResultVisitor[Exception] {
      override def visit(row: QueryResult.Record): Boolean = {
        val map = new java.util.HashMap[String, AnyRef]()
        val values = row.fields()
        for (i <- columns.indices) {
          map.put(columns(i), queryContext.asObject(values(i)))
        }
        result += scalaValues.asDeepScalaMap(map).asInstanceOf[Map[String, AnyRef]]
        true
      }
    })

    new RewindableExecutionResultImplementation(columns, result, NormalMode, null, runtimeResult.queryStatistics(), Seq.empty)
  }

  def apply(internal: InternalExecutionResult) : RewindableExecutionResult = {
    val result = new ArrayBuffer[Map[String, AnyRef]]()
    val columns = internal.fieldNames()

    internal.accept(new ResultVisitor[Exception] {
      override def visit(row: ResultRow): Boolean = {
        val map = new java.util.HashMap[String, AnyRef]()
        for (c <- columns) {
          map.put(c, row.get(c))
        }
        result += scalaValues.asDeepScalaMap(map).asInstanceOf[Map[String, AnyRef]]
        true
      }
    })

    new RewindableExecutionResultImplementation(
      columns,
      result.toList,
      internal.executionMode,
      internal.executionPlanDescription(),
      internal.queryStatistics(),
      internal.notifications
    )
  }
}
