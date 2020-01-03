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
package org.neo4j.cypher.internal

import org.neo4j.cypher.internal.plandescription.InternalPlanDescription
import org.neo4j.cypher.internal.result.InternalExecutionResult
import org.neo4j.cypher.internal.runtime._
import org.neo4j.cypher.result.RuntimeResult
import org.neo4j.graphdb.{Notification, Result}
import org.neo4j.kernel.impl.query.{QueryExecution, QuerySubscription, RecordingQuerySubscriber}

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

  import scala.collection.JavaConverters._
  val scalaValues = new RuntimeScalaValueConverter(isGraphKernelResultValue)

  def apply(in: Result): RewindableExecutionResult = {
    try {
      val columns = in.columns().asScala.toArray
      val result = in.asScala.map(javaResult => scalaValues.asDeepScalaMap(javaResult).asInstanceOf[Map[String, AnyRef]]).toList
      new RewindableExecutionResultImplementation(columns,
                                                  result,
                                                  NormalMode,
                                                  in.getExecutionPlanDescription.asInstanceOf[InternalPlanDescription],
                                                  in.getQueryStatistics.asInstanceOf[QueryStatistics],
                                                  Seq.empty)
    } finally in.close()
  }

  def apply(runtimeResult: RuntimeResult, queryContext: QueryContext,
            subscriber: RecordingQuerySubscriber): RewindableExecutionResult = {
    try {
      apply(runtimeResult,
            queryContext,
            subscriber,
            runtimeResult.fieldNames(),
            NormalMode,
            () => InternalPlanDescription.error("Can't get plan description from RuntimeResult"),
            Set.empty
            )
    } finally runtimeResult.close()
  }

  def apply(result: QueryExecution, queryContext: QueryContext,
            subscriber: RecordingQuerySubscriber): RewindableExecutionResult = {
    try {
      val (executionMode, notifications) = result match {
        case r: InternalExecutionResult => (r.executionMode, r.notifications.toSet)
        case _ => (NormalMode, Set.empty[Notification])
      }

      apply(result,
            queryContext,
            subscriber,
            result.fieldNames(),
            executionMode,
            () => result.executionPlanDescription().asInstanceOf[InternalPlanDescription],
            notifications
            )
    } finally result.cancel()
  }

  def apply(subscription: QuerySubscription,
            queryContext: QueryContext,
            subscriber: RecordingQuerySubscriber,
            columns: Array[String],
            executionMode: ExecutionMode,
            planDescription: () => InternalPlanDescription,
            notifications: Set[Notification]): RewindableExecutionResult = {
    try {
      subscription.request(Long.MaxValue)
      subscriber.assertNoErrors()
      subscription.await()
      val result = new ArrayBuffer[Map[String, AnyRef]]()
      subscriber.getOrThrow().asScala.foreach( record => {
        val row = columns.zipWithIndex.map {
          case (value, index) => value -> scalaValues.asDeepScalaValue(queryContext.asObject(record(index))).asInstanceOf[AnyRef]
        }
        if (row.nonEmpty) result.append(row.toMap)
      })
      new RewindableExecutionResultImplementation(columns,
                                                  result,
                                                  executionMode,
                                                  planDescription(),
                                                  subscriber.queryStatistics().asInstanceOf[QueryStatistics],
                                                  notifications)
    } finally subscription.cancel()
  }
}
