/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal

import org.neo4j.cypher.internal.plandescription.InternalPlanDescription
import org.neo4j.cypher.internal.result.InternalExecutionResult
import org.neo4j.cypher.internal.runtime.ExecutionMode
import org.neo4j.cypher.internal.runtime.NormalMode
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.QueryStatistics
import org.neo4j.cypher.internal.runtime.QueryTransactionalContext
import org.neo4j.cypher.internal.runtime.RuntimeScalaValueConverter
import org.neo4j.cypher.internal.runtime.isGraphKernelResultValue
import org.neo4j.graphdb.Entity
import org.neo4j.graphdb.GqlStatusObject
import org.neo4j.graphdb.Result
import org.neo4j.kernel.impl.query.QueryExecution
import org.neo4j.kernel.impl.query.RecordingQuerySubscriber
import org.neo4j.kernel.impl.query.TransactionalContext
import org.neo4j.notifications.NotificationImplementation
import org.neo4j.util.Table
import org.neo4j.values.storable.Values

import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters.IterableHasAsScala
import scala.jdk.CollectionConverters.IteratorHasAsScala
import scala.jdk.CollectionConverters.ListHasAsScala

trait RewindableExecutionResult extends AutoCloseable {
  def columns: Array[String]
  protected def result: Seq[Map[String, AnyRef]]
  def executionMode: ExecutionMode
  protected def planDescription: InternalPlanDescription
  protected def statistics: QueryStatistics
  def notifications: Iterable[NotificationImplementation]
  def gqlStatusObjectsCallable: () => Iterable[GqlStatusObject]

  def gqlStatusObjects: Iterable[GqlStatusObject] = gqlStatusObjectsCallable.apply()
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

  protected def closeable: AutoCloseable

  def close(): Unit = closeable.close()

  def asTable: Table =
    Table(columns, result.map(m => columns.map(c => Values.of(m(c)))))
}

/**
 * Helper class to ease asserting on cypher results from scala.
 */
class RewindableExecutionResultImplementation(
  val columns: Array[String],
  protected val result: Seq[Map[String, AnyRef]],
  val executionMode: ExecutionMode,
  protected val planDescription: InternalPlanDescription,
  protected val statistics: QueryStatistics,
  notificationsSupplier: () => Iterable[NotificationImplementation],
  val gqlStatusObjectsCallable: () => Iterable[GqlStatusObject],
  val closeable: AutoCloseable
) extends RewindableExecutionResult {

  override def notifications: Iterable[NotificationImplementation] = notificationsSupplier()
}

object RewindableExecutionResult {

  val scalaValues = new RuntimeScalaValueConverter(isGraphKernelResultValue)

  def apply(in: Result): RewindableExecutionResult = {
    try {
      val columns = in.columns().asScala.toArray
      val result =
        in.asScala.map(javaResult => scalaValues.asDeepScalaMap(javaResult).asInstanceOf[Map[String, AnyRef]]).toList
      new RewindableExecutionResultImplementation(
        columns,
        result,
        NormalMode,
        in.getExecutionPlanDescription.asInstanceOf[InternalPlanDescription],
        QueryStatistics(in.getQueryStatistics),
        () => in.getNotifications.asScala.map(not => not.asInstanceOf[NotificationImplementation]).toSeq,
        () => in.getGqlStatusObjects.asScala.toSeq,
        closeable = () => in.close()
      )
    } finally in.close()
  }

  def apply(
    result: QueryExecution,
    transactionalContext: TransactionalContext,
    queryContext: QueryContext,
    subscriber: RecordingQuerySubscriber
  ): RewindableExecutionResult = {
    try {
      val (executionMode, notificationsSupplier, gqlStatusObjectsCallable) = result match {
        case r: InternalExecutionResult =>
          (r.executionMode, () => r.notifications, () => r.gqlStatusObjects)
        case _ =>
          (NormalMode, () => Set.empty[NotificationImplementation], () => Seq.empty[GqlStatusObject])
      }

      apply(
        result,
        queryContext,
        subscriber,
        result.fieldNames(),
        executionMode,
        () => result.executionPlanDescription().asInstanceOf[InternalPlanDescription],
        notificationsSupplier,
        gqlStatusObjectsCallable
      )
    } finally {
      result.cancel()
      transactionalContext.close()
    }
  }

  private def apply(
    subscription: QueryExecution,
    queryContext: QueryContext,
    subscriber: RecordingQuerySubscriber,
    columns: Array[String],
    executionMode: ExecutionMode,
    planDescription: () => InternalPlanDescription,
    notifications: () => Iterable[NotificationImplementation],
    gqlStatusObjectsCallable: () => Iterable[GqlStatusObject]
  ): RewindableExecutionResult = {
    subscription.request(Long.MaxValue)
    subscriber.assertNoErrors()
    subscription.await()
    val result = new ArrayBuffer[Map[String, AnyRef]]()
    subscriber.getOrThrow().asScala.foreach(record => {
      val row = columns.zipWithIndex.map {
        case (value, index) => {
          val atIndex = scalaValues.asDeepScalaValue(queryContext.asObject(record(index))).asInstanceOf[AnyRef]
          checkValidInput(queryContext.transactionalContext, atIndex)
          value -> atIndex
        }
      }
      if (row.nonEmpty) result.append(row.toMap)
    })
    new RewindableExecutionResultImplementation(
      columns,
      result.toSeq,
      executionMode,
      planDescription(),
      QueryStatistics(subscriber.queryStatistics()),
      notifications,
      gqlStatusObjectsCallable,
      closeable = () => {
        subscription.cancel()
        subscription.awaitCleanup()
      }
    )
  }

  private def checkValidInput(context: QueryTransactionalContext, input: AnyRef): Unit = input match {
    case entity: Entity =>
      context.validateSameDB(entity)
    case _ => ()
  }
}
