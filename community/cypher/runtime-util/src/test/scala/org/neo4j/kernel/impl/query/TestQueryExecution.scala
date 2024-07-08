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
package org.neo4j.kernel.impl.query

import org.neo4j.graphdb.ExecutionPlanDescription
import org.neo4j.graphdb.GqlStatusObject
import org.neo4j.graphdb.Notification
import org.neo4j.graphdb.QueryExecutionType
import org.neo4j.graphdb.QueryStatistics
import org.neo4j.util.Table
import org.neo4j.values.AnyValue

import java.lang
import java.util
import java.util.Collections

import scala.language.implicitConversions

class TestQueryExecution(
  override val fieldNames: Array[String],
  events: Seq[TestQueryExecution.Event]
) extends QueryExecution {

  private var index = 0
  var subscriber: QuerySubscriber = _

  def request(numberOfRecords: Long): Unit = {
    val upTo = Math.min(index + numberOfRecords, events.length)
    while (index < upTo) {
      subscriber.onResult(fieldNames.length)
      events(index) match {
        case TestQueryExecution.Row(values) =>
          subscriber.onRecord()
          for ((v, i) <- values.zipWithIndex) {
            subscriber.onField(i, v)
          }
          subscriber.onRecordCompleted()

          if (index + 1 == events.size) {
            subscriber.onResultCompleted(QueryStatistics.EMPTY)
          }
        case TestQueryExecution.Error(ex) => subscriber.onError(ex);
      }
      index += 1
    }
  }

  def await(): Boolean = index < events.size

  def cancel(): Unit = ()

  def executionType(): QueryExecutionType =
    QueryExecutionType.query(QueryExecutionType.QueryType.READ_ONLY)

  def executionPlanDescription(): ExecutionPlanDescription =
    new ExecutionPlanDescription {
      override def getName: String = ""
      override def getChildren: util.List[ExecutionPlanDescription] = Collections.emptyList
      override def getArguments: util.Map[String, AnyRef] = Collections.emptyMap
      override def getIdentifiers: util.Set[String] = Collections.emptySet
      override def hasProfilerStatistics: Boolean = false
      override def getProfilerStatistics: ExecutionPlanDescription.ProfilerStatistics = null
    }

  def getNotifications: lang.Iterable[Notification] =
    Collections.emptyList

  def getGqlStatusObjects: lang.Iterable[GqlStatusObject] =
    Collections.emptyList
}

object TestQueryExecution {
  sealed trait Event
  case class Row(values: Seq[AnyValue]) extends Event
  case class Error(err: Throwable) extends Event

  implicit def fromTable(table: Table): TestQueryExecution =
    new TestQueryExecution(table.header.toArray, table.rows.map(Row))

  def fromThrowable(header: Seq[String], err: Throwable): TestQueryExecution =
    new TestQueryExecution(header.toArray, Seq(Error(err)))
}
