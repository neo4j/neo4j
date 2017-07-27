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
package org.neo4j.cypher.internal.compatibility.v3_3.runtime

import java.io.PrintWriter
import java.util

import org.neo4j.cypher.internal.compatibility.v3_3.runtime.executionplan.InternalQueryType
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.planDescription.InternalPlanDescription
import org.neo4j.cypher.internal.{InternalExecutionResult, QueryStatistics}
import org.neo4j.graphdb.Result.ResultVisitor
import org.neo4j.graphdb.{Notification, ResourceIterator}
import org.neo4j.values.result.QueryResult.QueryResultVisitor

case class ExplainExecutionResult(fieldNames: Array[String],
                                  executionPlanDescription: InternalPlanDescription,
                                  queryType: InternalQueryType,
                                  notifications: Set[Notification])
  extends InternalExecutionResult {

  def javaIterator: ResourceIterator[util.Map[String, Any]] = new EmptyResourceIterator()
  def columnAs[T](column: String) = Iterator.empty

  def queryStatistics() = QueryStatistics()

  def dumpToString(writer: PrintWriter) {
    writer.print(dumpToString)
  }

  val dumpToString: String =
     """+--------------------------------------------+
       || No data returned, and nothing was changed. |
       |+--------------------------------------------+
       |""".stripMargin

  def javaColumnAs[T](column: String): ResourceIterator[T] = new EmptyResourceIterator()

  def planDescriptionRequested = true

  def close() {}

  def next() = Iterator.empty.next()

  def hasNext = false

  override def accept[EX <: Exception](visitor: ResultVisitor[EX]): Unit = {}
  override def accept[EX <: Exception](visitor: QueryResultVisitor[EX]): Unit = {}

  override def executionMode: ExecutionMode = ExplainMode

  override def withNotifications(added: Notification*): InternalExecutionResult = copy(notifications = notifications ++ added)
}

final class EmptyResourceIterator[T]() extends ResourceIterator[T] {
  def close() {}

  def next(): T= Iterator.empty.next()

  def remove() {
    throw new UnsupportedOperationException
  }

  def hasNext = false
}
