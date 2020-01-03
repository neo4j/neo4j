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
package org.neo4j.cypher.internal.compatibility.v3_5.runtime

import java.io.PrintWriter
import java.util
import java.util.Collections

import org.neo4j.cypher.internal.runtime._
import org.neo4j.cypher.internal.runtime.planDescription.InternalPlanDescription
import org.neo4j.cypher.result.QueryResult.QueryResultVisitor
import org.neo4j.graphdb.Result.ResultVisitor
import org.neo4j.graphdb.{Notification, ResourceIterator}
import org.neo4j.helpers.collection.Iterators

case class ExplainExecutionResult(fieldNames: Array[String],
                                  planDescription: InternalPlanDescription,
                                  queryType: InternalQueryType,
                                  notifications: Set[Notification])
  extends InternalExecutionResult {

  override def initiate(): Unit = {}

  override def javaIterator: ResourceIterator[util.Map[String, AnyRef]] = Iterators.emptyResourceIterator()
  override def javaColumns: util.List[String] = Collections.emptyList()

  override def queryStatistics() = QueryStatistics()

  override def dumpToString(writer: PrintWriter): Unit = writer.print(dumpToString)

  override val dumpToString: String =
     """+--------------------------------------------+
       || No data returned, and nothing was changed. |
       |+--------------------------------------------+
       |""".stripMargin

  override def javaColumnAs[T](column: String): ResourceIterator[T] = Iterators.emptyResourceIterator()

  override def isClosed: Boolean = true

  override def close(reason: CloseReason): Unit = {}

  override def accept[EX <: Exception](visitor: ResultVisitor[EX]): Unit = {}
  override def accept[EX <: Exception](visitor: QueryResultVisitor[EX]): Unit = {}

  override def executionMode: ExecutionMode = ExplainMode

  override def executionPlanDescription(): InternalPlanDescription = planDescription
}
