/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v3_1

import java.io.PrintWriter
import java.util
import java.util.Collections

import org.neo4j.cypher.internal.compiler.v3_1.executionplan.{InternalExecutionResult, InternalQueryType}
import org.neo4j.cypher.internal.compiler.v3_1.planDescription.InternalPlanDescription
import org.neo4j.cypher.internal.compiler.v3_1.spi.InternalResultVisitor
import org.neo4j.cypher.internal.frontend.v3_1.notification.InternalNotification
import org.neo4j.graphdb.ResourceIterator

case class ExplainExecutionResult(columns: List[String],
                                  executionPlanDescription: InternalPlanDescription,
                                  executionType: InternalQueryType,
                                  notifications: Set[InternalNotification])
  extends InternalExecutionResult {

  def javaIterator: ResourceIterator[util.Map[String, Any]] = new EmptyResourceIterator()
  def columnAs[T](column: String) = Iterator.empty
  def javaColumns: util.List[String] = Collections.emptyList()

  def queryStatistics() = InternalQueryStatistics()

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

  override def accept[EX <: Exception](visitor: InternalResultVisitor[EX]) = {}

  override def executionMode: ExecutionMode = ExplainMode
}

final class EmptyResourceIterator[T]() extends ResourceIterator[T] {
  def close() {}

  def next(): T= Iterator.empty.next()

  def remove() {
    throw new UnsupportedOperationException
  }

  def hasNext = false
}
