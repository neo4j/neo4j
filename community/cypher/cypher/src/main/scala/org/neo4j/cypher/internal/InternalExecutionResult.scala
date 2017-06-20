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
package org.neo4j.cypher.internal

import java.io.PrintWriter

import org.neo4j.cypher.internal.compatibility.v3_3.runtime.{ExecutionMode, ExplainMode, NormalMode, ProfileMode}
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.executionplan._
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.planDescription.InternalPlanDescription
import org.neo4j.graphdb.Result.ResultVisitor
import org.neo4j.graphdb.{Notification, QueryExecutionType, ResourceIterator}

trait InternalExecutionResult extends Iterator[Map[String, Any]] {

  def columns: List[String]
  def columnAs[T](column: String): Iterator[T]

  def javaColumns: java.util.List[String]
  def javaColumnAs[T](column: String): ResourceIterator[T]
  def javaIterator: ResourceIterator[java.util.Map[String, Any]]

  def dumpToString(writer: PrintWriter)
  def dumpToString(): String

  def queryStatistics(): QueryStatistics

  def planDescriptionRequested: Boolean
  def executionPlanDescription(): InternalPlanDescription

  def executionType: InternalQueryType
  def executionMode: ExecutionMode

  def notifications: Iterable[Notification]

  def accept[EX <: Exception](visitor: ResultVisitor[EX])

  def close()

  def withNotifications(notification: Notification*): InternalExecutionResult

  def publicExecutionType: QueryExecutionType = {

    val qt = executionType match {
      case READ_ONLY => QueryExecutionType.QueryType.READ_ONLY
      case READ_WRITE => QueryExecutionType.QueryType.READ_WRITE
      case WRITE => QueryExecutionType.QueryType.WRITE
      case SCHEMA_WRITE => QueryExecutionType.QueryType.SCHEMA_WRITE
      case DBMS => QueryExecutionType.QueryType.READ_ONLY
    }

    executionMode match {
      case ExplainMode => QueryExecutionType.explained(qt)
      case ProfileMode => QueryExecutionType.profiled(qt)
      case NormalMode => QueryExecutionType.query(qt)
    }
  }
}
