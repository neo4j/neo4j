/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_3.executionplan

import java.io.PrintWriter

import org.neo4j.cypher.internal.compiler.v2_3.InternalQueryStatistics
import org.neo4j.cypher.internal.compiler.v2_3.planDescription.InternalPlanDescription
import org.neo4j.cypher.internal.frontend.v2_3.notification.InternalNotification
import org.neo4j.graphdb.Result.ResultVisitor
import org.neo4j.graphdb.{QueryExecutionType, ResourceIterator}

trait InternalExecutionResult extends Iterator[Map[String, Any]] {
  def columns: List[String]
  def javaColumns: java.util.List[String]
  def javaColumnAs[T](column: String): ResourceIterator[T]
  def columnAs[T](column: String): Iterator[T]
  def javaIterator: ResourceIterator[java.util.Map[String, Any]]
  def dumpToString(writer: PrintWriter)
  def dumpToString(): String
  def queryStatistics(): InternalQueryStatistics
  def executionPlanDescription(): InternalPlanDescription
  def close()
  def planDescriptionRequested: Boolean
  def executionType: QueryExecutionType
  def notifications: Iterable[InternalNotification]
  //todo this should not depend on external visitor
  @throws(classOf[Exception])
  def accept[EX <: Exception](visitor: ResultVisitor[EX])
}
