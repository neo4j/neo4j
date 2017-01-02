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

import org.neo4j.graphdb.Result.ResultVisitor
import org.neo4j.graphdb.{Notification, QueryExecutionType, ResourceIterator}

trait ExecutionResult extends Iterator[Map[String, Any]] {
  def columns: List[String]
  def javaColumns: java.util.List[String]
  def javaColumnAs[T](column: String): ResourceIterator[T]
  def columnAs[T](column: String): Iterator[T]
  def javaIterator: ResourceIterator[java.util.Map[String, Any]]
  def dumpToString(writer: PrintWriter)
  def dumpToString(): String
  def queryStatistics(): QueryStatistics
  def executionPlanDescription(): PlanDescription
  def close()
  def planDescriptionRequested: Boolean
  def executionType: QueryExecutionType
  def notifications: Iterable[Notification]
  def accept[EX <: Exception](visitor: ResultVisitor[EX])
}
