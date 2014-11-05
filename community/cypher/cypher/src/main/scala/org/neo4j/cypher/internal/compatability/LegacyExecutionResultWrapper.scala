/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.cypher.internal.compatability

import java.io.PrintWriter

import org.neo4j.cypher.internal.AmendedRootPlanDescription
import org.neo4j.cypher._
import org.neo4j.graphdb.QueryExecutionType.{QueryType, profiled, query}

case class LegacyExecutionResultWrapper(inner: ExecutionResult, planDescriptionRequested: Boolean, version: CypherVersion) extends ExtendedExecutionResult {
  def columns = inner.columns

  def javaIterator = inner.javaIterator

  def columnAs[T](column: String) = inner.columnAs[T](column)

  def javaColumns = inner.javaColumns

  def queryStatistics() = inner.queryStatistics()

  def dumpToString(writer: PrintWriter) = inner.dumpToString(writer)

  def dumpToString() = inner.dumpToString()

  def javaColumnAs[T](column: String) = inner.javaColumnAs[T](column)

  def executionPlanDescription(): PlanDescription =
    new AmendedRootPlanDescription(inner.executionPlanDescription(), version)

  def close() = inner.close()

  def next() = inner.next()

  def hasNext = inner.hasNext

  def executionType = if (planDescriptionRequested) profiled(queryType) else query(queryType)

  // since we can't introspect the query result returned by the legacy planners, this is the best we can do
  private def queryType = if (schemaQuery(queryStatistics()))
    QueryType.SCHEMA_WRITE
  else if (columns.isEmpty)
    QueryType.WRITE
  else
    QueryType.READ_WRITE

  private def schemaQuery(stats: QueryStatistics): Boolean = stats.containsUpdates &&
    (stats.indexesAdded > 0 ||
      stats.indexesRemoved > 0 ||
      stats.constraintsAdded > 0 ||
      stats.constraintsRemoved > 0)
}
