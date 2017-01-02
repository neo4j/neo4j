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
package org.neo4j.cypher.internal.compatibility

import java.io.PrintWriter

import org.neo4j.cypher._
import org.neo4j.cypher.internal.AmendedRootPlanDescription
import org.neo4j.cypher.internal.compiler.v2_2.RulePlannerName
import org.neo4j.graphdb.QueryExecutionType.{QueryType, profiled, query}
import org.neo4j.graphdb.ResourceIterator
import org.neo4j.kernel.impl.query.{QueryExecutionMonitor, QuerySession}

case class LegacyExecutionResultWrapper(inner: ExecutionResult, planDescriptionRequested: Boolean, version: CypherVersion)
                                       (implicit monitor: QueryExecutionMonitor, session: QuerySession) extends ExtendedExecutionResult {
  def columns = inner.columns

  protected val jIterator = inner.javaIterator

  private def endQueryExecution() = {
    monitor.endSuccess(session) // this method is expected to be idempotent
  }

  if (!jIterator.hasNext) {
    endQueryExecution()
  }

  def javaIterator = new ResourceIterator[java.util.Map[String, Any]] {

    override def close() = {
      endQueryExecution()
      jIterator.close()
    }

    override def next() = {
      try {
        jIterator.next()
      } catch {
        case e: Throwable =>
          monitor.endFailure(session, e)
          throw e
      }
    }

    override def remove(): Unit = jIterator.remove()

    override def hasNext: Boolean = {
      val next = jIterator.hasNext
      if (!next) {
        endQueryExecution()
      }
      next
    }
  }

  def columnAs[T](column: String) = inner.columnAs[T](column)

  def javaColumns = inner.javaColumns

  def queryStatistics() = inner.queryStatistics()

  def dumpToString(writer: PrintWriter) = inner.dumpToString(writer)

  def dumpToString() = inner.dumpToString()

  def javaColumnAs[T](column: String) = inner.javaColumnAs[T](column)

  def executionPlanDescription(): PlanDescription = {
    val description = inner.executionPlanDescription() match {
      case extended: ExtendedPlanDescription => extended
      case other => ExtendedPlanDescriptionWrapper(other)
    }

    new AmendedRootPlanDescription(description, version, RulePlannerName)
  }

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

case class ExtendedPlanDescriptionWrapper(inner: PlanDescription) extends ExtendedPlanDescription {
  def extendedChildren = inner.children.map(ExtendedPlanDescriptionWrapper)
  def identifiers = Set.empty
  def asJava = inner.asJava
  def children = inner.children
  def arguments = inner.arguments
  def hasProfilerStatistics = inner.hasProfilerStatistics
  def name = inner.name
  override def toString = inner.toString
}
