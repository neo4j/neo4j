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
package org.neo4j.cypher.internal.compatibility

import java.io.PrintWriter
import java.util

import org.neo4j.cypher._
import org.neo4j.cypher.internal.AmendedRootPlanDescription
import org.neo4j.cypher.internal.compiler.v2_2.planDescription.Argument
import org.neo4j.cypher.internal.compiler.v2_2.planDescription.InternalPlanDescription.Arguments.{Rows, DbHits}
import org.neo4j.cypher.internal.compiler.v2_3.{InterpretedRuntimeName, RulePlannerName}
import org.neo4j.cypher.internal.compiler.v2_3.helpers.iteratorToVisitable
import org.neo4j.cypher.javacompat.ProfilerStatistics
import org.neo4j.graphdb.QueryExecutionType.{QueryType, profiled, query}
import org.neo4j.graphdb.ResourceIterator
import org.neo4j.graphdb.Result.ResultVisitor
import org.neo4j.kernel.impl.query.{QueryExecutionMonitor, QuerySession}
import org.neo4j.cypher.internal.compiler.v1_9.executionplan.{PlanDescription => PlanDescription_v1_9}
import scala.collection.JavaConverters._

case class LegacyExecutionResultWrapper(inner: ExecutionResult, planDescriptionRequested: Boolean, version: CypherVersion)
                                       (implicit monitor: QueryExecutionMonitor, session: QuerySession) extends ExtendedExecutionResult {
  self =>

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
      case extended: ExtendedPlanDescription =>
        extended

      case legacy: org.neo4j.cypher.internal.compiler.v1_9.executionplan.PlanDescription =>
        CompatibilityPlanDescriptionFor1_9(legacy, planDescriptionRequested)

      case other =>
        ExtendedPlanDescriptionWrapper(other)
    }

    new AmendedRootPlanDescription(description, version, RulePlannerName, InterpretedRuntimeName)
  }

  def close() = inner.close()

  def next() = inner.next()

  def hasNext = inner.hasNext

  def executionType = if (planDescriptionRequested) profiled(queryType) else query(queryType)

  def notifications = inner match {
    case extended: ExtendedExecutionResult => extended.notifications
    case _ => Iterable.empty
  }

  def accept[EX <: Exception](visitor: ResultVisitor[EX]) = {
    try {
      iteratorToVisitable.accept(self, visitor)
    } finally {
      self.close()
    }
  }

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

// This is carefully tiptoeing around calling abstract methods that have changed between 2.3 and 2.0
// (i.e. if you touch this be very sure to not unintentionally introduce an AbstractMethodError)
case class CompatibilityPlanDescriptionFor1_9(legacy: PlanDescription_v1_9, planDescriptionRequested: Boolean) extends ExtendedPlanDescription {
  self =>

  def identifiers: Set[String] = Set.empty

  override def children: Seq[PlanDescription] = legacy.children.map(CompatibilityPlanDescriptionFor1_9(_, planDescriptionRequested))

  def extendedChildren: Seq[ExtendedPlanDescription] = legacy.children.map(CompatibilityPlanDescriptionFor1_9(_, planDescriptionRequested))

  override def asJava: javacompat.PlanDescription = new javacompat.PlanDescription {
    def getProfilerStatistics: ProfilerStatistics = legacy.asJava.getProfilerStatistics

    def getName: String = name
    def getIdentifiers: util.Set[String] = identifiers.asJava
    def getArguments: util.Map[String, AnyRef] = self.arguments.asJava
    def getChildren: util.List[javacompat.PlanDescription] = extendedChildren.toList.map(_.asJava).asJava

    def hasProfilerStatistics: Boolean = self.hasProfilerStatistics

    override def toString: String = self.toString
  }

  def arguments: Map[String, AnyRef] = legacy.args.toMap

  override def hasProfilerStatistics = planDescriptionRequested

  def name: String = legacy.name

  override def toString = legacy.toString
}

case class ExtendedPlanDescriptionWrapper(inner: PlanDescription) extends ExtendedPlanDescription {
  def extendedChildren = inner.children.map(ExtendedPlanDescriptionWrapper)
  def identifiers = Set.empty
  def asJava = inner.asJava
  override def children = inner.children
  def arguments = inner.arguments
  override def hasProfilerStatistics = inner.hasProfilerStatistics
  def name = inner.name

  override def toString = inner.toString
}
