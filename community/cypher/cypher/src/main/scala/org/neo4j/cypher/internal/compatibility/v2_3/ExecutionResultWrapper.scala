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
package org.neo4j.cypher.internal.compatibility.v2_3

import java.io.PrintWriter

import org.neo4j.cypher._
import org.neo4j.cypher.internal._
import org.neo4j.cypher.internal.compatibility._
import org.neo4j.cypher.internal.compiler.v2_3.executionplan.InternalExecutionResult
import org.neo4j.cypher.internal.compiler.v2_3.planDescription.InternalPlanDescription
import org.neo4j.cypher.internal.compiler.v2_3.planDescription.InternalPlanDescription.Arguments._
import org.neo4j.cypher.internal.compiler.v2_3.{PlannerName, _}
import org.neo4j.cypher.internal.frontend.v2_3.notification.{InternalNotification, LegacyPlannerNotification, PlannerUnsupportedNotification, RuntimeUnsupportedNotification, _}
import org.neo4j.cypher.internal.frontend.v2_3.{InputPosition => InternalInputPosition}
import org.neo4j.graphdb.Result.ResultVisitor
import org.neo4j.graphdb.impl.notification.{NotificationCode, NotificationDetail}
import org.neo4j.graphdb.{InputPosition, QueryExecutionType}

import scala.collection.JavaConverters._

object ExecutionResultWrapper {
  def unapply(v: Any): Option[(InternalExecutionResult, PlannerName, RuntimeName)] = v match {
    case closing: ClosingExecutionResult => unapply(closing.inner)
    case wrapper: ExecutionResultWrapper => Some((wrapper.inner, wrapper.planner, wrapper.runtime))
    case _ => None
  }
}

class ExecutionResultWrapper(val inner: InternalExecutionResult, val planner: PlannerName, val runtime: RuntimeName)
  extends ExecutionResult {

  override def planDescriptionRequested = inner.planDescriptionRequested

  override def javaIterator = inner.javaIterator

  override def columnAs[T](column: String) = inner.columnAs(column)

  override def columns = inner.columns

  override def javaColumns = inner.javaColumns

  def queryStatistics() = {
    val i = inner.queryStatistics()
    QueryStatistics(nodesCreated = i.nodesCreated,
      relationshipsCreated = i.relationshipsCreated,
      propertiesSet = i.propertiesSet,
      nodesDeleted = i.nodesDeleted,
      relationshipsDeleted = i.relationshipsDeleted,
      labelsAdded = i.labelsAdded,
      labelsRemoved = i.labelsRemoved,
      indexesAdded = i.indexesAdded,
      indexesRemoved = i.indexesRemoved,
      constraintsAdded = i.uniqueConstraintsAdded + i.existenceConstraintsAdded,
      constraintsRemoved = i.uniqueConstraintsRemoved + i.existenceConstraintsRemoved
    )
  }

  override def dumpToString(writer: PrintWriter) = inner.dumpToString(writer)

  override def dumpToString() = inner.dumpToString()

  override def javaColumnAs[T](column: String) = inner.javaColumnAs(column)

  def executionPlanDescription(): org.neo4j.cypher.internal.PlanDescription =
    convert(
      inner.executionPlanDescription().
        addArgument(Version("CYPHER 2.3")).
        addArgument(Planner(planner.toTextOutput)).
        addArgument(PlannerImpl(planner.name)).
        addArgument(Runtime(runtime.toTextOutput)).
        addArgument(RuntimeImpl(runtime.name))
    )

  private def convert(i: InternalPlanDescription): org.neo4j.cypher.internal.PlanDescription = exceptionHandler.runSafely {
    CompatibilityPlanDescription(i, CypherVersion.v2_3, planner, runtime)
  }

  override def hasNext = inner.hasNext

  override def next() = inner.next()

  override def close() = inner.close()

  def executionType: QueryExecutionType = inner.executionType

  def notifications = inner.notifications.map(asKernelNotification)

  private def asKernelNotification(notification: InternalNotification) = notification match {
    case CartesianProductNotification(pos, variables) =>
      NotificationCode.CARTESIAN_PRODUCT.notification(pos.asInputPosition, NotificationDetail.Factory.cartesianProduct(variables.asJava))
    case LegacyPlannerNotification =>
      NotificationCode.LEGACY_PLANNER.notification(InputPosition.empty)
    case LengthOnNonPathNotification(pos) =>
      NotificationCode.LENGTH_ON_NON_PATH.notification(pos.asInputPosition)
    case PlannerUnsupportedNotification =>
      NotificationCode.PLANNER_UNSUPPORTED.notification(InputPosition.empty)
    case RuntimeUnsupportedNotification =>
      NotificationCode.RUNTIME_UNSUPPORTED.notification(InputPosition.empty)
    case IndexHintUnfulfillableNotification(label, propertyKey) =>
      NotificationCode.INDEX_HINT_UNFULFILLABLE.notification(InputPosition.empty, NotificationDetail.Factory.index(label, propertyKey))
    case JoinHintUnfulfillableNotification(variables) =>
      NotificationCode.JOIN_HINT_UNFULFILLABLE.notification(InputPosition.empty, NotificationDetail.Factory.joinKey(variables.asJava))
    case JoinHintUnsupportedNotification(variables) =>
      NotificationCode.JOIN_HINT_UNSUPPORTED.notification(InputPosition.empty, NotificationDetail.Factory.joinKey(variables.asJava))
    case IndexLookupUnfulfillableNotification(labels) =>
      NotificationCode.INDEX_LOOKUP_FOR_DYNAMIC_PROPERTY.notification(InputPosition.empty, NotificationDetail.Factory.indexSeekOrScan(labels.asJava))
    case BareNodeSyntaxDeprecatedNotification(pos) =>
      NotificationCode.BARE_NODE_SYNTAX_DEPRECATED.notification(pos.asInputPosition)
    case EagerLoadCsvNotification =>
      NotificationCode.EAGER_LOAD_CSV.notification(InputPosition.empty)
    case LargeLabelWithLoadCsvNotification =>
      NotificationCode.LARGE_LABEL_LOAD_CSV.notification(InputPosition.empty)
    case MissingLabelNotification(pos, label) =>
      NotificationCode.MISSING_LABEL.notification(pos.asInputPosition, NotificationDetail.Factory.label(label))
    case MissingRelTypeNotification(pos, relType) =>
      NotificationCode.MISSING_REL_TYPE.notification(pos.asInputPosition, NotificationDetail.Factory.relationshipType(relType))
    case MissingPropertyNameNotification(pos, name) =>
      NotificationCode.MISSING_PROPERTY_NAME.notification(pos.asInputPosition, NotificationDetail.Factory.propertyName(name))
    case UnboundedShortestPathNotification(pos) =>
      NotificationCode.UNBOUNDED_SHORTEST_PATH.notification(pos.asInputPosition)
  }

  override def accept[EX <: Exception](visitor: ResultVisitor[EX]) = inner.accept(visitor)

  private implicit class ConvertibleCompilerInputPosition(pos: InternalInputPosition) {
    def asInputPosition = new InputPosition(pos.offset, pos.line, pos.column)
  }
}
