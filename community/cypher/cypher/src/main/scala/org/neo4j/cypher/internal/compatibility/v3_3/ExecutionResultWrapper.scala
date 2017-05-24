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
package org.neo4j.cypher.internal.compatibility.v3_3

import java.io.PrintWriter

import org.neo4j.cypher._
import org.neo4j.cypher.internal._
import org.neo4j.cypher.internal.compatibility._
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.{ExplainMode => ExplainModev3_3, NormalMode => NormalModev3_3, ProfileMode => ProfileModev3_3, _}
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.RuntimeName
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.executionplan._
import org.neo4j.cypher.internal.compiler.v3_3.planDescription.InternalPlanDescription
import org.neo4j.cypher.internal.compiler.v3_3.planDescription.InternalPlanDescription.Arguments._
import org.neo4j.cypher.internal.compiler.v3_3.spi.{InternalResultRow, InternalResultVisitor}
import org.neo4j.cypher.internal.frontend.v3_3.PlannerName
import org.neo4j.cypher.internal.frontend.v3_3.notification.{DeprecatedPlannerNotification, InternalNotification, PlannerUnsupportedNotification, RuntimeUnsupportedNotification, _}
import org.neo4j.graphdb
import org.neo4j.graphdb.Result.{ResultRow, ResultVisitor}
import org.neo4j.graphdb.impl.notification.{NotificationCode, NotificationDetail}

import scala.collection.JavaConverters._

object ExecutionResultWrapper {
  def unapply(v: Any): Option[(InternalExecutionResult, PlannerName, RuntimeName)] = v match {
    case closing: ClosingExecutionResult => unapply(closing.inner)
    case wrapper: ExecutionResultWrapper => Some((wrapper.inner, wrapper.planner, wrapper.runtime))
    case _ => None
  }
}

class ExecutionResultWrapper(val inner: InternalExecutionResult, val planner: PlannerName, val runtime: RuntimeName,
                             preParsingNotifications: Set[org.neo4j.graphdb.Notification]) extends ExecutionResult {

  override def planDescriptionRequested = inner.planDescriptionRequested
  override def javaIterator = inner.javaIterator
  override def columnAs[T](column: String) = inner.columnAs(column)
  override def columns = inner.columns
  override def javaColumns = inner.javaColumns

  override def queryStatistics() = {
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

  override def executionPlanDescription(): org.neo4j.cypher.internal.PlanDescription =
    convert(
      inner.executionPlanDescription().
        addArgument(Version("CYPHER 3.3")).
        addArgument(Planner(planner.toTextOutput)).
        addArgument(PlannerImpl(planner.name)).
        addArgument(Runtime(runtime.toTextOutput)).
        addArgument(RuntimeImpl(runtime.name))
      )

  private def convert(i: InternalPlanDescription): org.neo4j.cypher.internal.PlanDescription =
    CompatibilityPlanDescription(i, CypherVersion.v3_3, planner, runtime)

  override def hasNext = inner.hasNext
  override def next() = inner.next()
  override def close() = inner.close()

  override def executionType: graphdb.QueryExecutionType = {
    val qt = inner.executionType match {
      case READ_ONLY => graphdb.QueryExecutionType.QueryType.READ_ONLY
      case READ_WRITE => graphdb.QueryExecutionType.QueryType.READ_WRITE
      case WRITE => graphdb.QueryExecutionType.QueryType.WRITE
      case SCHEMA_WRITE => graphdb.QueryExecutionType.QueryType.SCHEMA_WRITE
      case DBMS => graphdb.QueryExecutionType.QueryType.READ_ONLY // TODO: We need to decide how we expose this in the public API
    }
    inner.executionMode match {
      case ExplainModev3_3 => graphdb.QueryExecutionType.explained(qt)
      case ProfileModev3_3 => graphdb.QueryExecutionType.profiled(qt)
      case NormalModev3_3 => graphdb.QueryExecutionType.query(qt)
    }
  }

  override def notifications = inner.notifications.map(asKernelNotification) ++ preParsingNotifications

  private def asKernelNotification(notification: InternalNotification) = notification match {
    case CartesianProductNotification(pos, variables) =>
      NotificationCode.CARTESIAN_PRODUCT.notification(pos.asInputPosition, NotificationDetail.Factory.cartesianProduct(variables.asJava))
    case LengthOnNonPathNotification(pos) =>
      NotificationCode.LENGTH_ON_NON_PATH.notification(pos.asInputPosition)
    case PlannerUnsupportedNotification =>
      NotificationCode.PLANNER_UNSUPPORTED.notification(graphdb.InputPosition.empty)
    case RuntimeUnsupportedNotification =>
      NotificationCode.RUNTIME_UNSUPPORTED.notification(graphdb.InputPosition.empty)
    case IndexHintUnfulfillableNotification(label, propertyKeys) =>
      NotificationCode.INDEX_HINT_UNFULFILLABLE.notification(graphdb.InputPosition.empty, NotificationDetail.Factory.index(label, propertyKeys: _*))
    case JoinHintUnfulfillableNotification(variables) =>
      NotificationCode.JOIN_HINT_UNFULFILLABLE.notification(graphdb.InputPosition.empty, NotificationDetail.Factory.joinKey(variables.asJava))
    case JoinHintUnsupportedNotification(variables) =>
      NotificationCode.JOIN_HINT_UNSUPPORTED.notification(graphdb.InputPosition.empty, NotificationDetail.Factory.joinKey(variables.asJava))
    case IndexLookupUnfulfillableNotification(labels) =>
      NotificationCode.INDEX_LOOKUP_FOR_DYNAMIC_PROPERTY.notification(graphdb.InputPosition.empty, NotificationDetail.Factory.indexSeekOrScan(labels.asJava))
    case EagerLoadCsvNotification =>
      NotificationCode.EAGER_LOAD_CSV.notification(graphdb.InputPosition.empty)
    case LargeLabelWithLoadCsvNotification =>
      NotificationCode.LARGE_LABEL_LOAD_CSV.notification(graphdb.InputPosition.empty)
    case MissingLabelNotification(pos, label) =>
      NotificationCode.MISSING_LABEL.notification(pos.asInputPosition, NotificationDetail.Factory.label(label))
    case MissingRelTypeNotification(pos, relType) =>
      NotificationCode.MISSING_REL_TYPE.notification(pos.asInputPosition, NotificationDetail.Factory.relationshipType(relType))
    case MissingPropertyNameNotification(pos, name) =>
      NotificationCode.MISSING_PROPERTY_NAME.notification(pos.asInputPosition, NotificationDetail.Factory.propertyName(name))
    case UnboundedShortestPathNotification(pos) =>
      NotificationCode.UNBOUNDED_SHORTEST_PATH.notification(pos.asInputPosition)
    case ExhaustiveShortestPathForbiddenNotification(pos) =>
      NotificationCode.EXHAUSTIVE_SHORTEST_PATH.notification(pos.asInputPosition)
    case DeprecatedFunctionNotification(pos, oldName, newName) =>
      NotificationCode.DEPRECATED_FUNCTION.notification(pos.asInputPosition, NotificationDetail.Factory.deprecatedName(oldName, newName))
    case DeprecatedProcedureNotification(pos, oldName, newName) =>
      NotificationCode.DEPRECATED_PROCEDURE.notification(pos.asInputPosition, NotificationDetail.Factory.deprecatedName(oldName, newName))
    case DeprecatedFieldNotification(pos, procedure, field) =>
      NotificationCode.DEPRECATED_PROCEDURE_RETURN_FIELD.notification(pos.asInputPosition, NotificationDetail.Factory.deprecatedField(procedure, field))
    case DeprecatedVarLengthBindingNotification(pos, variable) =>
      NotificationCode.DEPRECATED_BINDING_VAR_LENGTH_RELATIONSHIP.notification(pos.asInputPosition, NotificationDetail.Factory.bindingVarLengthRelationship(variable))
    case DeprecatedRelTypeSeparatorNotification(pos) =>
      NotificationCode.DEPRECATED_RELATIONSHIP_TYPE_SEPARATOR.notification(pos.asInputPosition)
    case DeprecatedPlannerNotification =>
      NotificationCode.DEPRECATED_PLANNER.notification(graphdb.InputPosition.empty)
    case ProcedureWarningNotification(pos, name, warning) =>
      NotificationCode.PROCEDURE_WARNING.notification(pos.asInputPosition, NotificationDetail.Factory.procedureWarning(name, warning))
  }

  override def accept[EX <: Exception](visitor: ResultVisitor[EX]) = inner.accept(wrapVisitor(visitor))

  private def wrapVisitor[EX <: Exception](visitor: ResultVisitor[EX]) = new InternalResultVisitor[EX] {
    override def visit(row: InternalResultRow) = visitor.visit(unwrapResultRow(row))
  }

  private def unwrapResultRow(row: InternalResultRow): ResultRow = new ResultRow {
    override def getRelationship(key: String): graphdb.Relationship = row.getRelationship(key)
    override def get(key: String): AnyRef = row.get(key)
    override def getBoolean(key: String): java.lang.Boolean = row.getBoolean(key)
    override def getPath(key: String): graphdb.Path = row.getPath(key)
    override def getNode(key: String): graphdb.Node = row.getNode(key)
    override def getNumber(key: String): Number = row.getNumber(key)
    override def getString(key: String): String = row.getString(key)
  }

  private implicit class ConvertibleCompilerInputPosition(pos: frontend.v3_3.InputPosition) {
    def asInputPosition = new graphdb.InputPosition(pos.offset, pos.line, pos.column)
  }
}
