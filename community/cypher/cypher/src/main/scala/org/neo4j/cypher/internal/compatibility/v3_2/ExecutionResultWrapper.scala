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
package org.neo4j.cypher.internal.compatibility.v3_2

import java.io.PrintWriter
import java.util

import org.neo4j.cypher._
import org.neo4j.cypher.internal._
import org.neo4j.cypher.internal.compatibility._
import org.neo4j.cypher.internal.compatibility.v3_2.ExecutionResultWrapper.asKernelNotification
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.planDescription.LegacyPlanDescription
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.planDescription.{Argument => Argument3_3}
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.planDescription.{
  InternalPlanDescription => InternalPlanDescription3_3
}
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.ExecutionMode
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.ExplainMode
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.NormalMode
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.ProfileMode
import org.neo4j.cypher.internal.compiler.v3_2.executionplan.InternalExecutionResult
import org.neo4j.cypher.internal.compiler.v3_2.executionplan._
import org.neo4j.cypher.internal.compiler.v3_2.planDescription.InternalPlanDescription.Arguments
import org.neo4j.cypher.internal.compiler.v3_2.planDescription.InternalPlanDescription.Arguments._
import org.neo4j.cypher.internal.compiler.v3_2.planDescription.Argument
import org.neo4j.cypher.internal.compiler.v3_2.planDescription.InternalPlanDescription
import org.neo4j.cypher.internal.compiler.v3_2.spi.InternalResultRow
import org.neo4j.cypher.internal.compiler.v3_2.spi.InternalResultVisitor
import org.neo4j.cypher.internal.compiler.v3_2.RuntimeName
import org.neo4j.cypher.internal.compiler.v3_2.{ExplainMode => ExplainModev3_2}
import org.neo4j.cypher.internal.compiler.v3_2.{NormalMode => NormalModev3_2}
import org.neo4j.cypher.internal.compiler.v3_2.{ProfileMode => ProfileModev3_2}
import org.neo4j.cypher.internal.frontend.v3_2.PlannerName
import org.neo4j.cypher.internal.frontend.v3_2.SemanticDirection.BOTH
import org.neo4j.cypher.internal.frontend.v3_2.SemanticDirection.INCOMING
import org.neo4j.cypher.internal.frontend.v3_2.SemanticDirection.OUTGOING
import org.neo4j.cypher.internal.frontend.v3_2.notification.DeprecatedPlannerNotification
import org.neo4j.cypher.internal.frontend.v3_2.notification.InternalNotification
import org.neo4j.cypher.internal.frontend.v3_2.notification.PlannerUnsupportedNotification
import org.neo4j.cypher.internal.frontend.v3_2.notification.RuntimeUnsupportedNotification
import org.neo4j.cypher.internal.frontend.v3_2.notification._
import org.neo4j.cypher.internal.frontend.v3_3
import org.neo4j.graphdb
import org.neo4j.graphdb.Result.ResultRow
import org.neo4j.graphdb.Result.ResultVisitor
import org.neo4j.graphdb.impl.notification.NotificationCode
import org.neo4j.graphdb.impl.notification.NotificationDetail
import org.neo4j.graphdb.Notification
import org.neo4j.graphdb.ResourceIterator
import org.neo4j.values.result.QueryResult
import org.neo4j.values.result.QueryResult.Record
import org.neo4j.values.AnyValue
import org.neo4j.values.AnyValues

import scala.collection.JavaConverters._

object ExecutionResultWrapper {

  def unapply(v: Any): Option[(InternalExecutionResult, PlannerName, RuntimeName)] = v match {
    case closing: ClosingExecutionResult => unapply(closing.inner)
    case wrapper: ExecutionResultWrapper => Some((wrapper.inner, wrapper.planner, wrapper.runtime))
    case _                               => None
  }

  def asKernelNotification(offset: Option[frontend.v3_2.InputPosition])(
      notification: InternalNotification): org.neo4j.graphdb.Notification = notification match {
    case CartesianProductNotification(pos, variables) =>
      NotificationCode.CARTESIAN_PRODUCT.notification(pos.withOffset(offset).asInputPosition,
                                                      NotificationDetail.Factory.cartesianProduct(variables.asJava))
    case LengthOnNonPathNotification(pos) =>
      NotificationCode.LENGTH_ON_NON_PATH.notification(pos.withOffset(offset).asInputPosition)
    case PlannerUnsupportedNotification =>
      NotificationCode.PLANNER_UNSUPPORTED.notification(graphdb.InputPosition.empty)
    case RuntimeUnsupportedNotification =>
      NotificationCode.RUNTIME_UNSUPPORTED.notification(graphdb.InputPosition.empty)
    case IndexHintUnfulfillableNotification(label, propertyKeys) =>
      NotificationCode.INDEX_HINT_UNFULFILLABLE
        .notification(graphdb.InputPosition.empty, NotificationDetail.Factory.index(label, propertyKeys: _*))
    case JoinHintUnfulfillableNotification(variables) =>
      NotificationCode.JOIN_HINT_UNFULFILLABLE
        .notification(graphdb.InputPosition.empty, NotificationDetail.Factory.joinKey(variables.asJava))
    case JoinHintUnsupportedNotification(variables) =>
      NotificationCode.JOIN_HINT_UNSUPPORTED
        .notification(graphdb.InputPosition.empty, NotificationDetail.Factory.joinKey(variables.asJava))
    case IndexLookupUnfulfillableNotification(labels) =>
      NotificationCode.INDEX_LOOKUP_FOR_DYNAMIC_PROPERTY
        .notification(graphdb.InputPosition.empty, NotificationDetail.Factory.indexSeekOrScan(labels.asJava))
    case EagerLoadCsvNotification =>
      NotificationCode.EAGER_LOAD_CSV.notification(graphdb.InputPosition.empty)
    case LargeLabelWithLoadCsvNotification =>
      NotificationCode.LARGE_LABEL_LOAD_CSV.notification(graphdb.InputPosition.empty)
    case MissingLabelNotification(pos, label) =>
      NotificationCode.MISSING_LABEL
        .notification(pos.withOffset(offset).asInputPosition, NotificationDetail.Factory.label(label))
    case MissingRelTypeNotification(pos, relType) =>
      NotificationCode.MISSING_REL_TYPE
        .notification(pos.withOffset(offset).asInputPosition, NotificationDetail.Factory.relationshipType(relType))
    case MissingPropertyNameNotification(pos, name) =>
      NotificationCode.MISSING_PROPERTY_NAME
        .notification(pos.withOffset(offset).asInputPosition, NotificationDetail.Factory.propertyName(name))
    case UnboundedShortestPathNotification(pos) =>
      NotificationCode.UNBOUNDED_SHORTEST_PATH.notification(pos.withOffset(offset).asInputPosition)
    case ExhaustiveShortestPathForbiddenNotification(pos) =>
      NotificationCode.EXHAUSTIVE_SHORTEST_PATH.notification(pos.withOffset(offset).asInputPosition)
    case DeprecatedFunctionNotification(pos, oldName, newName) =>
      NotificationCode.DEPRECATED_FUNCTION.notification(pos.withOffset(offset).asInputPosition,
                                                        NotificationDetail.Factory.deprecatedName(oldName, newName))
    case DeprecatedProcedureNotification(pos, oldName, newName) =>
      NotificationCode.DEPRECATED_PROCEDURE.notification(pos.withOffset(offset).asInputPosition,
                                                         NotificationDetail.Factory.deprecatedName(oldName, newName))
    case DeprecatedFieldNotification(pos, procedure, field) =>
      NotificationCode.DEPRECATED_PROCEDURE_RETURN_FIELD.notification(
        pos.withOffset(offset).asInputPosition,
        NotificationDetail.Factory.deprecatedField(procedure, field))
    case DeprecatedVarLengthBindingNotification(pos, variable) =>
      NotificationCode.DEPRECATED_BINDING_VAR_LENGTH_RELATIONSHIP.notification(
        pos.withOffset(offset).asInputPosition,
        NotificationDetail.Factory.bindingVarLengthRelationship(variable))

    case DeprecatedRelTypeSeparatorNotification(pos) =>
      NotificationCode.DEPRECATED_RELATIONSHIP_TYPE_SEPARATOR.notification(pos.withOffset(offset).asInputPosition)
    case DeprecatedPlannerNotification =>
      NotificationCode.DEPRECATED_PLANNER.notification(graphdb.InputPosition.empty)
    case ProcedureWarningNotification(pos, name, warning) =>
      NotificationCode.PROCEDURE_WARNING.notification(pos.withOffset(offset).asInputPosition,
                                                      NotificationDetail.Factory.procedureWarning(name, warning))
  }

  private implicit class ConvertibleCompilerInputPosition(pos: frontend.v3_2.InputPosition) {
    def asInputPosition = new graphdb.InputPosition(pos.offset, pos.line, pos.column)
  }
}

class ExecutionResultWrapper(val inner: InternalExecutionResult,
                             val planner: PlannerName,
                             val runtime: RuntimeName,
                             preParsingNotifications: Set[org.neo4j.graphdb.Notification],
                             offset: Option[frontend.v3_2.InputPosition])
    extends org.neo4j.cypher.internal.InternalExecutionResult {

  override def planDescriptionRequested: Boolean = inner.planDescriptionRequested

  override def javaIterator: ResourceIterator[util.Map[String, Any]] = inner.javaIterator

  override def columnAs[T](column: String): Iterator[Nothing] = inner.columnAs(column)

  override def queryStatistics(): QueryStatistics = {
    val i = inner.queryStatistics()
    QueryStatistics(
      nodesCreated = i.nodesCreated,
      relationshipsCreated = i.relationshipsCreated,
      propertiesSet = i.propertiesSet,
      nodesDeleted = i.nodesDeleted,
      relationshipsDeleted = i.relationshipsDeleted,
      labelsAdded = i.labelsAdded,
      labelsRemoved = i.labelsRemoved,
      indexesAdded = i.indexesAdded,
      indexesRemoved = i.indexesRemoved,
      uniqueConstraintsAdded = i.uniqueConstraintsAdded,
      uniqueConstraintsRemoved = i.uniqueConstraintsRemoved,
      existenceConstraintsAdded = i.existenceConstraintsAdded,
      existenceConstraintsRemoved = i.existenceConstraintsRemoved
    )
  }

  override def dumpToString(writer: PrintWriter): Unit = inner.dumpToString(writer)

  override def dumpToString(): String = inner.dumpToString()

  override def javaColumnAs[T](column: String): ResourceIterator[T] = inner.javaColumnAs(column)

  override def executionPlanDescription(): InternalPlanDescription3_3 =
    convert(
      inner
        .executionPlanDescription()
        .addArgument(Version("CYPHER 3.2"))
        .addArgument(Planner(planner.toTextOutput))
        .addArgument(PlannerImpl(planner.name))
        .addArgument(Runtime(runtime.toTextOutput))
        .addArgument(RuntimeImpl(runtime.name))
    )

  private def convert(i: InternalPlanDescription): InternalPlanDescription3_3 = exceptionHandler.runSafely {
    LegacyPlanDescription(i.name, convert(i.arguments), Set.empty, i.toString)
  }

  private def convert(args: Seq[Argument]): Seq[Argument3_3] = args.collect {
    case Arguments.LabelName(label)     => InternalPlanDescription3_3.Arguments.LabelName(label)
    case Arguments.ColumnsLeft(value)   => InternalPlanDescription3_3.Arguments.ColumnsLeft(value)
    case Arguments.DbHits(value)        => InternalPlanDescription3_3.Arguments.DbHits(value)
    case Arguments.EstimatedRows(value) => InternalPlanDescription3_3.Arguments.EstimatedRows(value)
    case Arguments.ExpandExpression(from, relName, relTypes, to, direction, min, max) =>
      val dir3_3 = direction match {
        case INCOMING => v3_3.SemanticDirection.INCOMING
        case OUTGOING => v3_3.SemanticDirection.OUTGOING
        case BOTH     => v3_3.SemanticDirection.BOTH
      }
      InternalPlanDescription3_3.Arguments.ExpandExpression(from, relName, relTypes, to, dir3_3, min, max)

    case Arguments.Index(label, propertyKey) => InternalPlanDescription3_3.Arguments.Index(label, propertyKey)
    case Arguments.LegacyIndex(value)        => InternalPlanDescription3_3.Arguments.LegacyIndex(value)
    case Arguments.InequalityIndex(label, propertyKey, bounds) =>
      InternalPlanDescription3_3.Arguments.InequalityIndex(label, propertyKey, bounds)
    case Arguments.Planner(value)      => InternalPlanDescription3_3.Arguments.Planner(value)
    case Arguments.PlannerImpl(value)  => InternalPlanDescription3_3.Arguments.PlannerImpl(value)
    case Arguments.Runtime(value)      => InternalPlanDescription3_3.Arguments.Runtime(value)
    case Arguments.RuntimeImpl(value)  => InternalPlanDescription3_3.Arguments.RuntimeImpl(value)
    case Arguments.KeyNames(keys)      => InternalPlanDescription3_3.Arguments.KeyNames(keys)
    case Arguments.MergePattern(start) => InternalPlanDescription3_3.Arguments.MergePattern(start)
    case Arguments.Version(value)      => InternalPlanDescription3_3.Arguments.Version(value)
  }
  override def hasNext: Boolean = inner.hasNext

  override def next(): Map[String, Any] = inner.next()

  override def close(): Unit = inner.close()

  override def queryType: compatibility.v3_3.runtime.executionplan.InternalQueryType = inner.executionType match {
    case READ_ONLY    => compatibility.v3_3.runtime.executionplan.READ_ONLY
    case READ_WRITE   => compatibility.v3_3.runtime.executionplan.READ_WRITE
    case WRITE        => compatibility.v3_3.runtime.executionplan.WRITE
    case SCHEMA_WRITE => compatibility.v3_3.runtime.executionplan.SCHEMA_WRITE
    case DBMS         => compatibility.v3_3.runtime.executionplan.DBMS
  }

  override def notifications: Iterable[Notification] =
    inner.notifications.map(asKernelNotification(offset)) ++ preParsingNotifications

  override def accept[EX <: Exception](visitor: ResultVisitor[EX]): Unit = inner.accept(wrapVisitor(visitor))

  private def wrapVisitor[EX <: Exception](visitor: ResultVisitor[EX]) = new InternalResultVisitor[EX] {
    override def visit(row: InternalResultRow): Boolean = visitor.visit(unwrapResultRow(row))
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

  override def executionMode: ExecutionMode = inner.executionMode match {
    case ExplainModev3_2 => ExplainMode
    case ProfileModev3_2 => ProfileMode
    case NormalModev3_2  => NormalMode
  }

  override def withNotifications(notification: Notification*): internal.InternalExecutionResult =
    new ExecutionResultWrapper(inner, planner, runtime, preParsingNotifications ++ notification, offset)

  override def fieldNames(): Array[String] = inner.columns.toArray

  override def accept[E <: Exception](visitor: QueryResult.QueryResultVisitor[E]): Unit =
    inner.accept(new InternalResultVisitor[E] {
      override def visit(row: InternalResultRow): Boolean =
        visitor.visit(new Record {
          override def fields(): Array[AnyValue] = fieldNames().map(k => AnyValues.of(row.get(k)))
        })
    })
}
