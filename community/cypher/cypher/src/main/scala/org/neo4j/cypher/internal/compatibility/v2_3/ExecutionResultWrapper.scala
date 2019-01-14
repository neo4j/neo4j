/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
import java.util

import org.neo4j.cypher.internal
import org.neo4j.cypher.internal.compatibility._
import org.neo4j.cypher.internal.compatibility.v2_3.ExecutionResultWrapper.asKernelNotification
import org.neo4j.cypher.internal.compiler.v2_3.executionplan.InternalExecutionResult
import org.neo4j.cypher.internal.compiler.v2_3.{PlannerName, planDescription => planDescriptionv2_3, _}
import org.neo4j.cypher.internal.frontend.v2_3
import org.neo4j.cypher.internal.frontend.v2_3.notification.{InternalNotification, LegacyPlannerNotification, PlannerUnsupportedNotification, RuntimeUnsupportedNotification, _}
import org.neo4j.cypher.internal.frontend.v2_3.{InputPosition => InternalInputPosition, SemanticDirection => SemanticDirection2_3, notification => notification_2_3}
import org.neo4j.cypher.internal.runtime.planDescription.InternalPlanDescription.Arguments
import org.neo4j.cypher.internal.runtime.planDescription.InternalPlanDescription.Arguments._
import org.neo4j.cypher.internal.runtime.planDescription.{Children, NoChildren, PlanDescriptionImpl, SingleChild, TwoChildren, Argument => Argument3_4, InternalPlanDescription => InternalPlanDescription3_4}
import org.neo4j.cypher.internal.runtime.{QueryStatistics, SCHEMA_WRITE, ExecutionMode => ExecutionModeV3_4, _}
import org.neo4j.cypher.internal.util.v3_4.attribution.Id
import org.neo4j.cypher.internal.v3_4.expressions.SemanticDirection.{BOTH, INCOMING, OUTGOING}
import org.neo4j.cypher.result.QueryResult
import org.neo4j.cypher.result.QueryResult.Record
import org.neo4j.graphdb.Result.ResultVisitor
import org.neo4j.graphdb._
import org.neo4j.graphdb.impl.notification.{NotificationCode, NotificationDetail}
import org.neo4j.kernel.impl.util.ValueUtils
import org.neo4j.values.AnyValue

import scala.collection.JavaConverters._

object ExecutionResultWrapper {

  def unapply(v: Any): Option[(InternalExecutionResult, PlannerName, RuntimeName, Set[org.neo4j.graphdb.Notification], Option[v2_3.InputPosition])] = v match {
    case closing: ClosingExecutionResult => unapply(closing.inner)
    case wrapper: ExecutionResultWrapper => Some((wrapper.inner, wrapper.planner, wrapper.runtime, wrapper.preParsingNotifications, wrapper.offset))
    case _ => None
  }

   def asKernelNotification(offset : Option[v2_3.InputPosition])(notification: InternalNotification): org.neo4j.graphdb.Notification = notification match {
    case CartesianProductNotification(pos, variables) =>
      NotificationCode.CARTESIAN_PRODUCT.notification(pos.withOffset(offset).asInputPosition, NotificationDetail.Factory.cartesianProduct(variables.asJava))
    case LegacyPlannerNotification =>
      NotificationCode.LEGACY_PLANNER.notification(InputPosition.empty)
    case LengthOnNonPathNotification(pos) =>
      NotificationCode.LENGTH_ON_NON_PATH.notification(pos.withOffset(offset).asInputPosition)
    case PlannerUnsupportedNotification =>
      NotificationCode.PLANNER_UNSUPPORTED.notification(InputPosition.empty)
    case RuntimeUnsupportedNotification =>
      NotificationCode.RUNTIME_UNSUPPORTED.notification(InputPosition.empty)
    case IndexHintUnfulfillableNotification(label, propertyKey) =>
      NotificationCode.INDEX_HINT_UNFULFILLABLE
        .notification(InputPosition.empty, NotificationDetail.Factory.index(label, propertyKey))
    case JoinHintUnfulfillableNotification(variables) =>
      NotificationCode.JOIN_HINT_UNFULFILLABLE
        .notification(InputPosition.empty, NotificationDetail.Factory.joinKey(variables.asJava))
    case JoinHintUnsupportedNotification(variables) =>
      NotificationCode.JOIN_HINT_UNSUPPORTED
        .notification(InputPosition.empty, NotificationDetail.Factory.joinKey(variables.asJava))
    case IndexLookupUnfulfillableNotification(labels) =>
      NotificationCode.INDEX_LOOKUP_FOR_DYNAMIC_PROPERTY
        .notification(InputPosition.empty, NotificationDetail.Factory.indexSeekOrScan(labels.asJava))
    case BareNodeSyntaxDeprecatedNotification(pos) =>
      NotificationCode.BARE_NODE_SYNTAX_DEPRECATED.notification(pos.withOffset(offset).asInputPosition)
    case EagerLoadCsvNotification =>
      NotificationCode.EAGER_LOAD_CSV.notification(InputPosition.empty)
    case LargeLabelWithLoadCsvNotification =>
      NotificationCode.LARGE_LABEL_LOAD_CSV.notification(InputPosition.empty)
    case MissingLabelNotification(pos, label) =>
      NotificationCode.MISSING_LABEL.notification(pos.withOffset(offset).asInputPosition, NotificationDetail.Factory.label(label))
    case MissingRelTypeNotification(pos, relType) =>
      NotificationCode.MISSING_REL_TYPE.notification(pos.withOffset(offset).asInputPosition, NotificationDetail.Factory.relationshipType(relType))
    case MissingPropertyNameNotification(pos, name) =>
      NotificationCode.MISSING_PROPERTY_NAME.notification(pos.withOffset(offset).asInputPosition, NotificationDetail.Factory.propertyName(name))
    case UnboundedShortestPathNotification(pos) =>
      NotificationCode.UNBOUNDED_SHORTEST_PATH.notification(pos.withOffset(offset).asInputPosition)
  }

  private implicit class ConvertibleCompilerInputPosition(pos: InternalInputPosition) {

    def asInputPosition = new InputPosition(pos.offset, pos.line, pos.column)
  }

}

class ExecutionResultWrapper(val inner: InternalExecutionResult, val planner: PlannerName, val runtime: RuntimeName,
                             val preParsingNotifications: Set[org.neo4j.graphdb.Notification],
                             val offset : Option[v2_3.InputPosition])
  extends internal.runtime.InternalExecutionResult {

  override def planDescriptionRequested: Boolean = inner.planDescriptionRequested

  override def javaIterator: ResourceIterator[util.Map[String, Any]] = inner.javaIterator

  override def columnAs[T](column: String): Iterator[Nothing] = inner.columnAs(column)

  def queryStatistics(): QueryStatistics = {
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
                    uniqueConstraintsAdded = i.uniqueConstraintsAdded,
                    uniqueConstraintsRemoved = i.uniqueConstraintsRemoved,
                    existenceConstraintsAdded = i.existenceConstraintsAdded,
                    existenceConstraintsRemoved = i.existenceConstraintsRemoved
    )
  }

  override def dumpToString(writer: PrintWriter): Unit = inner.dumpToString(writer)

  override def dumpToString(): String = inner.dumpToString()

  override def javaColumnAs[T](column: String): ResourceIterator[T] = inner.javaColumnAs(column)

  override def executionPlanDescription(): InternalPlanDescription3_4 =
    lift(inner.executionPlanDescription()).addArgument(Version("CYPHER 2.3")).
      addArgument(Planner(planner.toTextOutput)).
      addArgument(PlannerImpl(planner.name)).
      addArgument(PlannerVersion("2.3")).
      addArgument(Runtime(runtime.toTextOutput)).
      addArgument(RuntimeImpl(runtime.name)).
      addArgument(RuntimeVersion("2.3"))

  private def lift(planDescription: planDescriptionv2_3.InternalPlanDescription): InternalPlanDescription3_4 = {
    val name: String = planDescription.name
    val children: Children = planDescription.children match {
      case planDescriptionv2_3.NoChildren => NoChildren
      case planDescriptionv2_3.SingleChild(child) => SingleChild(lift(child))
      case planDescriptionv2_3.TwoChildren(left, right) => TwoChildren(lift(left), lift(right))
    }

    val arguments: Seq[Argument3_4] = planDescription.arguments.map {
      case planDescriptionv2_3.InternalPlanDescription.Arguments.Time(value) => Arguments.Time(value)
      case planDescriptionv2_3.InternalPlanDescription.Arguments.Rows(value) => Arguments.Rows(value)
      case planDescriptionv2_3.InternalPlanDescription.Arguments.DbHits(value) => Arguments.DbHits(value)
      case planDescriptionv2_3.InternalPlanDescription.Arguments.ColumnsLeft(value) => Arguments.ColumnsLeft(value)
      case planDescriptionv2_3.InternalPlanDescription.Arguments.Expression(_) => Arguments.Expression(null)
      case planDescriptionv2_3.InternalPlanDescription.Arguments.LegacyExpression(_) => Arguments.Expression(null)
      case planDescriptionv2_3.InternalPlanDescription.Arguments.UpdateActionName(value) => Arguments
        .UpdateActionName(value)
      case planDescriptionv2_3.InternalPlanDescription.Arguments.MergePattern(startPoint) => Arguments
        .MergePattern(startPoint)
      case planDescriptionv2_3.InternalPlanDescription.Arguments.LegacyIndex(value) => Arguments.ExplicitIndex(value)
      case planDescriptionv2_3.InternalPlanDescription.Arguments.Index(label, propertyKey) => Arguments
        .Index(label, Seq(propertyKey))
      case planDescriptionv2_3.InternalPlanDescription.Arguments.PrefixIndex(label, propertyKey, _) => Arguments
        .PrefixIndex(label, propertyKey, null)
      case planDescriptionv2_3.InternalPlanDescription.Arguments.InequalityIndex(label, propertyKey,
      bounds) => Arguments
        .InequalityIndex(label, propertyKey, bounds)
      case planDescriptionv2_3.InternalPlanDescription.Arguments.LabelName(label) => Arguments.LabelName(label)
      case planDescriptionv2_3.InternalPlanDescription.Arguments.KeyNames(keys) => Arguments.KeyNames(keys)
      case planDescriptionv2_3.InternalPlanDescription.Arguments.KeyExpressions(_) => Arguments.KeyExpressions(null)
      case planDescriptionv2_3.InternalPlanDescription.Arguments.EntityByIdRhs(_) => Arguments.EntityByIdRhs(null)
      case planDescriptionv2_3.InternalPlanDescription.Arguments.EstimatedRows(value) => Arguments
        .EstimatedRows(value)
      case planDescriptionv2_3.InternalPlanDescription.Arguments.Version(value) => Arguments.Version(value)
      case planDescriptionv2_3.InternalPlanDescription.Arguments.Planner(value) => Arguments.Planner(value)
      case planDescriptionv2_3.InternalPlanDescription.Arguments.PlannerImpl(value) => Arguments.PlannerImpl(value)
      case planDescriptionv2_3.InternalPlanDescription.Arguments.Runtime(value) => Arguments.Runtime(value)
      case planDescriptionv2_3.InternalPlanDescription.Arguments.RuntimeImpl(value) => Arguments.RuntimeImpl(value)
      case planDescriptionv2_3.InternalPlanDescription.Arguments.ExpandExpression(from, relName, relTypes, to,
      direction, varLength) =>
        val (min, max) = if (varLength) (1, None) else (1, Some(1))
        val dir = direction match {
          case SemanticDirection2_3.OUTGOING => OUTGOING
          case SemanticDirection2_3.INCOMING => INCOMING
          case SemanticDirection2_3.BOTH => BOTH
        }
        Arguments.ExpandExpression(from, relName, relTypes, to, dir, min, max)
      case planDescriptionv2_3.InternalPlanDescription.Arguments.SourceCode(className, sourceCode) =>
        Arguments.SourceCode(className, sourceCode)
    }
    PlanDescriptionImpl(Id.INVALID_ID, name, children, arguments, planDescription.identifiers)
  }

  override def hasNext: Boolean = inner.hasNext

  override def next(): Map[String, Any] = inner.next()

  override def close(): Unit = inner.close()

  def queryType: InternalQueryType = inner.executionType.queryType() match {
    case QueryExecutionType.QueryType.READ_ONLY => READ_ONLY
    case QueryExecutionType.QueryType.WRITE => WRITE
    case QueryExecutionType.QueryType.READ_WRITE => READ_WRITE
    case QueryExecutionType.QueryType.SCHEMA_WRITE => SCHEMA_WRITE
  }

  def notifications: Iterable[Notification] = inner.notifications.map(asKernelNotification(offset)) ++ preParsingNotifications

  override def accept[EX <: Exception](visitor: ResultVisitor[EX]): Unit = inner.accept(visitor)

  override def executionMode: ExecutionModeV3_4 = {
    val et = inner.executionType
    if (et.isExplained) internal.runtime.ExplainMode
    else if (et.isProfiled) internal.runtime.ProfileMode
    else internal.runtime.NormalMode
  }

  override def withNotifications(notification: Notification*): internal.runtime.InternalExecutionResult =
    new ExecutionResultWrapper(inner, planner, runtime, preParsingNotifications ++ notification, offset)

  override def fieldNames(): Array[String] = inner.columns.toArray

  override def accept[E <: Exception](visitor: QueryResult.QueryResultVisitor[E]): Unit =
    inner.accept(new ResultVisitor[E] {
      override def visit(row: Result.ResultRow): Boolean = visitor.visit(new Record {
        override def fields(): Array[AnyValue] = fieldNames().map(k => ValueUtils.of(row.get(k)))
      })
    })
}
