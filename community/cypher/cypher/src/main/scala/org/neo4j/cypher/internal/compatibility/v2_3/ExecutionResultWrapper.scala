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
package org.neo4j.cypher.internal.compatibility.v2_3

import java.io.PrintWriter
import java.util

import org.neo4j.cypher._
import org.neo4j.cypher.internal.compatibility._
import org.neo4j.cypher.internal.compatibility.v2_3.ExecutionResultWrapper.asKernelNotification
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.executionplan._
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.planDescription.{LegacyPlanDescription, Argument => Argument3_3, InternalPlanDescription => InternalPlanDescription3_3}
import org.neo4j.cypher.internal.compiler.v2_3.executionplan.InternalExecutionResult
import org.neo4j.cypher.internal.compiler.v2_3.planDescription.InternalPlanDescription.Arguments
import org.neo4j.cypher.internal.compiler.v2_3.planDescription.InternalPlanDescription.Arguments._
import org.neo4j.cypher.internal.compiler.v2_3.planDescription.{Argument, InternalPlanDescription}
import org.neo4j.cypher.internal.compiler.v2_3.{PlannerName, _}
import org.neo4j.cypher.internal.frontend.v2_3.SemanticDirection.{BOTH, INCOMING, OUTGOING}
import org.neo4j.cypher.internal.frontend.v2_3.notification.{InternalNotification, LegacyPlannerNotification, PlannerUnsupportedNotification, RuntimeUnsupportedNotification, _}
import org.neo4j.cypher.internal.frontend.v2_3.{InputPosition => InternalInputPosition}
import org.neo4j.cypher.internal.frontend.{v2_3, v3_3}
import org.neo4j.cypher.internal.{QueryStatistics, compatibility}
import org.neo4j.cypher.result.QueryResult
import org.neo4j.cypher.result.QueryResult.Record
import org.neo4j.graphdb.Result.ResultVisitor
import org.neo4j.graphdb._
import org.neo4j.graphdb.impl.notification.{NotificationCode, NotificationDetail}
import org.neo4j.kernel.impl.util.ValueUtils
import org.neo4j.values.AnyValue

import scala.collection.JavaConverters._

object ExecutionResultWrapper {

  def unapply(v: Any): Option[(InternalExecutionResult, PlannerName, RuntimeName)] = v match {
    case closing: ClosingExecutionResult => unapply(closing.inner)
    case wrapper: ExecutionResultWrapper => Some((wrapper.inner, wrapper.planner, wrapper.runtime))
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
                             preParsingNotifications: Set[org.neo4j.graphdb.Notification],
                             offset : Option[v2_3.InputPosition])
  extends org.neo4j.cypher.internal.InternalExecutionResult {

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

  def executionPlanDescription(): InternalPlanDescription3_3 =
    convert(inner.executionPlanDescription().
      addArgument(Version("CYPHER 2.3")).
      addArgument(Planner(planner.toTextOutput)).
      addArgument(PlannerImpl(planner.name)).
      addArgument(Runtime(runtime.toTextOutput)).
      addArgument(RuntimeImpl(runtime.name)))

  private def convert(i: InternalPlanDescription): InternalPlanDescription3_3 = exceptionHandler.runSafely {
    LegacyPlanDescription(i.name, convert(i.arguments), Set.empty, i.toString)
  }

  private def convert(args: Seq[Argument]): Seq[Argument3_3] = args.collect {
    case Arguments.LabelName(label) => InternalPlanDescription3_3.Arguments.LabelName(label)
    case Arguments.ColumnsLeft(value) => InternalPlanDescription3_3.Arguments.ColumnsLeft(value)
    case Arguments.DbHits(value) => InternalPlanDescription3_3.Arguments.DbHits(value)
    case Arguments.EstimatedRows(value) => InternalPlanDescription3_3.Arguments.EstimatedRows(value)
    case Arguments.ExpandExpression(from, relName, relTypes, to, direction, varLength) =>
      val dir3_3 = direction match {
        case INCOMING => v3_3.SemanticDirection.INCOMING
        case OUTGOING => v3_3.SemanticDirection.OUTGOING
        case BOTH => v3_3.SemanticDirection.BOTH
      }
      InternalPlanDescription3_3.Arguments.ExpandExpression(from, relName, relTypes, to, dir3_3, 0, None)

    case Arguments.Index(label, propertyKey) => InternalPlanDescription3_3.Arguments.Index(label, Seq(propertyKey))
    case Arguments.LegacyIndex(value) => InternalPlanDescription3_3.Arguments.ExplicitIndex(value)
    case Arguments.InequalityIndex(label, propertyKey, bounds) => InternalPlanDescription3_3.Arguments
      .InequalityIndex(label, propertyKey, bounds)
    case Arguments.Planner(value) => InternalPlanDescription3_3.Arguments.Planner(value)
    case Arguments.PlannerImpl(value) => InternalPlanDescription3_3.Arguments.PlannerImpl(value)
    case Arguments.Runtime(value) => InternalPlanDescription3_3.Arguments.Runtime(value)
    case Arguments.RuntimeImpl(value) => InternalPlanDescription3_3.Arguments.RuntimeImpl(value)
    case Arguments.KeyNames(keys) => InternalPlanDescription3_3.Arguments.KeyNames(keys)
    case Arguments.MergePattern(start) => InternalPlanDescription3_3.Arguments.MergePattern(start)
    case Arguments.Version(value) => InternalPlanDescription3_3.Arguments.Version(value)
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

  override def executionMode: compatibility.v3_3.runtime.ExecutionMode = {
    val et = inner.executionType
    if (et.isExplained) compatibility.v3_3.runtime.ExplainMode
    else if (et.isProfiled) compatibility.v3_3.runtime.ProfileMode
    else compatibility.v3_3.runtime.NormalMode
  }

  override def withNotifications(notification: Notification*): internal.InternalExecutionResult =
    new ExecutionResultWrapper(inner, planner, runtime, preParsingNotifications ++ notification, offset)

  override def fieldNames(): Array[String] = inner.columns.toArray

  override def accept[E <: Exception](visitor: QueryResult.QueryResultVisitor[E]): Unit =
    inner.accept(new ResultVisitor[E] {
      override def visit(row: Result.ResultRow): Boolean = visitor.visit(new Record {
        override def fields(): Array[AnyValue] = fieldNames().map(k => ValueUtils.of(row.get(k)))
      })
    })
}
