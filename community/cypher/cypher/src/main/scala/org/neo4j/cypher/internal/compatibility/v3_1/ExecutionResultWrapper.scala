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
package org.neo4j.cypher.internal.compatibility.v3_1

import java.io.PrintWriter
import java.util

import org.neo4j.cypher.internal
import org.neo4j.cypher.internal._
import org.neo4j.cypher.internal.compatibility._
import org.neo4j.cypher.internal.compatibility.v3_1.ExecutionResultWrapper.asKernelNotification
import org.neo4j.cypher.internal.compiler.v3_1
import org.neo4j.cypher.internal.compiler.v3_1.executionplan.{InternalExecutionResult, _}
import org.neo4j.cypher.internal.compiler.v3_1.spi.{InternalResultRow, InternalResultVisitor}
import org.neo4j.cypher.internal.compiler.v3_1.{PlannerName, ExplainMode => ExplainModev3_1, NormalMode => NormalModev3_1, ProfileMode => ProfileModev3_1, _}
import org.neo4j.cypher.internal.frontend.v3_1.notification.{DeprecatedPlannerNotification, InternalNotification, PlannerUnsupportedNotification, RuntimeUnsupportedNotification, _}
import org.neo4j.cypher.internal.frontend.v3_1.{SemanticDirection => SemanticDirection3_1, symbols => symbols3_1}
import org.neo4j.cypher.internal.runtime.planDescription.InternalPlanDescription.Arguments
import org.neo4j.cypher.internal.runtime.planDescription.InternalPlanDescription.Arguments._
import org.neo4j.cypher.internal.runtime.planDescription.{Argument, Children, NoChildren, PlanDescriptionImpl, SingleChild, TwoChildren, InternalPlanDescription => InternalPlanDescription3_4}
import org.neo4j.cypher.internal.runtime.{ExplainMode, NormalMode, ProfileMode, QueryStatistics}
import org.neo4j.cypher.internal.util.v3_4.attribution.Id
import org.neo4j.cypher.internal.util.v3_4.{symbols => symbolsv3_4}
import org.neo4j.cypher.internal.v3_4.expressions.SemanticDirection.{BOTH, INCOMING, OUTGOING}
import org.neo4j.cypher.internal.v3_4.logical.plans.QualifiedName
import org.neo4j.cypher.result.QueryResult
import org.neo4j.cypher.result.QueryResult.Record
import org.neo4j.graphdb
import org.neo4j.graphdb.Result.{ResultRow, ResultVisitor}
import org.neo4j.graphdb.impl.notification.{NotificationCode, NotificationDetail}
import org.neo4j.graphdb.{InputPosition, Notification, ResourceIterator}
import org.neo4j.kernel.impl.util.ValueUtils
import org.neo4j.values.AnyValue

import scala.collection.JavaConverters._

class ExecutionResultWrapper(val inner: InternalExecutionResult, val planner: PlannerName, val runtime: RuntimeName,
                             val preParsingNotification: Set[org.neo4j.graphdb.Notification],
                             val offset : Option[frontend.v3_1.InputPosition])
  extends internal.runtime.InternalExecutionResult  {

  override def planDescriptionRequested: Boolean = inner.planDescriptionRequested
  override def javaIterator: ResourceIterator[util.Map[String, Any]] = inner.javaIterator
  override def columnAs[T](column: String): Iterator[Nothing] = inner.columnAs(column)

  override def queryStatistics(): QueryStatistics = {
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
    lift(inner.executionPlanDescription()).addArgument(Version("CYPHER 3.1")).
      addArgument(Planner(planner.toTextOutput)).
      addArgument(PlannerVersion("3.1")).
      addArgument(PlannerImpl(planner.name)).
      addArgument(Runtime(runtime.toTextOutput)).
      addArgument(RuntimeImpl(runtime.name)).
      addArgument(RuntimeVersion("3.1"))

  private def lift(planDescription: v3_1.planDescription.InternalPlanDescription): InternalPlanDescription3_4 = {

    import v3_1.planDescription.InternalPlanDescription.{Arguments => Arguments3_1}

    val name: String = planDescription.name
    val children: Children = planDescription.children match {
      case v3_1.planDescription.NoChildren => NoChildren
      case v3_1.planDescription.SingleChild(child) => SingleChild(lift(child))
      case v3_1.planDescription.TwoChildren(left, right) => TwoChildren(lift(left), lift(right))
    }

    val arguments: Seq[Argument] = planDescription.arguments.map {
      case Arguments3_1.Time(value) => Arguments.Time(value)
      case Arguments3_1.Rows(value) => Arguments.Rows(value)
      case Arguments3_1.DbHits(value) => Arguments.DbHits(value)
      case Arguments3_1.ColumnsLeft(value) => Arguments.ColumnsLeft(value)
      case Arguments3_1.Expression(_) => Arguments.Expression(null)
      case Arguments3_1.LegacyExpression(_) => Arguments.Expression(null)
      case Arguments3_1.UpdateActionName(value) => Arguments.UpdateActionName(value)
      case Arguments3_1.MergePattern(startPoint) => Arguments.MergePattern(startPoint)
      case Arguments3_1.LegacyIndex(value) => Arguments.ExplicitIndex(value)
      case Arguments3_1.Index(label, propertyKey) => Arguments.Index(label, Seq(propertyKey))
      case Arguments3_1.PrefixIndex(label, propertyKey, _) => Arguments.PrefixIndex(label, propertyKey, null)
      case Arguments3_1.InequalityIndex(label, propertyKey, bounds) => Arguments
        .InequalityIndex(label, propertyKey, bounds)
      case Arguments3_1.LabelName(label) => Arguments.LabelName(label)
      case Arguments3_1.KeyNames(keys) => Arguments.KeyNames(keys)
      case Arguments3_1.KeyExpressions(_) => Arguments.KeyExpressions(null)
      case Arguments3_1.EntityByIdRhs(_) => Arguments.EntityByIdRhs(null)
      case Arguments3_1.EstimatedRows(value) => Arguments.EstimatedRows(value)
      case Arguments3_1.Version(value) => Arguments.Version(value)
      case Arguments3_1.Planner(value) => Arguments.Planner(value)
      case Arguments3_1.PlannerImpl(value) => Arguments.PlannerImpl(value)
      case Arguments3_1.Runtime(value) => Arguments.Runtime(value)
      case Arguments3_1.RuntimeImpl(value) => Arguments.RuntimeImpl(value)
      case Arguments3_1.ExpandExpression(from, relName, relTypes, to, direction, min, max) =>
        val dir = direction match {
          case SemanticDirection3_1.OUTGOING => OUTGOING
          case SemanticDirection3_1.INCOMING => INCOMING
          case SemanticDirection3_1.BOTH => BOTH
        }
        Arguments.ExpandExpression(from, relName, relTypes, to, dir, min, max)
      case Arguments3_1.SourceCode(className, sourceCode) =>
        Arguments.SourceCode(className, sourceCode)
      case Arguments3_1.CountNodesExpression(ident, label) => Arguments
        .CountNodesExpression(ident, List(label.map(_.name)))
      case Arguments3_1.CountRelationshipsExpression(ident, startLabel, typeNames, endLabel) => Arguments
        .CountRelationshipsExpression(ident, startLabel.map(_.name), typeNames.names, endLabel.map(_.name))
      case Arguments3_1.LegacyExpressions(expressions) => Arguments.Expressions(
        expressions.mapValues(_ => null))
      case Arguments3_1.Signature(procedureName, _, results) =>
        val procName = QualifiedName(procedureName.namespace, procedureName.name)
        Arguments.Signature(procName, Seq.empty, results.map(pair => (pair._1, lift(pair._2))))
    }
    PlanDescriptionImpl(Id.INVALID_ID, name, children, arguments, planDescription.variables)
  }

  private def lift(cypherType: symbols3_1.CypherType): symbolsv3_4.CypherType = cypherType match {
    case symbols3_1.CTAny => symbolsv3_4.CTAny
    case symbols3_1.CTBoolean => symbolsv3_4.CTBoolean
    case symbols3_1.CTString => symbolsv3_4.CTString
    case symbols3_1.CTNumber => symbolsv3_4.CTNumber
    case symbols3_1.CTFloat => symbolsv3_4.CTFloat
    case symbols3_1.CTInteger => symbolsv3_4.CTInteger
    case symbols3_1.CTMap => symbolsv3_4.CTMap
    case symbols3_1.CTNode => symbolsv3_4.CTNode
    case symbols3_1.CTRelationship => symbolsv3_4.CTRelationship
    case symbols3_1.CTPoint => symbolsv3_4.CTPoint
    case symbols3_1.CTGeometry => symbolsv3_4.CTGeometry
    case symbols3_1.CTPath => symbolsv3_4.CTPath
    case symbols3_1.ListType(t) => symbolsv3_4.ListType(lift(t))
  }

  override def hasNext: Boolean = inner.hasNext
  override def next(): Map[String, Any] = inner.next()
  override def close(): Unit = inner.close()

  override def queryType: internal.runtime.InternalQueryType = inner.executionType match {
      case READ_ONLY => internal.runtime.READ_ONLY
      case READ_WRITE => internal.runtime.READ_WRITE
      case WRITE => internal.runtime.WRITE
      case SCHEMA_WRITE => internal.runtime.SCHEMA_WRITE
      case DBMS => internal.runtime.DBMS
  }

  override def notifications: Iterable[Notification] = inner.notifications.map(asKernelNotification(offset)) ++ preParsingNotification

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

  override def executionMode: internal.runtime.ExecutionMode = inner.executionMode match {
    case ExplainModev3_1 => ExplainMode
    case ProfileModev3_1 => ProfileMode
    case NormalModev3_1 => NormalMode
  }

  override def withNotifications(notification: Notification*): internal.runtime.InternalExecutionResult =
    new ExecutionResultWrapper(inner, planner, runtime, preParsingNotification ++ notification, offset)

  override def fieldNames(): Array[String] = inner.columns.toArray

  override def accept[E <: Exception](visitor: QueryResult.QueryResultVisitor[E]): Unit =
    inner.accept(new InternalResultVisitor[E] {
      override def visit(internalResultRow: InternalResultRow): Boolean = visitor.visit(new Record {
        override def fields(): Array[AnyValue] = fieldNames().map(k => ValueUtils.of(internalResultRow.get(k)))
      })
    })
}

object ExecutionResultWrapper {
  def unapply(v: Any): Option[(InternalExecutionResult, PlannerName, RuntimeName, Set[org.neo4j.graphdb.Notification], Option[frontend.v3_1.InputPosition])] = v match {
    case closing: ClosingExecutionResult => unapply(closing.inner)
    case wrapper: ExecutionResultWrapper => Some((wrapper.inner, wrapper.planner, wrapper.runtime, wrapper.preParsingNotification, wrapper.offset))
    case _ => None
  }

  def asKernelNotification(offset : Option[frontend.v3_1.InputPosition])(notification: InternalNotification): org.neo4j.graphdb.Notification = notification match {
    case CartesianProductNotification(pos, variables) =>
      NotificationCode.CARTESIAN_PRODUCT.notification(pos.withOffset(offset).asInputPosition, NotificationDetail.Factory.cartesianProduct(variables.asJava))
    case LengthOnNonPathNotification(pos) =>
      NotificationCode.LENGTH_ON_NON_PATH.notification(pos.withOffset(offset).asInputPosition)
    case PlannerUnsupportedNotification =>
      NotificationCode.PLANNER_UNSUPPORTED.notification(graphdb.InputPosition.empty)
    case RuntimeUnsupportedNotification =>
      NotificationCode.RUNTIME_UNSUPPORTED.notification(graphdb.InputPosition.empty)
    case IndexHintUnfulfillableNotification(label, propertyKey) =>
      NotificationCode.INDEX_HINT_UNFULFILLABLE.notification(graphdb.InputPosition.empty, NotificationDetail.Factory.index(label, propertyKey))
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
      NotificationCode.MISSING_LABEL.notification(pos.withOffset(offset).asInputPosition, NotificationDetail.Factory.label(label))
    case MissingRelTypeNotification(pos, relType) =>
      NotificationCode.MISSING_REL_TYPE.notification(pos.withOffset(offset).asInputPosition, NotificationDetail.Factory.relationshipType(relType))
    case MissingPropertyNameNotification(pos, name) =>
      NotificationCode.MISSING_PROPERTY_NAME.notification(pos.withOffset(offset).asInputPosition, NotificationDetail.Factory.propertyName(name))
    case UnboundedShortestPathNotification(pos) =>
      NotificationCode.UNBOUNDED_SHORTEST_PATH.notification(pos.withOffset(offset).asInputPosition)
    case ExhaustiveShortestPathForbiddenNotification(pos) =>
      NotificationCode.EXHAUSTIVE_SHORTEST_PATH.notification(pos.withOffset(offset).asInputPosition)
    case DeprecatedFunctionNotification(pos, oldName, newName) =>
      NotificationCode.DEPRECATED_FUNCTION.notification(pos.withOffset(offset).asInputPosition, NotificationDetail.Factory.deprecatedName(oldName, newName))
    case DeprecatedProcedureNotification(pos, oldName, newName) =>
      NotificationCode.DEPRECATED_PROCEDURE.notification(pos.withOffset(offset).asInputPosition, NotificationDetail.Factory.deprecatedName(oldName, newName))
    case DeprecatedPlannerNotification =>
      NotificationCode.DEPRECATED_PLANNER.notification(graphdb.InputPosition.empty)
  }

  private implicit class ConvertibleCompilerInputPosition(pos: frontend.v3_1.InputPosition) {
    def asInputPosition: InputPosition = new graphdb.InputPosition(pos.offset, pos.line, pos.column)
  }
}
