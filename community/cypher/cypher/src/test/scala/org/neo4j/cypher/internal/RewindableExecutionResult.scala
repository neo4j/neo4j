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
import java.util

import org.neo4j.cypher.internal.compatibility.ClosingExecutionResult
import org.neo4j.cypher.internal.compatibility.v2_3.{ExecutionResultWrapper => ExecutionResultWrapperFor2_3, exceptionHandler => exceptionHandlerFor2_3}
import org.neo4j.cypher.internal.compatibility.v3_1.{ExecutionResultWrapper => ExecutionResultWrapperFor3_1, exceptionHandler => exceptionHandlerFor3_1}
import org.neo4j.cypher.internal.compatibility.v3_2.{ExecutionResultWrapper => ExecutionResultWrapperFor3_2, exceptionHandler => exceptionHandlerFor3_2}
import org.neo4j.cypher.internal.compatibility.v3_3.exceptionHandler
import org.neo4j.cypher.internal.compatibility.v3_3.runtime._
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.executionplan._
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.planDescription.InternalPlanDescription.Arguments
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.planDescription._
import org.neo4j.cypher.internal.compiler.v3_3.CostBasedPlannerName
import org.neo4j.cypher.internal.compiler.v3_3.spi.QualifiedName
import org.neo4j.cypher.internal.compiler.{v2_3, v3_1, v3_2}
import org.neo4j.cypher.internal.frontend.v2_3.{SemanticDirection => SemanticDirection2_3, notification => notification_2_3}
import org.neo4j.cypher.internal.frontend.v3_1.{SemanticDirection => SemanticDirection3_1, notification => notification_3_1, symbols => symbols3_1}
import org.neo4j.cypher.internal.frontend.v3_2.{SemanticDirection => SemanticDirection3_2, notification => notification_3_2, symbols => symbols3_2}
import org.neo4j.cypher.internal.frontend.v3_3.SemanticDirection.{BOTH, INCOMING, OUTGOING}
import org.neo4j.cypher.internal.frontend.v3_3.{InputPosition, symbols}
import org.neo4j.cypher.internal.javacompat.ExecutionResult
import org.neo4j.graphdb.Result.ResultVisitor
import org.neo4j.graphdb.{Notification, QueryExecutionType, ResourceIterator, Result}
import org.neo4j.values.result.QueryResult.QueryResultVisitor

object RewindableExecutionResult {

  private def current(inner: InternalExecutionResult): InternalExecutionResult =
    inner match {
      case other: PipeExecutionResult =>
        exceptionHandler.runSafely {
          new PipeExecutionResult(other.result.toEager, other.columns.toArray, other.state, other.executionPlanBuilder,
                                  other.executionMode, READ_WRITE)
        }
      case other: StandardInternalExecutionResult =>
        exceptionHandler.runSafely {
          other.toEagerResultForTestingOnly(CostBasedPlannerName.default)
        }

      case _ =>
        inner
    }

  private def compatibility(inner: v2_3.executionplan.InternalExecutionResult, planner: v2_3.PlannerName,
                            runtime: v2_3.RuntimeName): InternalExecutionResult = {
    val result: v2_3.executionplan.InternalExecutionResult = inner match {
      case other: v2_3.PipeExecutionResult =>
        exceptionHandlerFor2_3.runSafely {
          new v2_3.PipeExecutionResult(other.result.toEager, other.columns, other.state, other.executionPlanBuilder,
                                       other.executionMode, QueryExecutionType.QueryType.READ_WRITE) {
            override def executionPlanDescription(): v2_3.planDescription.InternalPlanDescription = super
              .executionPlanDescription()
              .addArgument(v2_3.planDescription.InternalPlanDescription.Arguments.Planner(planner.name))
              .addArgument(v2_3.planDescription.InternalPlanDescription.Arguments.Runtime(runtime.name))
          }
        }
      case _ =>
        inner
    }
    InternalExecutionResultCompatibilityWrapperFor2_3(result)
  }

  private def compatibility(inner: v3_1.executionplan.InternalExecutionResult, planner: v3_1.PlannerName,
                            runtime: v3_1.RuntimeName): InternalExecutionResult = {
    val result: v3_1.executionplan.InternalExecutionResult = inner match {
      case other: v3_1.PipeExecutionResult =>
        exceptionHandlerFor3_1.runSafely {
          new v3_1.PipeExecutionResult(other.result.toEager, other.columns, other.state, other.executionPlanBuilder,
                                       other.executionMode, v3_1.executionplan.READ_WRITE) {
            override def executionPlanDescription(): v3_1.planDescription.InternalPlanDescription = super
              .executionPlanDescription()
              .addArgument(v3_1.planDescription.InternalPlanDescription.Arguments.Planner(planner.name))
              .addArgument(v3_1.planDescription.InternalPlanDescription.Arguments.Runtime(runtime.name))
          }
        }
      case _ =>
        inner
    }
    InternalExecutionResultCompatibilityWrapperFor3_1(result)
  }

  private def compatibility(inner: v3_2.executionplan.InternalExecutionResult, planner: frontend.v3_2.PlannerName,
                            runtime: v3_2.RuntimeName): InternalExecutionResult = {
    val result: v3_2.executionplan.InternalExecutionResult = inner match {
      case other: v3_2.PipeExecutionResult =>
        exceptionHandlerFor3_2.runSafely {
          new v3_2.PipeExecutionResult(other.result.toEager, other.columns, other.state, other.executionPlanBuilder,
                                       other.executionMode, v3_2.executionplan.READ_WRITE) {
            override def executionPlanDescription(): v3_2.planDescription.InternalPlanDescription = super
              .executionPlanDescription()
              .addArgument(v3_2.planDescription.InternalPlanDescription.Arguments.Planner(planner.name))
              .addArgument(v3_2.planDescription.InternalPlanDescription.Arguments.Runtime(runtime.name))
          }
        }
      case _ =>
        inner
    }
    InternalExecutionResultCompatibilityWrapperFor3_2(result)
  }

  def apply(in: Result): InternalExecutionResult = {
    val internal = in.asInstanceOf[ExecutionResult].internalExecutionResult
      .asInstanceOf[ClosingExecutionResult].inner
    internal match {
      case ExecutionResultWrapperFor3_2(inner, planner, runtime) =>
        exceptionHandlerFor3_2.runSafely(compatibility(inner, planner, runtime))
      case ExecutionResultWrapperFor3_1(inner, planner, runtime) =>
        exceptionHandlerFor3_1.runSafely(compatibility(inner, planner, runtime))
      case ExecutionResultWrapperFor2_3(inner, planner, runtime) =>
        exceptionHandlerFor2_3.runSafely(compatibility(inner, planner, runtime))
      case _ => exceptionHandler.runSafely(current(internal))
    }
  }

  private case class InternalExecutionResultCompatibilityWrapperFor2_3(inner: v2_3.executionplan.InternalExecutionResult)
    extends InternalExecutionResult {

    private val cache = inner.toList

    override def toList: List[Map[String, Any]] = cache

    override def javaIterator: ResourceIterator[util.Map[String, Any]] = ???

    override def queryType: InternalQueryType = inner.executionType.queryType() match {
      case QueryExecutionType.QueryType.READ_ONLY => READ_ONLY
      case QueryExecutionType.QueryType.READ_WRITE => READ_WRITE
      case QueryExecutionType.QueryType.WRITE => WRITE
      case QueryExecutionType.QueryType.SCHEMA_WRITE => SCHEMA_WRITE
    }

    override def columnAs[T](column: String): Iterator[T] = inner.columnAs(column)

    override def fieldNames(): Array[String] = inner.columns.toArray

    override def queryStatistics(): QueryStatistics = {
      val stats = inner.queryStatistics()
      QueryStatistics(stats.nodesCreated, stats.relationshipsCreated, stats.propertiesSet,
                      stats.nodesDeleted, stats.relationshipsDeleted, stats.labelsAdded, stats.labelsRemoved,
                      stats.indexesAdded, stats.indexesRemoved, stats.uniqueConstraintsAdded,
                      stats.uniqueConstraintsRemoved,
                      stats.existenceConstraintsAdded, stats.existenceConstraintsRemoved)
    }

    override def executionMode: ExecutionMode = NormalMode

    override def dumpToString(writer: PrintWriter): Unit = inner.dumpToString(writer)

    override def dumpToString(): String = inner.dumpToString()

    @throws(classOf[Exception])
    override def accept[EX <: Exception](visitor: ResultVisitor[EX]): Unit = ???

    @throws(classOf[Exception])
    override def accept[EX <: Exception](visitor: QueryResultVisitor[EX]): Unit = ???

    override def javaColumnAs[T](column: String): ResourceIterator[T] = inner.javaColumnAs(column)

    override def executionPlanDescription(): InternalPlanDescription = lift(inner.executionPlanDescription())

    private def lift(planDescription: v2_3.planDescription.InternalPlanDescription): InternalPlanDescription = {
      val name: String = planDescription.name
      val children: Children = planDescription.children match {
        case v2_3.planDescription.NoChildren => NoChildren
        case v2_3.planDescription.SingleChild(child) => SingleChild(lift(child))
        case v2_3.planDescription.TwoChildren(left, right) => TwoChildren(lift(left), lift(right))
      }

      val arguments: Seq[Argument] = planDescription.arguments.map {
        case v2_3.planDescription.InternalPlanDescription.Arguments.Time(value) => Arguments.Time(value)
        case v2_3.planDescription.InternalPlanDescription.Arguments.Rows(value) => Arguments.Rows(value)
        case v2_3.planDescription.InternalPlanDescription.Arguments.DbHits(value) => Arguments.DbHits(value)
        case v2_3.planDescription.InternalPlanDescription.Arguments.ColumnsLeft(value) => Arguments.ColumnsLeft(value)
        case v2_3.planDescription.InternalPlanDescription.Arguments.Expression(_) => Arguments.Expression(null)
        case v2_3.planDescription.InternalPlanDescription.Arguments.LegacyExpression(_) => Arguments.Expression(null)
        case v2_3.planDescription.InternalPlanDescription.Arguments.UpdateActionName(value) => Arguments
          .UpdateActionName(value)
        case v2_3.planDescription.InternalPlanDescription.Arguments.MergePattern(startPoint) => Arguments
          .MergePattern(startPoint)
        case v2_3.planDescription.InternalPlanDescription.Arguments.LegacyIndex(value) => Arguments.LegacyIndex(value)
        case v2_3.planDescription.InternalPlanDescription.Arguments.Index(label, propertyKey) => Arguments
          .Index(label, Seq(propertyKey))
        case v2_3.planDescription.InternalPlanDescription.Arguments.PrefixIndex(label, propertyKey, _) => Arguments
          .PrefixIndex(label, propertyKey, null)
        case v2_3.planDescription.InternalPlanDescription.Arguments.InequalityIndex(label, propertyKey,
                                                                                    bounds) => Arguments
          .InequalityIndex(label, propertyKey, bounds)
        case v2_3.planDescription.InternalPlanDescription.Arguments.LabelName(label) => Arguments.LabelName(label)
        case v2_3.planDescription.InternalPlanDescription.Arguments.KeyNames(keys) => Arguments.KeyNames(keys)
        case v2_3.planDescription.InternalPlanDescription.Arguments.KeyExpressions(_) => Arguments.KeyExpressions(null)
        case v2_3.planDescription.InternalPlanDescription.Arguments.EntityByIdRhs(_) => Arguments.EntityByIdRhs(null)
        case v2_3.planDescription.InternalPlanDescription.Arguments.EstimatedRows(value) => Arguments
          .EstimatedRows(value)
        case v2_3.planDescription.InternalPlanDescription.Arguments.Version(value) => Arguments.Version(value)
        case v2_3.planDescription.InternalPlanDescription.Arguments.Planner(value) => Arguments.Planner(value)
        case v2_3.planDescription.InternalPlanDescription.Arguments.PlannerImpl(value) => Arguments.PlannerImpl(value)
        case v2_3.planDescription.InternalPlanDescription.Arguments.Runtime(value) => Arguments.Runtime(value)
        case v2_3.planDescription.InternalPlanDescription.Arguments.RuntimeImpl(value) => Arguments.RuntimeImpl(value)
        case v2_3.planDescription.InternalPlanDescription.Arguments.ExpandExpression(from, relName, relTypes, to,
                                                                                     direction, varLength) =>
          val (min, max) = if (varLength) (1, None) else (1, Some(1))
          val dir = direction match {
            case SemanticDirection2_3.OUTGOING => OUTGOING
            case SemanticDirection2_3.INCOMING => INCOMING
            case SemanticDirection2_3.BOTH => BOTH
          }
          Arguments.ExpandExpression(from, relName, relTypes, to, dir, min, max)
        case v2_3.planDescription.InternalPlanDescription.Arguments.SourceCode(className, sourceCode) =>
          Arguments.SourceCode(className, sourceCode)
      }
      PlanDescriptionImpl(new Id, name, children, arguments, planDescription.identifiers)
    }


    override def close(): Unit = inner.close()

    override def notifications: Iterable[Notification] =
      inner.notifications.map(org.neo4j.cypher.internal.compatibility.v2_3.ExecutionResultWrapper.asKernelNotification(None))

    private def lift(position: frontend.v2_3.InputPosition): InputPosition = {
      InputPosition.apply(position.offset, position.line, position.column)
    }

    override def planDescriptionRequested: Boolean = inner.planDescriptionRequested

    override def next(): Map[String, Any] = inner.next()

    override def hasNext: Boolean = inner.hasNext

    override def withNotifications(notification: Notification*): InternalExecutionResult = this
  }

  private case class InternalExecutionResultCompatibilityWrapperFor3_1(inner: v3_1.executionplan.InternalExecutionResult)
    extends InternalExecutionResult {

    private val cache = inner.toList

    override def toList: List[Map[String, Any]] = cache

    override def javaIterator: ResourceIterator[util.Map[String, Any]] = ???

    override def queryType: InternalQueryType = inner.executionType match {
      case v3_1.executionplan.READ_ONLY => READ_ONLY
      case v3_1.executionplan.READ_WRITE => READ_WRITE
      case v3_1.executionplan.WRITE => WRITE
      case v3_1.executionplan.SCHEMA_WRITE => SCHEMA_WRITE
      case v3_1.executionplan.DBMS => DBMS
    }

    override def columnAs[T](column: String): Iterator[T] = inner.columnAs(column)

    override def fieldNames(): Array[String] = inner.columns.toArray

    override def queryStatistics(): QueryStatistics = {
      val stats = inner.queryStatistics()
      QueryStatistics(stats.nodesCreated, stats.relationshipsCreated, stats.propertiesSet,
                      stats.nodesDeleted, stats.relationshipsDeleted, stats.labelsAdded, stats.labelsRemoved,
                      stats.indexesAdded, stats.indexesRemoved, stats.uniqueConstraintsAdded,
                      stats.uniqueConstraintsRemoved,
                      stats.existenceConstraintsAdded, stats.existenceConstraintsRemoved)
    }

    override def executionMode: ExecutionMode = NormalMode

    override def dumpToString(writer: PrintWriter): Unit = inner.dumpToString(writer)

    override def dumpToString(): String = inner.dumpToString()

    @throws(classOf[Exception])
    override def accept[EX <: Exception](visitor: ResultVisitor[EX]): Unit = ???

    @throws(classOf[Exception])
    override def accept[EX <: Exception](visitor: QueryResultVisitor[EX]): Unit = ???

    override def javaColumnAs[T](column: String): ResourceIterator[T] = inner.javaColumnAs(column)

    override def executionPlanDescription(): InternalPlanDescription = lift(inner.executionPlanDescription())

    private def lift(planDescription: v3_1.planDescription.InternalPlanDescription): InternalPlanDescription = {

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
        case Arguments3_1.LegacyIndex(value) => Arguments.LegacyIndex(value)
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
      PlanDescriptionImpl(new Id, name, children, arguments, planDescription.variables)
    }

    override def close(): Unit = inner.close()

    override def notifications: Iterable[Notification] = inner.notifications
      .map(org.neo4j.cypher.internal.compatibility.v3_1.ExecutionResultWrapper.asKernelNotification(None))

    private def lift(position: frontend.v3_1.InputPosition): InputPosition = {
      InputPosition.apply(position.offset, position.line, position.column)
    }

    private def lift(cypherType: symbols3_1.CypherType): symbols.CypherType = cypherType match {
      case symbols3_1.CTAny => symbols.CTAny
      case symbols3_1.CTBoolean => symbols.CTBoolean
      case symbols3_1.CTString => symbols.CTString
      case symbols3_1.CTNumber => symbols.CTNumber
      case symbols3_1.CTFloat => symbols.CTFloat
      case symbols3_1.CTInteger => symbols.CTInteger
      case symbols3_1.CTMap => symbols.CTMap
      case symbols3_1.CTNode => symbols.CTNode
      case symbols3_1.CTRelationship => symbols.CTRelationship
      case symbols3_1.CTPoint => symbols.CTPoint
      case symbols3_1.CTGeometry => symbols.CTGeometry
      case symbols3_1.CTPath => symbols.CTPath
      case symbols3_1.ListType(t) => symbols.ListType(lift(t))
    }

    override def planDescriptionRequested: Boolean = inner.planDescriptionRequested

    override def next(): Map[String, Any] = inner.next()

    override def hasNext: Boolean = inner.hasNext

    override def withNotifications(notification: Notification*): InternalExecutionResult = this
  }

  private case class InternalExecutionResultCompatibilityWrapperFor3_2(inner: v3_2.executionplan.InternalExecutionResult)
    extends InternalExecutionResult {

    override def javaIterator: ResourceIterator[util.Map[String, Any]] = inner.javaIterator

    override def queryType: InternalQueryType = inner.executionType match {
      case v3_2.executionplan.READ_ONLY => READ_ONLY
      case v3_2.executionplan.READ_WRITE => READ_WRITE
      case v3_2.executionplan.WRITE => WRITE
      case v3_2.executionplan.SCHEMA_WRITE => SCHEMA_WRITE
      case v3_2.executionplan.DBMS => DBMS
    }

    override def columnAs[T](column: String): Iterator[T] = inner.columnAs(column)

    override def fieldNames(): Array[String] = inner.columns.toArray

    override def queryStatistics(): QueryStatistics = {
      val stats = inner.queryStatistics()
      QueryStatistics(stats.nodesCreated, stats.relationshipsCreated, stats.propertiesSet,
                      stats.nodesDeleted, stats.relationshipsDeleted, stats.labelsAdded, stats.labelsRemoved,
                      stats.indexesAdded, stats.indexesRemoved, stats.uniqueConstraintsAdded,
                      stats.uniqueConstraintsRemoved,
                      stats.existenceConstraintsAdded, stats.existenceConstraintsRemoved)
    }

    override def executionMode: ExecutionMode = NormalMode

    override def dumpToString(writer: PrintWriter): Unit = inner.dumpToString(writer)

    override def dumpToString(): String = inner.dumpToString()

    @throws(classOf[Exception])
    override def accept[EX <: Exception](visitor: ResultVisitor[EX]): Unit = ???

    @throws(classOf[Exception])
    override def accept[EX <: Exception](visitor: QueryResultVisitor[EX]): Unit = ???

    override def javaColumnAs[T](column: String): ResourceIterator[T] = inner.javaColumnAs(column)

    override def executionPlanDescription(): InternalPlanDescription = lift(inner.executionPlanDescription())

    private def lift(planDescription: v3_2.planDescription.InternalPlanDescription): InternalPlanDescription = {

      import v3_2.planDescription.InternalPlanDescription.{Arguments => Arguments3_2}

      val name: String = planDescription.name
      val children: Children = planDescription.children match {
        case v3_2.planDescription.NoChildren => NoChildren
        case v3_2.planDescription.SingleChild(child) => SingleChild(lift(child))
        case v3_2.planDescription.TwoChildren(left, right) => TwoChildren(lift(left), lift(right))
      }

      val arguments: Seq[Argument] = planDescription.arguments.map {
        case Arguments3_2.ByteCode(className, disassembly) => Arguments.ByteCode(className, disassembly)
        case Arguments3_2.Expressions(e) => Arguments.Expressions(e.mapValues(oldExpression => null))
        case Arguments3_2.PageCacheHits(e) => Arguments.PageCacheHits(e)
        case Arguments3_2.PageCacheMisses(e) => Arguments.PageCacheMisses(e)
        case Arguments3_2.Time(value) => Arguments.Time(value)
        case Arguments3_2.Rows(value) => Arguments.Rows(value)
        case Arguments3_2.DbHits(value) => Arguments.DbHits(value)
        case Arguments3_2.ColumnsLeft(value) => Arguments.ColumnsLeft(value)
        case Arguments3_2.Expression(_) => Arguments.Expression(null)
        case Arguments3_2.LegacyExpression(_) => Arguments.Expression(null)
        case Arguments3_2.UpdateActionName(value) => Arguments.UpdateActionName(value)
        case Arguments3_2.MergePattern(startPoint) => Arguments.MergePattern(startPoint)
        case Arguments3_2.LegacyIndex(value) => Arguments.LegacyIndex(value)
        case Arguments3_2.Index(label, propertyKey) => Arguments.Index(label, propertyKey)
        case Arguments3_2.PrefixIndex(label, propertyKey, _) => Arguments.PrefixIndex(label, propertyKey, null)
        case Arguments3_2.InequalityIndex(label, propertyKey, bounds) => Arguments
          .InequalityIndex(label, propertyKey, bounds)
        case Arguments3_2.LabelName(label) => Arguments.LabelName(label)
        case Arguments3_2.KeyNames(keys) => Arguments.KeyNames(keys)
        case Arguments3_2.KeyExpressions(_) => Arguments.KeyExpressions(null)
        case Arguments3_2.EntityByIdRhs(_) => Arguments.EntityByIdRhs(null)
        case Arguments3_2.EstimatedRows(value) => Arguments.EstimatedRows(value)
        case Arguments3_2.Version(value) => Arguments.Version(value)
        case Arguments3_2.Planner(value) => Arguments.Planner(value)
        case Arguments3_2.PlannerImpl(value) => Arguments.PlannerImpl(value)
        case Arguments3_2.Runtime(value) => Arguments.Runtime(value)
        case Arguments3_2.RuntimeImpl(value) => Arguments.RuntimeImpl(value)
        case Arguments3_2.ExpandExpression(from, relName, relTypes, to, direction, min, max) =>
          val dir = direction match {
            case SemanticDirection3_2.OUTGOING => OUTGOING
            case SemanticDirection3_2.INCOMING => INCOMING
            case SemanticDirection3_2.BOTH => BOTH
          }
          Arguments.ExpandExpression(from, relName, relTypes, to, dir, min, max)
        case Arguments3_2.SourceCode(className, sourceCode) =>
          Arguments.SourceCode(className, sourceCode)
        case Arguments3_2.CountNodesExpression(ident, label) => Arguments.CountNodesExpression(ident, List(label))
        case Arguments3_2.CountRelationshipsExpression(ident, startLabel, typeNames, endLabel) => Arguments
          .CountRelationshipsExpression(ident, startLabel, typeNames, endLabel)
        case Arguments3_2.LegacyExpressions(expressions) => Arguments.Expressions(
          expressions.mapValues(_ => null))
        case Arguments3_2.Signature(procedureName, _, results) =>
          val procName = QualifiedName(procedureName.namespace, procedureName.name)
          Arguments.Signature(procName, Seq.empty, results.map(pair => (pair._1, lift(pair._2))))
      }
      PlanDescriptionImpl(new Id, name, children, arguments, planDescription.variables)
    }

    override def close(): Unit = inner.close()

    override def notifications: Iterable[Notification] = inner.notifications
      .map(org.neo4j.cypher.internal.compatibility.v3_2.ExecutionResultWrapper.asKernelNotification(None))


    private def lift(position: frontend.v3_2.InputPosition): InputPosition = {
      InputPosition.apply(position.offset, position.line, position.column)
    }

    private def lift(cypherType: symbols3_2.CypherType): symbols.CypherType = cypherType match {
      case symbols3_2.CTAny => symbols.CTAny
      case symbols3_2.CTBoolean => symbols.CTBoolean
      case symbols3_2.CTString => symbols.CTString
      case symbols3_2.CTNumber => symbols.CTNumber
      case symbols3_2.CTFloat => symbols.CTFloat
      case symbols3_2.CTInteger => symbols.CTInteger
      case symbols3_2.CTMap => symbols.CTMap
      case symbols3_2.CTNode => symbols.CTNode
      case symbols3_2.CTRelationship => symbols.CTRelationship
      case symbols3_2.CTPoint => symbols.CTPoint
      case symbols3_2.CTGeometry => symbols.CTGeometry
      case symbols3_2.CTPath => symbols.CTPath
      case symbols3_2.ListType(t) => symbols.ListType(lift(t))
    }

    override def planDescriptionRequested: Boolean = inner.planDescriptionRequested

    override def next(): Map[String, Any] = inner.next()

    override def hasNext: Boolean = inner.hasNext

    override def withNotifications(notification: Notification*): InternalExecutionResult = this
  }

}
