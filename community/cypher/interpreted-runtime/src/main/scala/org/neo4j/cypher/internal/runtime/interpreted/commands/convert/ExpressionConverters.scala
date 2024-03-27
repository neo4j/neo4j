/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.runtime.interpreted.commands.convert

import org.neo4j.cypher.internal
import org.neo4j.cypher.internal.expressions
import org.neo4j.cypher.internal.expressions.ElementIdToLongId
import org.neo4j.cypher.internal.expressions.EntityType
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.logical.plans.ManySeekableArgs
import org.neo4j.cypher.internal.logical.plans.SeekableArgs
import org.neo4j.cypher.internal.logical.plans.SingleSeekableArg
import org.neo4j.cypher.internal.runtime.interpreted.CommandProjection
import org.neo4j.cypher.internal.runtime.interpreted.GroupingExpression
import org.neo4j.cypher.internal.runtime.interpreted.commands
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.ProjectedPath
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.ProjectedPath.Projector
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.ProjectedPath.multiIncomingRelationshipProjector
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.ProjectedPath.multiIncomingRelationshipWithKnownTargetProjector
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.ProjectedPath.multiOutgoingRelationshipProjector
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.ProjectedPath.multiOutgoingRelationshipWithKnownTargetProjector
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.ProjectedPath.multiUndirectedRelationshipProjector
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.ProjectedPath.multiUndirectedRelationshipWithKnownTargetProjector
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.ProjectedPath.nilProjector
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.ProjectedPath.quantifiedPathProjector
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.ProjectedPath.singleIncomingRelationshipProjector
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.ProjectedPath.singleNodeProjector
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.ProjectedPath.singleOutgoingRelationshipProjector
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.ProjectedPath.singleRelationshipWithKnownTargetProjector
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.ProjectedPath.singleUndirectedRelationshipProjector
import org.neo4j.cypher.internal.runtime.interpreted.commands.predicates
import org.neo4j.cypher.internal.runtime.interpreted.commands.predicates.Predicate
import org.neo4j.cypher.internal.runtime.interpreted.pipes.ManySeekArgs
import org.neo4j.cypher.internal.runtime.interpreted.pipes.SeekArgs
import org.neo4j.cypher.internal.runtime.interpreted.pipes.SingleSeekArg
import org.neo4j.cypher.internal.util.InternalNotification
import org.neo4j.cypher.internal.util.Many
import org.neo4j.cypher.internal.util.One
import org.neo4j.cypher.internal.util.Zero
import org.neo4j.cypher.internal.util.ZeroOneOrMany
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.exceptions.InternalException
import org.neo4j.graphdb.Direction

trait ExpressionConverter {

  def toCommandExpression(
    id: Id,
    expression: Expression,
    self: ExpressionConverters
  ): Option[commands.expressions.Expression]

  def toCommandProjection(
    id: Id,
    projections: Map[LogicalVariable, Expression],
    self: ExpressionConverters
  ): Option[CommandProjection]

  def toGroupingExpression(
    id: Id,
    groupings: Map[LogicalVariable, Expression],
    orderToLeverage: collection.Seq[Expression],
    self: ExpressionConverters
  ): Option[GroupingExpression]
}

trait ExpressionConversionLogger {
  def failedToConvertCompiledExpression(expression: internal.expressions.Expression): Unit
  def failedToConvertCompiledProjection(projection: Map[LogicalVariable, expressions.Expression]): Unit
  def warnings: Set[InternalNotification]
}

object NullExpressionConversionLogger extends ExpressionConversionLogger {
  override def failedToConvertCompiledExpression(expression: internal.expressions.Expression): Unit = {}
  override def failedToConvertCompiledProjection(projection: Map[LogicalVariable, Expression]): Unit = {}
  override def warnings: Set[InternalNotification] = Set.empty

}

/**
 * Converts or compiles expressions to executables.
 *
 * Uses a chained approach where if one converter responds with `None` when trying to compile an expression we try with
 * the next one in the chain of fallBackExpressionConverters. If a 'main' converter is specified we try this one first but
 * does not use that when falling back. That means for an expression-tree A(B(C(..)))) if the main converter cannot compile
 * A we will not attempt to use the main converter for compiling B and C. However, for the fallback converters we will use
 * the same chain for each expression, so if the first (fallback) converter fails to compile A we will still attempt
 * to compile B and C with that converter.
 *
 * @param maybeMainConverter           if specified we will use try with this one first,
 *                                     if it fails we will rely on fallback-converters for compilation.
 * @param fallbackExpressionConverters A chain of converters where we try to use the converters in the specified order
 *                                     for each expression of the expression-tree.
 */
class ExpressionConverters(
  maybeMainConverter: Option[ExpressionConverter],
  fallbackExpressionConverters: ExpressionConverter*
) {

  self =>

  private val fallbackConverters = maybeMainConverter match {
    case Some(_) => new ExpressionConverters(None, fallbackExpressionConverters: _*)
    case _       => this
  }

  def toCommandExpression(id: Id, expression: internal.expressions.Expression): commands.expressions.Expression = {
    compile(
      (c, cs) => c.toCommandExpression(id, expression, cs),
      s"Unknown expression type during transformation (${expression.getClass})"
    )
  }

  def toCommandProjection(
    id: Id,
    projections: Map[LogicalVariable, internal.expressions.Expression]
  ): CommandProjection = {
    compile(
      (c, cs) => c.toCommandProjection(id, projections, cs),
      s"Unknown projection type during transformation ($projections)"
    )
  }

  def toGroupingExpression(
    id: Id,
    groupings: Map[LogicalVariable, internal.expressions.Expression],
    orderToLeverage: Seq[internal.expressions.Expression]
  ): GroupingExpression = {
    compile(
      (c, cs) => c.toGroupingExpression(id, groupings, orderToLeverage, cs),
      s"Unknown grouping type during transformation ($groupings)"
    )
  }

  def toCommandPredicate(id: Id, in: internal.expressions.Expression): Predicate = in match {
    case e: internal.expressions.PatternExpression => predicates.NonEmpty(toCommandExpression(id, e))
    case e: internal.expressions.ListComprehension => predicates.NonEmpty(toCommandExpression(id, e))
    case e => toCommandExpression(id, e) match {
        case c: Predicate => c
        case c            => predicates.CoercedPredicate(c)
      }
  }

  def toCommandPredicate(id: Id, expression: Option[internal.expressions.Expression]): Predicate =
    expression.map(e => self.toCommandPredicate(id, e)).getOrElse(predicates.True())

  def toCommandSeekArgs(id: Id, seek: SeekableArgs): SeekArgs = seek match {
    case SingleSeekableArg(expr) => SingleSeekArg(toCommandExpression(id, expr))
    case ManySeekableArgs(expr) => expr match {
        case coll: internal.expressions.ListLiteral =>
          ZeroOneOrMany(coll.expressions) match {
            case Zero       => SeekArgs.empty
            case One(value) => SingleSeekArg(toCommandExpression(id, value))
            case Many(_)    => ManySeekArgs(toCommandExpression(id, coll))
          }

        case _ =>
          ManySeekArgs(toCommandExpression(id, expr))
      }
  }

  def toCommandElementIdSeekArgs(id: Id, seek: SeekableArgs, entityType: EntityType): SeekArgs = {
    def single(expr: Expression): SeekArgs = {
      val newExpr = ElementIdToLongId(entityType, ElementIdToLongId.Mode.Single, expr)(expr.position)
      SingleSeekArg(toCommandExpression(id, newExpr))
    }

    def many(expr: Expression): SeekArgs = {
      val newExpr = ElementIdToLongId(entityType, ElementIdToLongId.Mode.Many, expr)(expr.position)
      ManySeekArgs(toCommandExpression(id, newExpr))
    }

    seek match {
      case SingleSeekableArg(expr) => single(expr)
      case ManySeekableArgs(expr) => expr match {
          case coll: internal.expressions.ListLiteral =>
            ZeroOneOrMany(coll.expressions) match {
              case Zero       => SeekArgs.empty
              case One(value) => single(value)
              case Many(_)    => many(coll)
            }

          case _ =>
            many(expr)
        }
    }
  }

  def toCommandProjectedPath(e: internal.expressions.PathExpression): ProjectedPath = {
    def project(pathStep: internal.expressions.PathStep): Projector = pathStep match {

      case internal.expressions.NodePathStep(node: LogicalVariable, next) =>
        singleNodeProjector(node.name, project(next))

      case internal.expressions.SingleRelationshipPathStep(
          rel: LogicalVariable,
          _,
          Some(target: LogicalVariable),
          next
        ) =>
        singleRelationshipWithKnownTargetProjector(rel.name, target.name, project(next))

      case internal.expressions.SingleRelationshipPathStep(rel: LogicalVariable, SemanticDirection.INCOMING, _, next) =>
        singleIncomingRelationshipProjector(rel.name, project(next))

      case internal.expressions.SingleRelationshipPathStep(rel: LogicalVariable, SemanticDirection.OUTGOING, _, next) =>
        singleOutgoingRelationshipProjector(rel.name, project(next))

      case internal.expressions.SingleRelationshipPathStep(rel: LogicalVariable, SemanticDirection.BOTH, _, next) =>
        singleUndirectedRelationshipProjector(rel.name, project(next))

      case internal.expressions.MultiRelationshipPathStep(
          rel: LogicalVariable,
          SemanticDirection.INCOMING,
          Some(target),
          next
        ) =>
        multiIncomingRelationshipWithKnownTargetProjector(rel.name, target.name, project(next))

      case internal.expressions.MultiRelationshipPathStep(
          rel: LogicalVariable,
          SemanticDirection.OUTGOING,
          Some(target),
          next
        ) =>
        multiOutgoingRelationshipWithKnownTargetProjector(rel.name, target.name, project(next))

      case internal.expressions.MultiRelationshipPathStep(
          rel: LogicalVariable,
          SemanticDirection.BOTH,
          Some(target),
          next
        ) =>
        multiUndirectedRelationshipWithKnownTargetProjector(rel.name, target.name, project(next))

      case internal.expressions.MultiRelationshipPathStep(
          rel: LogicalVariable,
          SemanticDirection.INCOMING,
          None,
          next
        ) =>
        multiIncomingRelationshipProjector(rel.name, project(next))

      case internal.expressions.MultiRelationshipPathStep(
          rel: LogicalVariable,
          SemanticDirection.OUTGOING,
          None,
          next
        ) =>
        multiOutgoingRelationshipProjector(rel.name, project(next))

      case internal.expressions.MultiRelationshipPathStep(rel: LogicalVariable, SemanticDirection.BOTH, None, next) =>
        multiUndirectedRelationshipProjector(rel.name, project(next))

      case internal.expressions.RepeatPathStep(variables, toNode, next) =>
        quantifiedPathProjector(variables.flatMap(_.variables).map(_.name), toNode.name, project(next))

      case internal.expressions.NilPathStep() =>
        nilProjector

      case x =>
        throw new IllegalArgumentException(s"Unknown pattern part found in expression: $x")
    }

    ProjectedPath(project(e.step))
  }

  private def compile[T](call: (ExpressionConverter, ExpressionConverters) => Option[T], errorMsg: => String): T = {
    maybeMainConverter match {
      case Some(converter) => call(converter, self) match {
          case Some(value) => value
          case None        => fallback(call, errorMsg)
        }
      case None => fallback(call, errorMsg)
    }
  }

  private def fallback[T](call: (ExpressionConverter, ExpressionConverters) => Option[T], errorMsg: => String): T = {
    var i = 0
    while (i < fallbackExpressionConverters.length) {
      call(fallbackExpressionConverters(i), fallbackConverters) match {
        case Some(x) => return x
        case None    =>
      }
      i += 1
    }
    throw new InternalException(errorMsg)
  }
}

object DirectionConverter {

  def toGraphDb(dir: SemanticDirection): Direction = dir match {
    case SemanticDirection.INCOMING => Direction.INCOMING
    case SemanticDirection.OUTGOING => Direction.OUTGOING
    case SemanticDirection.BOTH     => Direction.BOTH
  }
}
