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
package org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter

import org.neo4j.cypher.internal.ast.VectorValueConstructor
import org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter.extractRuntimeConstants.STOPPER
import org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter.extractRuntimeConstants.getConstantExpressionWithConstantVariables
import org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter.extractRuntimeConstants.isConstant
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.IsAggregate
import org.neo4j.cypher.internal.expressions.Literal
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.MapExpression
import org.neo4j.cypher.internal.expressions.OperatorExpression
import org.neo4j.cypher.internal.expressions.Parameter
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.frontend.phases.ResolvedFunctionInvocation
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.LogicalPlans
import org.neo4j.cypher.internal.logical.plans.Projection
import org.neo4j.cypher.internal.logical.plans.RemoteBatchPropertiesWithFilter
import org.neo4j.cypher.internal.runtime.ast.RuntimeConstant
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.CancellationChecker
import org.neo4j.cypher.internal.util.FunctionName
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.RewriterStopperWithParent
import org.neo4j.cypher.internal.util.RewriterWithParent
import org.neo4j.cypher.internal.util.attribution.SameId
import org.neo4j.cypher.internal.util.topDown
import org.neo4j.cypher.internal.util.topDownWithParent

import scala.collection.mutable

/**
 * Extract expressions that are known to be constant for the lifetime of the execution of the query.
 * These are not the same thing as literals, since the value can change between executions.
 *
 * Example:
 * {{{
 *   MATCH (n) WHERE n.datetime < datetime({date: $date}) RETURN n
 * }}}
 *
 * will be rewritten to something like
 *
 * {{{
 *   MATCH (n) WHERE n.datetime < RuntimeConstant(datetime({date: $date})) RETURN n
 * }}}
 */
case class extractRuntimeConstants(
  anonymousVariableNameGenerator: AnonymousVariableNameGenerator,
  cancellationChecker: CancellationChecker = CancellationChecker.NeverCancelled
) extends Rewriter {

  override def apply(input: AnyRef): AnyRef = {

    val constantsByExpressions: Map[Expression, Expression] = input match {
      case plan: LogicalPlan =>
        LogicalPlans.foldPlan(Map.empty[Expression, Expression])(
          // input should be a ProjectResults
          plan,
          {
            case (acc, Projection(_, projections)) =>
              projections.foldLeft(acc) { case (innerAcc, (variable, expr)) =>
                getConstantExpressionWithConstantVariables(expr, innerAcc)
                  .map(constant => innerAcc.updated(variable, constant))
                  .getOrElse(innerAcc)
              }
            case (acc, _) => acc
          },
          combineLeftAndRight = (lhs, rhs, _) => lhs ++ rhs,
          mapArguments = (acc, _) => acc
        )(cancellationChecker)
      case _ =>
        Map.empty
    }

    val rewriter = topDownWithParent(
      RewriterWithParent.lift {
        case (runtimeConstant: RuntimeConstant, _)                  => runtimeConstant
        case (ConstantTemporalFunction(expr), _)                    => constant(expr)
        case (v: VectorValueConstructor, _) if v.isConstantForQuery => constant(v)
        case (rbpwf @ RemoteBatchPropertiesWithFilter(_, predicates, _), _) =>
          val newPredicates = predicates.endoRewrite(
            expressionRewriter(constantsByExpressions)
          )
          rbpwf.copy(predicates = newPredicates)(SameId(rbpwf.id))
      },
      stopper = STOPPER,
      cancellationChecker
    )

    rewriter.apply(input)
  }

  def expressionRewriter(constantsByExpressions: Map[Expression, Expression] = Map.empty)
    : Rewriter =
    topDownWithParent(
      RewriterWithParent.lift {
        case (expr: Expression, _) =>
          constantsByExpressions.get(expr).map(constant).getOrElse(expr)
      },
      stopper = STOPPER,
      cancellation = cancellationChecker
    )

  val runtimeConstants = mutable.Map.empty[Expression, RuntimeConstant]

  private def constant(e: Expression) = e match {
    case _: Parameter | _: Literal => e
    case nonObviousConstant =>
      runtimeConstants.getOrElseUpdate(
        e,
        RuntimeConstant(
          Variable(anonymousVariableNameGenerator.nextName)(InputPosition.NONE, Variable.isIsolatedDefault),
          nonObviousConstant
        )
      )
  }
}

object extractRuntimeConstants {

  val STOPPER: RewriterStopperWithParent = {
    // when we have found a constant, don't rewrite its children.
    case (_, Some(_: RuntimeConstant)) => true
    case (_, _)                        => false
  }

  def isConstant(arg: Expression): Boolean = arg match {
    // this is not supported semantically, but here just in case
    case MapExpression(Seq()) => false
    // {timezone: TZ} is like the no arg case, that is not necessarily constant
    case MapExpression(Seq((PropertyKeyName("timezone"), _))) => false
    case MapExpression(items)                                 => items.forall(entry => isConstant(entry._2))
    case ConstantExpression(_)                                => true
    case other                                                => other.isConstantForQuery
  }

  def getConstantExpressionWithConstantVariables(
    expression: Expression,
    constantsByOriginalExpression: Map[Expression, Expression]
  ): Option[Expression] =
    expression match {
      case variable: LogicalVariable =>
        constantsByOriginalExpression.get(variable)
      case ConstantExpression(expression) =>
        Some(expression)
      case expression: OperatorExpression =>
        val constantArguments =
          expression.arguments
            .map(getConstantExpressionWithConstantVariables(_, constantsByOriginalExpression))
        Option.when(constantArguments.forall(_.isDefined)) {
          expression.endoRewrite(topDown(Rewriter.lift {
            case variable: LogicalVariable =>
              constantsByOriginalExpression.getOrElse(variable, variable)
          }))
        }
      case _ => None
    }
}

trait FunctionMatcher extends Product {

  def unapply(arg: Expression): Option[IndexedSeq[Expression]] = {
    arg match {
      case ResolvedFunctionInvocation(FunctionName(ns, n), _, args, _)
        if n.equalsIgnoreCase(name) && ns.parts.toSeq == namespace =>
        Some(args)
      case _ => None
    }
  }

  def namespace: Seq[String] = Seq.empty
  def name: String = toString
}

case object Date extends FunctionMatcher
case object Datetime extends FunctionMatcher
case object LocalDatetime extends FunctionMatcher
case object Time extends FunctionMatcher
case object LocalTime extends FunctionMatcher
case object Duration extends FunctionMatcher

case object ConstantTemporalFunction {

  def unapply(arg: Expression): Option[Expression] = arg match {
    case d @ Datetime(Seq(arg)) if isConstant(arg)        => Some(d)
    case d @ LocalDatetime(Seq(arg)) if isConstant(arg)   => Some(d)
    case d @ Date(Seq(arg)) if isConstant(arg)            => Some(d)
    case d @ LocalTime(Seq(arg)) if isConstant(arg)       => Some(d)
    case d @ Time(Seq(arg)) if isConstant(arg)            => Some(d)
    case d @ Duration(Seq(arg)) if arg.isConstantForQuery => Some(d)
    case _                                                => None
  }
}

object ConstantExpression {

  def unapply(arg: Expression): Option[Expression] = arg match {
    case IsAggregate(_)                  => None
    case ConstantTemporalFunction(expr)  => Some(expr)
    case param: Parameter                => Some(param)
    case expr if expr.isConstantForQuery => Some(expr)
    case _                               => None
  }
}
