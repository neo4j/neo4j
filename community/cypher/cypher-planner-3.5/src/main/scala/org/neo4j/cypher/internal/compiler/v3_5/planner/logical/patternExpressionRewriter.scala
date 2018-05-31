/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.cypher.internal.compiler.v3_5.planner.logical

import org.neo4j.cypher.internal.planner.v3_5.spi.PlanningAttributes.{Cardinalities, Solveds}
import org.neo4j.cypher.internal.v3_5.logical.plans.NestedPlanExpression
import org.opencypher.v9_0.expressions._
import org.opencypher.v9_0.rewriting.rewriters.projectNamedPaths
import org.opencypher.v9_0.util.Foldable._
import org.opencypher.v9_0.util.{IdentityMap, Rewriter, topDown}

/*
Rewrite pattern expressions and pattern comprehensions to nested plan expressions by planning them using the given context.
This is only done for expressions that have not already been unnested
 */
case class patternExpressionRewriter(planArguments: Set[String], context: LogicalPlanningContext, solveds: Solveds, cardinalities: Cardinalities) extends Rewriter {

  override def apply(that: AnyRef): AnyRef = that match {
    case expression: Expression =>
      val scopeMap = computeScopeMap(expression)

      // build an identity map of replacements
      val replacements = computeReplacements(scopeMap, that)

      // apply replacements, descending into the replacements themselves recursively
      val rewriter = createRewriter(replacements)

      expression.endoRewrite(rewriter)
  }

  private def computeScopeMap(expression: Expression) = {
    val exprScopes = expression.inputs.map {
      case (k, v) => k -> v.map(_.name)
    }
    IdentityMap(exprScopes: _*)
  }

  private def computeReplacements(scopeMap: IdentityMap[Expression, Set[String]], that: AnyRef): IdentityMap[AnyRef, AnyRef] = {
    that.treeFold(IdentityMap.empty[AnyRef, AnyRef]) {

      // replace pattern expressions with their plan and also register
      // the contained pattern expression for no further processing
      // by this tree fold
      case expr@PatternExpression(pattern: RelationshipsPattern) =>
        acc =>
          // only process pattern expressions that were not contained in previously seen nested plans
          val newAcc = if (acc.contains(expr)) {
            acc
          } else {
            val arguments = planArguments ++ scopeMap(expr)
            val (plan, namedExpr) = context.strategy.planPatternExpression(arguments, expr, context, solveds, cardinalities)
            val uniqueNamedExpr = namedExpr.copy()
            val path = EveryPath(namedExpr.pattern.element)
            val step: PathStep = projectNamedPaths.patternPartPathExpression(path)
            val pathExpression: PathExpression = PathExpression(step)(expr.position)

            val rewrittenExpression = NestedPlanExpression(plan, pathExpression)(uniqueNamedExpr.position)
            acc.updated(expr, rewrittenExpression)
          }

          (newAcc, Some(identity))

      // replace pattern comprehension
      case expr@PatternComprehension(namedPath, pattern, predicate, projection, _) =>
        acc =>
          assert(namedPath.isEmpty, "Named paths in pattern comprehensions should have been rewritten away already")
          // only process pattern expressions that were not contained in previously seen nested plans
          val newAcc = if (acc.contains(expr)) {
            acc
          } else {
            val arguments = planArguments ++ scopeMap(expr)
            val (plan, namedExpr) = context.strategy.planPatternComprehension(arguments, expr, context, solveds, cardinalities)
            val uniqueNamedExpr = namedExpr.copy()(expr.position)

            val rewrittenExpression = NestedPlanExpression(plan, projection)(uniqueNamedExpr.position)
            acc.updated(expr, rewrittenExpression)
          }

          (newAcc, Some(identity))

      // Never ever replace pattern expressions in nested plan expressions in the original expression
      case NestedPlanExpression(_, pattern) =>
        acc => (acc.updated(pattern, pattern), Some(identity))
    }
  }

  private def createRewriter(replacements: IdentityMap[AnyRef, AnyRef]): Rewriter = {
    val rewriter = Rewriter.lift {
      case that => replacements.getOrElse(that, that)
    }
    topDown(rewriter, _.isInstanceOf[NestedPlanExpression])
  }
}
