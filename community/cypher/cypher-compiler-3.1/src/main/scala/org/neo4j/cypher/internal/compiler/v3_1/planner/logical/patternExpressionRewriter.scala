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
package org.neo4j.cypher.internal.compiler.v3_1.planner.logical

import org.neo4j.cypher.internal.compiler.v3_1.ast.NestedPlanExpression
import org.neo4j.cypher.internal.compiler.v3_1.planner.logical.plans.IdName
import org.neo4j.cypher.internal.frontend.v3_1.Foldable._
import org.neo4j.cypher.internal.frontend.v3_1.ast.{Expression, PatternExpression}
import org.neo4j.cypher.internal.frontend.v3_1.{topDown, IdentityMap, Rewriter}

// Rewrite pattern expressions to nested plan expressions by planning them using the given context
case class patternExpressionRewriter(planArguments: Set[IdName], context: LogicalPlanningContext) extends Rewriter {

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
      case (k, v) => k -> v.map(IdName.fromVariable)
    }
    IdentityMap(exprScopes: _*)
  }

  private def computeReplacements(scopeMap: IdentityMap[Expression, Set[IdName]], that: AnyRef): IdentityMap[AnyRef, AnyRef] = {
    that.treeFold(IdentityMap.empty[AnyRef, AnyRef]) {

      // replace pattern expressions with their plan and also register
      // the contained pattern expression for no further processing
      // by this tree fold
      case expr@PatternExpression(pattern) =>
        acc =>
          // only process pattern expressions that were not contained in previously seen nested plans
          val newAcc = if (acc.contains(expr)) {
            acc
          } else {
            val arguments = planArguments ++ scopeMap(expr)
            val (plan, namedExpr) = context.strategy.planPatternExpression(arguments, expr)(context)
            val uniqueNamedExpr = namedExpr.copy()
            val nestedPlan = NestedPlanExpression(plan, uniqueNamedExpr)(uniqueNamedExpr.position)
            acc.updated(expr, nestedPlan)
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
