/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_2.planner.logical

import org.neo4j.cypher.internal.compiler.v2_2._
import org.neo4j.cypher.internal.compiler.v2_2.ast.{NestedPlanExpression, PatternExpression, Expression}
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.plans.IdName

// Rewrite pattern expressions to nested plan expressions by planning them using the given context
case class patternExpressionRewriter(planArguments: Set[IdName], expression: Expression, context: LogicalPlanningContext) extends Rewriter {

    import org.neo4j.cypher.internal.compiler.v2_2.Foldable._

    val exprScopes = expression.inputs.map { case (k, v) => k -> v.map(IdName.fromIdentifier)}
    val exprScopeMap = IdentityMap(exprScopes: _*)

    def apply(that: AnyRef): AnyRef = {
        val replacements = that.treeFold(Map.empty[Ref[Expression], Expression]) {
          case expr @ PatternExpression(pattern) =>
            (acc, children) =>
              val (plan, namedExpr) = QueryPlanningStrategy.planPatternExpression(planArguments ++ exprScopeMap(expr), expr)(context)
              children(acc.updated(Ref(expr), NestedPlanExpression(plan, namedExpr)(namedExpr.position)))
          case _: NestedPlanExpression =>
            (acc, children) =>
              acc
        }

        val rewriter = Rewriter.lift {
          case expr: PatternExpression if replacements.contains(Ref(expr)) =>
            replacements(Ref(expr))
        }

        bottomUp(rewriter)(that)
    }
}
