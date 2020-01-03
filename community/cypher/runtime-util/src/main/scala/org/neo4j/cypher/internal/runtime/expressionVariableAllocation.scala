/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.cypher.internal.runtime

import org.neo4j.cypher.internal.logical.plans.{LogicalPlan, NestedPlanExpression, PruningVarExpand, VarExpand}
import org.neo4j.cypher.internal.runtime.ast.ExpressionVariable
import org.neo4j.cypher.internal.v4_0.expressions.{CachedProperty, LogicalVariable, Property, ScopeExpression}
import org.neo4j.cypher.internal.v4_0.util.attribution.Attribute
import org.neo4j.cypher.internal.v4_0.util.{Foldable, Rewritable, Rewriter, topDown}

import scala.collection.mutable

/**
  * Piece of physical planning which
  *
  *   1) identifies variables that have expression scope (expression variables)
  *   2) allocates slots for these in the expression slot space (separate from ExecutionContext longs and refs)
  *   3) rewrites instances of these variables to [[ExpressionVariable]]s with the correct slots offset
  */
object expressionVariableAllocation {

  /**
    * Attribute listing the expression variables in scope for nested logical plans. Only the root
    * of the nested plan tree will have in expression variables listed here.
    */
  class AvailableExpressionVariables extends Attribute[LogicalPlan, Seq[ExpressionVariable]]

  case class Result[T](rewritten: T,
                       nExpressionSlots: Int,
                       availableExpressionVars: AvailableExpressionVariables)

  def allocate[T <: Foldable with Rewritable](input: T): Result[T] = {

    val globalMapping = mutable.Map[String, ExpressionVariable]()
    val availableExpressionVars = new AvailableExpressionVariables

    def allocateVariables(outerVars: List[ExpressionVariable],
                          variables: Traversable[LogicalVariable]
                         ): List[ExpressionVariable] = {
      var innerVars = outerVars
      for (variable <- variables) {
        val nextVariable = ExpressionVariable(innerVars.length, variable.name)
        globalMapping += variable.name -> nextVariable
        innerVars = nextVariable :: innerVars
      }
      innerVars
    }

    // Note: we use the treeFold to keep track of the expression variables in scope
    // We don't need the result, the side-effect mutated `globalMapping` and
    // `availableExpressionVars` contain all the data we need.
    input.treeFold(List.empty[ExpressionVariable]) {
      case x: ScopeExpression =>
        outerVars =>
          val innerVars = allocateVariables(outerVars, x.introducedVariables)
          (innerVars, Some(_ => outerVars))

      case x: VarExpand =>
        outerVars =>
          val innerVars = allocateVariables(outerVars, (x.nodePredicate ++ x.relationshipPredicate).map(_.variable))
          (innerVars, Some(_ => outerVars))

      case x: PruningVarExpand =>
        outerVars =>
          val innerVars = allocateVariables(outerVars, (x.nodePredicate ++ x.relationshipPredicate).map(_.variable))
          (innerVars, Some(_ => outerVars))

      case x: NestedPlanExpression =>
        outerVars => {
          availableExpressionVars.set(x.plan.id, outerVars)
          (outerVars, Some(_ => outerVars))
        }
    }

    val rewriter =
      topDown( Rewriter.lift {
        // Cached properties would have to be cached together with the Expression Variables.
        // Not caching the property until we have support for that.
        case cp@CachedProperty(_, v, p, _) if globalMapping.contains(v.name) =>
          Property(globalMapping(v.name), p)(cp.position)
        case x: LogicalVariable if globalMapping.contains(x.name) =>
          globalMapping(x.name)
      })

    val nExpressionSlots = globalMapping.values.map(_.offset).reduceOption(math.max).map(_ + 1).getOrElse(0)
    Result(input.endoRewrite(rewriter), nExpressionSlots, availableExpressionVars)
  }
}
