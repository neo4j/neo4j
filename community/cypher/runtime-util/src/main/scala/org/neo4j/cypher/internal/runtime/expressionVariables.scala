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
package org.neo4j.cypher.internal.runtime

import org.neo4j.cypher.internal.runtime.ast.ExpressionVariable
import org.neo4j.cypher.internal.v4_0.expressions.{LogicalVariable, ScopeExpression}
import org.neo4j.cypher.internal.v4_0.logical.plans.{LogicalPlan, PruningVarExpand, VarExpand}
import org.neo4j.cypher.internal.v4_0.util.{Rewriter, topDown}

import scala.collection.mutable

/**
  * Piece of physical planning which
  *
  *   1) identifies variables that have expression scope (expression variables)
  *   2) allocates slots for these in the expression slot space (separate from ExecutionContext longs and refs)
  *   3) rewrites instances of these variables to [[ExpressionVariable]]s with the correct slots offset
  */
object expressionVariables {

  def replace(lp: LogicalPlan): (LogicalPlan, Int) = {

    val globalMapping = mutable.Map[String, Int]()

    lp.treeFold(0) {
      case x: ScopeExpression =>
        prevNumExpressionVariables => {
          var slot = prevNumExpressionVariables
          for (variable <- x.introducedVariables) {
            globalMapping += variable.name -> slot
            slot += 1
          }
          (slot, Some(_ => prevNumExpressionVariables))
        }

      case x: VarExpand =>
        prevNumExpressionVariables => {
          var slot = prevNumExpressionVariables
          for (varPred <- x.nodePredicate ++ x.edgePredicate) {
            globalMapping += varPred.variable.name -> slot
            slot += 1
          }
          (slot, Some(_ => prevNumExpressionVariables))
        }

      case x: PruningVarExpand =>
        prevNumExpressionVariables => {
          var slot = prevNumExpressionVariables
          for (varPred <- x.nodePredicate ++ x.edgePredicate) {
            globalMapping += varPred.variable.name -> slot
            slot += 1
          }
          (slot, Some(_ => prevNumExpressionVariables))
        }
    }

    val rewriter =
      topDown( Rewriter.lift {
        case x: LogicalVariable if globalMapping.contains(x.name) =>
          ExpressionVariable(globalMapping(x.name), x.name)
      })

    (lp.endoRewrite(rewriter), globalMapping.values.reduceOption(math.max).map(_ + 1).getOrElse(0))
  }
}
