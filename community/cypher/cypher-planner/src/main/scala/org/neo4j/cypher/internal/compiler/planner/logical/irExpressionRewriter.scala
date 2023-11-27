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
package org.neo4j.cypher.internal.compiler.planner.logical

import org.neo4j.cypher.internal.ir.ast.CountIRExpression
import org.neo4j.cypher.internal.ir.ast.ExistsIRExpression
import org.neo4j.cypher.internal.ir.ast.ListIRExpression
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.NestedPlanExpression
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.topDown

/**
 * Rewrite IRExpressions to nested plan expressions by planning them using the given context.
 * This is only done for expressions that have not already been unnested.
 * 
 * We don't pass in the interesting order because
 *   i) There is no way of expressing order within a ListIRExpressions
 *   ii) It can lead to endless recursion in case the sort expression contains the subquery we are solving
 */
case class irExpressionRewriter(outerPlan: LogicalPlan, context: LogicalPlanningContext) extends Rewriter {

  private val instance = topDown(
    Rewriter.lift {
      case expr: ExistsIRExpression =>
        val subQueryPlan = plannerQueryPlanner.planSubqueryWithLabelInfo(outerPlan, expr, context)
        NestedPlanExpression.exists(subQueryPlan, expr)(expr.position)

      case expr @ ListIRExpression(_, variableToCollect, _, _) =>
        val subQueryPlan = plannerQueryPlanner.planSubqueryWithLabelInfo(outerPlan, expr, context)
        NestedPlanExpression.collect(subQueryPlan, variableToCollect, expr)(expr.position)

      case expr: CountIRExpression =>
        val subQueryPlan = plannerQueryPlanner.planSubqueryWithLabelInfo(outerPlan, expr, context)
        NestedPlanExpression.count(subQueryPlan, expr.countVariable, expr)(expr.position)
    },
    // Do not rewrite anything inside the NestedPlanExpressions that we generate
    stopper = _.isInstanceOf[NestedPlanExpression],
    cancellation = context.staticComponents.cancellationChecker
  )

  override def apply(that: AnyRef): AnyRef = instance(that)
}
