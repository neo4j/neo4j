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
package org.neo4j.cypher.internal.compiler.planner.logical.steps

import org.neo4j.cypher.internal.compiler.planner.logical.LogicalPlanningContext
import org.neo4j.cypher.internal.compiler.planner.logical.PlanTransformer
import org.neo4j.cypher.internal.expressions.Add
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.ir.QueryProjection
import org.neo4j.cypher.internal.ir.SinglePlannerQuery
import org.neo4j.cypher.internal.logical.plans.LogicalPlan

object skipAndLimit extends PlanTransformer {

  def apply(plan: LogicalPlan, query: SinglePlannerQuery, context: LogicalPlanningContext): LogicalPlan = {
    def addSkip(maybeSkip: Option[Expression], plan: LogicalPlan): LogicalPlan =
      maybeSkip.fold(plan)(skipExpression => context.logicalPlanProducer.planSkip(plan, skipExpression, query.interestingOrder, context))

    def addLimit(maybeLimit: Option[Expression],
                 maybeSkip: Option[Expression],
                 plan: LogicalPlan): LogicalPlan = {
      maybeLimit.fold(plan) { limitExpression =>
        // In case we have SKIP n LIMIT m, we want to limit by (n + m), since we plan the Limit before the Skip.
        val effectiveLimit = maybeSkip.fold(limitExpression)(skip => Add(limitExpression, skip)(limitExpression.position))
        context.logicalPlanProducer.planLimit(plan, effectiveLimit, limitExpression, query.interestingOrder, context = context)
      }
    }

    query.horizon match {
      case p: QueryProjection =>
        val queryPagination = p.queryPagination
        (queryPagination.skip, queryPagination.limit) match {
          case (skip, limit) =>
            addSkip(skip, addLimit(limit, skip, plan))
        }

      case _ => plan
    }
  }

}
