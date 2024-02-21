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
package org.neo4j.cypher.internal.compiler.planner.logical.steps

import org.neo4j.cypher.internal.compiler.planner.logical.LogicalPlanningContext
import org.neo4j.cypher.internal.compiler.planner.logical.PlanTransformer
import org.neo4j.cypher.internal.compiler.planner.logical.simpleExpressionEvaluator
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.ir.QueryPagination
import org.neo4j.cypher.internal.ir.QueryProjection
import org.neo4j.cypher.internal.ir.SinglePlannerQuery
import org.neo4j.cypher.internal.logical.plans.ExhaustiveLimit
import org.neo4j.cypher.internal.logical.plans.ExhaustiveLogicalPlan
import org.neo4j.cypher.internal.logical.plans.Limit
import org.neo4j.cypher.internal.logical.plans.LogicalBinaryPlan
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.util.attribution.IdGen

import scala.annotation.tailrec

object skipAndLimit extends PlanTransformer {

  @tailrec
  def shouldPlanExhaustiveLimit(plan: LogicalPlan, limit: Option[Long]): Boolean = plan match {
    case p if p.isUpdatingPlan                           => true
    case _: ExhaustiveLogicalPlan if limit.exists(_ > 0) => false
    case p: LogicalBinaryPlan => if (p.hasUpdatingRhs) true else shouldPlanExhaustiveLimit(p.left, limit)
    case p: LogicalPlan => p.lhs match {
        case Some(source) => shouldPlanExhaustiveLimit(source, limit)
        case None         => false
      }
  }

  def planLimitOnTopOf(plan: LogicalPlan, count: Expression)(implicit idGen: IdGen): LogicalPlan =
    if (shouldPlanExhaustiveLimit(plan, simpleExpressionEvaluator.evaluateLongIfStable(count)))
      ExhaustiveLimit(plan, count)(idGen)
    else Limit(plan, count)(idGen)

  def apply(plan: LogicalPlan, query: SinglePlannerQuery, context: LogicalPlanningContext): LogicalPlan = {
    query.horizon match {
      case p: QueryProjection =>
        val queryPagination = p.queryPagination
        (queryPagination.skip, queryPagination.limit) match {
          case (Some(skipExpr), Some(limitExpr)) if skipExpr.isConstantForQuery =>
            context.staticComponents.logicalPlanProducer.planSkipAndLimit(
              plan,
              skipExpr,
              limitExpr,
              query.interestingOrder,
              context,
              shouldPlanExhaustiveLimit(plan, simpleExpressionEvaluator.evaluateLongIfStable(limitExpr))
            )

          case (Some(skipExpr), Some(limitExpr)) =>
            val skipped =
              context.staticComponents.logicalPlanProducer.planSkip(plan, skipExpr, query.interestingOrder, context)
            // Recurse and remove skip from horizon to get limit planned as well
            apply(skipped, query.withHorizon(p.withPagination(QueryPagination(None, Some(limitExpr)))), context)

          case (Some(skipExpr), _) =>
            context.staticComponents.logicalPlanProducer.planSkip(plan, skipExpr, query.interestingOrder, context)

          case (_, Some(limitExpr))
            if shouldPlanExhaustiveLimit(plan, simpleExpressionEvaluator.evaluateLongIfStable(limitExpr)) =>
            context.staticComponents.logicalPlanProducer.planExhaustiveLimit(
              plan,
              limitExpr,
              limitExpr,
              query.interestingOrder,
              context = context
            )

          case (_, Some(limitExpr)) =>
            context.staticComponents.logicalPlanProducer.planLimit(
              plan,
              limitExpr,
              limitExpr,
              query.interestingOrder,
              context = context
            )

          case _ =>
            plan
        }

      case _ => plan
    }
  }
}
