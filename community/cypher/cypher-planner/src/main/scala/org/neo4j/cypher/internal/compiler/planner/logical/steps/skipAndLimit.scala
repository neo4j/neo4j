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

import org.neo4j.cypher.internal.compiler.planner.logical.ExpressionEvaluator
import org.neo4j.cypher.internal.compiler.planner.logical.LogicalPlanningContext
import org.neo4j.cypher.internal.compiler.planner.logical.PlanTransformer
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.SignedDecimalIntegerLiteral
import org.neo4j.cypher.internal.ir.QueryPagination
import org.neo4j.cypher.internal.ir.QueryProjection
import org.neo4j.cypher.internal.ir.SinglePlannerQuery
import org.neo4j.cypher.internal.logical.plans.ExhaustiveLimit
import org.neo4j.cypher.internal.logical.plans.ExhaustiveLogicalPlan
import org.neo4j.cypher.internal.logical.plans.Limit
import org.neo4j.cypher.internal.logical.plans.LogicalBinaryPlan
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.RemoteBatchProperties
import org.neo4j.cypher.internal.util.InputPosition
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

  def planLimitOnTopOf(
    plan: LogicalPlan,
    count: Expression,
    expressionEvaluator: ExpressionEvaluator
  )(implicit idGen: IdGen): LogicalPlan = {
    doPlanLimitOnTopOf(plan, expressionEvaluator.evaluateLongIfStable(count), count)
  }

  def planLimitOnTopOf(plan: LogicalPlan, count: Long)(implicit idGen: IdGen): LogicalPlan = {
    val countExpr = SignedDecimalIntegerLiteral(count.toString)(InputPosition.NONE)
    doPlanLimitOnTopOf(plan, Some(count), countExpr)
  }

  private def doPlanLimitOnTopOf(plan: LogicalPlan, maybeCount: Option[Long], countExpr: Expression)(implicit
    idGen: IdGen): LogicalPlan = {
    if (shouldPlanExhaustiveLimit(plan, maybeCount))
      ExhaustiveLimit(plan, countExpr)(idGen)
    else Limit(plan, countExpr)(idGen)
  }

  def apply(plan: LogicalPlan, query: SinglePlannerQuery, context: LogicalPlanningContext): LogicalPlan = {
    plan match {
      // A remoteBatchProperties operator will run on a fixed batch size irrespective of the actual limit.
      // Since the remoteBatchProperties operator will not affect the overall correctness of the output,
      // and has to run on a remote shard, it is more performant to run this operator after the limit operator.
      case remoteBatchProperties: RemoteBatchProperties =>
        planSkipAndLimit(remoteBatchProperties.source, query, context)
          .map(skipAndLimitPlan =>
            context.staticComponents.logicalPlanProducer.changeSourceOnRemoteBatchProperties(
              skipAndLimitPlan,
              remoteBatchProperties,
              context
            )
          )
          .getOrElse(remoteBatchProperties)
      case otherPlans =>
        planSkipAndLimit(otherPlans, query, context).getOrElse(otherPlans)
    }
  }

  @tailrec
  private def planSkipAndLimit(
    plan: LogicalPlan,
    query: SinglePlannerQuery,
    context: LogicalPlanningContext
  ): Option[LogicalPlan] = {
    query.horizon match {
      case p: QueryProjection =>
        val queryPagination = p.queryPagination
        (queryPagination.skip, queryPagination.limit) match {
          case (Some(skipExpr), Some(limitExpr)) if skipExpr.isConstantForQuery =>
            Some(context.staticComponents.logicalPlanProducer.planSkipAndLimit(
              plan,
              skipExpr,
              limitExpr,
              query.interestingOrder,
              context,
              shouldPlanExhaustiveLimit(
                plan,
                context.staticComponents.expressionEvaluator.evaluateLongIfStable(limitExpr)
              )
            ))

          case (Some(skipExpr), Some(limitExpr)) =>
            val skipped =
              context.staticComponents.logicalPlanProducer.planSkip(plan, skipExpr, query.interestingOrder, context)
            // Recurse and remove skip from horizon to get limit planned as well
            planSkipAndLimit(
              skipped,
              query.withHorizon(p.withPagination(QueryPagination(None, Some(limitExpr)))),
              context
            )

          case (Some(skipExpr), _) =>
            Some(context.staticComponents.logicalPlanProducer.planSkip(plan, skipExpr, query.interestingOrder, context))

          case (_, Some(limitExpr))
            if shouldPlanExhaustiveLimit(
              plan,
              context.staticComponents.expressionEvaluator.evaluateLongIfStable(limitExpr)
            ) =>
            Some(context.staticComponents.logicalPlanProducer.planExhaustiveLimit(
              plan,
              limitExpr,
              limitExpr,
              query.interestingOrder,
              context = context
            ))

          case (_, Some(limitExpr)) =>
            Some(context.staticComponents.logicalPlanProducer.planLimit(
              plan,
              limitExpr,
              limitExpr,
              query.interestingOrder,
              context = context
            ))

          case _ =>
            None
        }

      case _ => None
    }
  }
}
