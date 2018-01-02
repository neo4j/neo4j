/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_3.planner.logical

import org.neo4j.cypher.internal.frontend.v2_3.Rewriter
import org.neo4j.cypher.internal.compiler.v2_3.planner.PlannerQuery
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.plans.LogicalPlan

/*
This class ties together disparate query graphs through their event horizons. It does so by using Apply,
which in most cases is then rewritten away by LogicalPlan rewriting
 */
case class PlanWithTail(expressionRewriterFactory: (LogicalPlanningContext => Rewriter) = ExpressionRewriterFactory,
                        planEventHorizon: LogicalPlanningFunction2[PlannerQuery, LogicalPlan, LogicalPlan] = PlanEventHorizon())
  extends LogicalPlanningFunction2[LogicalPlan, Option[PlannerQuery], LogicalPlan] {

  override def apply(pred: LogicalPlan, remaining: Option[PlannerQuery])(implicit context: LogicalPlanningContext): LogicalPlan = {
    remaining match {
      case Some(query) =>
        val lhs = pred
        val lhsContext = context.recurse(lhs)
        val rhs = planPart(query, lhsContext, Some(context.logicalPlanProducer.planQueryArgumentRow(query.graph)))
        val applyPlan = context.logicalPlanProducer.planTailApply(lhs, rhs)

        val applyContext = lhsContext.recurse(applyPlan)
        val projectedPlan = planEventHorizon(query, applyPlan)(applyContext)

        val projectedContext = applyContext.recurse(projectedPlan)
        val expressionRewriter = expressionRewriterFactory(projectedContext)
        val completePlan = projectedPlan.endoRewrite(expressionRewriter)

        // planning nested expressions doesn't change outer cardinality
        apply(completePlan, query.tail)(projectedContext)

      case None =>
        pred
    }
  }
}
