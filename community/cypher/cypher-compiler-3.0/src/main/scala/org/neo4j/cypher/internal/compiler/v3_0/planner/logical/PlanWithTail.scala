/*
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
package org.neo4j.cypher.internal.compiler.v3_0.planner.logical

import org.neo4j.cypher.internal.compiler.v3_0.planner.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.compiler.v3_0.planner.{PlannerQuery, QueryGraph}
import org.neo4j.cypher.internal.frontend.v3_0.Rewriter

import scala.annotation.tailrec

/*
This class ties together disparate query graphs through their event horizons. It does so by using Apply,
which in most cases is then rewritten away by LogicalPlan rewriting.

In cases where the preceding PlannerQuery has updates we must make the Apply an EagerApply if there
are overlaps between the previous update and the reads of any of the tails. We must also make Reads
in the tail into RepeatableReads if there is overlap between the read and update within the PlannerQuery.

For example:

    +Apply2
    |\
    | +Update3
    | |
    | +Read3
    |
    +Apply1
    |\
    | +Update2
    | |
    | +Read2
    |
    +Update1
    |
    +Read1

In this case the following must hold
  - Apply1 is eager if updates from Update1 will be matched by Reads2 or Reads3.
  - Apply2 is eager if updates from Update2 will be matched by Reads3
  - If Update2 affects Read2, Read2 must use RepeatableRead
  - If Update3 affects Read3, Read2 must use RepeatableRead

*/
case class PlanWithTail(expressionRewriterFactory: (LogicalPlanningContext => Rewriter) = ExpressionRewriterFactory,
                        planEventHorizon: LogicalPlanningFunction2[PlannerQuery, LogicalPlan, LogicalPlan] = PlanEventHorizon(),
                        planUpdates: LogicalPlanningFunction2[PlannerQuery, LogicalPlan, LogicalPlan] = PlanUpdates)
  extends LogicalPlanningFunction2[LogicalPlan, Option[PlannerQuery], LogicalPlan] {

  override def apply(lhs: LogicalPlan, remaining: Option[PlannerQuery])(implicit context: LogicalPlanningContext): LogicalPlan = {
    remaining match {
      case Some(query) =>
        val lhsContext = context.recurse(lhs)
        val partPlan = planPart(query, lhsContext, Some(context.logicalPlanProducer.planQueryArgumentRow(query.queryGraph)))


        //If reads interfere with writes, make it a RepeatableRead
        val planWithEffects =
          if (query.updateGraph.overlaps(query.queryGraph))
            context.logicalPlanProducer.planRepeatableRead(partPlan)
          else partPlan

        val planWithUpdates = planUpdates(query, planWithEffects)(context)

        //If previous update interferes with any of the reads here or in tail, make it an EagerApply
        val applyPlan = {
          val lastUpdateGraph = lhs.solved.lastUpdateGraph
          val previousWriteOnly = lhs.solved.lastQueryGraph.isEmpty && lastUpdateGraph.nonEmpty

          if (!previousWriteOnly &&
            query.allQueryGraphs.exists(lastUpdateGraph.overlaps))
            context.logicalPlanProducer.planEagerTailApply(lhs, planWithUpdates)
          else context.logicalPlanProducer.planTailApply(lhs, planWithUpdates)
        }

        val applyContext = lhsContext.recurse(applyPlan)
        val projectedPlan = planEventHorizon(query, applyPlan)(applyContext)
        val projectedContext = applyContext.recurse(projectedPlan)

        val completePlan = {
          val expressionRewriter = expressionRewriterFactory(projectedContext)

          projectedPlan.endoRewrite(expressionRewriter)
        }

        // planning nested expressions doesn't change outer cardinality
        apply(completePlan, query.tail)(projectedContext)

      case None =>
        lhs
    }
  }
}
