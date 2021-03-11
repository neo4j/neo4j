/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.cypher.internal.compiler.planner.logical

import org.neo4j.cypher.internal.compiler.planner.logical.idp.BestResults
import org.neo4j.cypher.internal.compiler.planner.logical.steps.BestPlans
import org.neo4j.cypher.internal.ir.SinglePlannerQuery
import org.neo4j.cypher.internal.ir.ordering.InterestingOrder
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.util.Selectivity
import org.neo4j.cypher.internal.util.attribution.Attributes

import scala.language.implicitConversions

/*
This coordinates PlannerQuery planning and delegates work to the classes that do the actual planning of
QueryGraphs and EventHorizons
 */
case class PlanSingleQuery(planHead: HeadPlanner = PlanHead(),
                           planWithTail: TailPlanner = PlanWithTail())
  extends SingleQueryPlanner {

  private type StepResult = (BestPlans, LogicalPlanningContext)

  override def apply(query: SinglePlannerQuery, context: LogicalPlanningContext): LogicalPlan = {
    // to enable for-comprehension syntax
    implicit def stepResult2Some(t: StepResult): Some[StepResult] = Some(t)

    val limitSelectivities = LimitSelectivity.forAllParts(query, context)

    val Some(bestPlan) = for {
      (plans, context) <- planHead(query, context.withLimitSelectivity(limitSelectivities.head))
      (plans, context) <- planRemainingParts(plans, query, context, limitSelectivities)
      (plans, context) <- unnestEager(plans, context)
      pickBest = context.config.pickBestCandidate(context)
      bestPlan <- pickBest(plans.allResults.toIterable, s"best finalized plan for ${query.queryGraph}")
    } yield {
      bestPlan
    }

    bestPlan
  }

  private def planRemainingParts(plans: BestPlans,
                                 query: SinglePlannerQuery,
                                 context: LogicalPlanningContext,
                                 limitSelectivities: List[Selectivity]): StepResult = {
    val remainingPartsWithExtras = {
      val allParts = query.allPlannerQueries
      assert(allParts.length == limitSelectivities.length, "We should have limit selectivities for all query parts.")
      (allParts.tail, limitSelectivities.tail, allParts.map(_.interestingOrder)).zipped
    }

    remainingPartsWithExtras.foldLeft((plans, context)) {
      case ((plans, context), (queryPart, limitSelectivity, previousInterestingOrder)) =>
        planWithTail(plans, queryPart, previousInterestingOrder, context.withLimitSelectivity(limitSelectivity))
    }
  }

  private def unnestEager(plans: BestPlans, context: LogicalPlanningContext): StepResult = {
    val unnest = Eagerness.unnestEager(
      context.planningAttributes.solveds,
      context.planningAttributes.cardinalities,
      context.planningAttributes.providedOrders,
      Attributes(context.idGen)
    )

    val unnestedPlans = plans.map(_.endoRewrite(unnest))
    (unnestedPlans, context)
  }
}

trait MatchPlanner {
  def apply(query: SinglePlannerQuery, context: LogicalPlanningContext, rhsPart: Boolean = false): BestPlans
}

trait EventHorizonPlanner {
  def planHorizon(plannerQuery: SinglePlannerQuery,
                  incomingPlans: BestResults[LogicalPlan],
                  prevInterestingOrder: Option[InterestingOrder],
                  context: LogicalPlanningContext): BestResults[LogicalPlan]
}

trait HeadPlanner {
  def apply(headQuery: SinglePlannerQuery, context: LogicalPlanningContext): (BestPlans, LogicalPlanningContext)
}

trait TailPlanner {
  def apply(lhsPlans: BestPlans,
            tailQuery: SinglePlannerQuery,
            previousInterestingOrder: InterestingOrder,
            context: LogicalPlanningContext): (BestPlans, LogicalPlanningContext)
}

trait UpdatesPlanner {
  def apply(query: SinglePlannerQuery, in: LogicalPlan, firstPlannerQuery: Boolean, context: LogicalPlanningContext): LogicalPlan
}
