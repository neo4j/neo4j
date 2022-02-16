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
import org.neo4j.cypher.internal.compiler.planner.logical.limit.LimitSelectivityConfig
import org.neo4j.cypher.internal.compiler.planner.logical.steps.BestPlans
import org.neo4j.cypher.internal.ir.SinglePlannerQuery
import org.neo4j.cypher.internal.ir.ordering.InterestingOrder
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.util.attribution.Attributes

import scala.language.implicitConversions

/*
This coordinates PlannerQuery planning and delegates work to the classes that do the actual planning of
QueryGraphs and EventHorizons
 */
case class PlanSingleQuery(headPlanner: HeadPlanner = PlanHead(),
                           tailPlanner: TailPlanner = PlanWithTail())
  extends SingleQueryPlanner {

  private type StepResult = (BestPlans, LogicalPlanningContext)

  override def plan(query: SinglePlannerQuery, context: LogicalPlanningContext): LogicalPlan = {
    // to enable for-comprehension syntax
    implicit def stepResult2Some(t: StepResult): Some[StepResult] = Some(t)

    val limitSelectivityConfigs = LimitSelectivityConfig.forAllParts(query, context)

    val Some(bestPlan) = for {
      (plans, context) <- headPlanner.plan(query, context.withLimitSelectivityConfig(limitSelectivityConfigs.head))
      (plans, context) <- planRemainingParts(plans, query, context, limitSelectivityConfigs)
      (plans, context) <- unnestEager(plans, context.withLimitSelectivityConfig(LimitSelectivityConfig.default))
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
                                 limitSelectivityConfigs: List[LimitSelectivityConfig]): StepResult = {
    val remainingPartsWithExtras = {
      val allParts = query.allPlannerQueries
      assert(allParts.length == limitSelectivityConfigs.length, "We should have limit selectivities for all query parts.")
      (allParts.tail, limitSelectivityConfigs.tail, allParts).zipped
    }

    remainingPartsWithExtras.foldLeft((plans, context)) {
      case ((plans, context), (queryPart, limitSelectivityConfig, prevQueryPart)) =>
        tailPlanner.plan(plans, queryPart, prevQueryPart.interestingOrder,
          context
            .withLimitSelectivityConfig(limitSelectivityConfig)
            .withLastSolvedQueryPart(prevQueryPart)
        )
    }
  }

  private def unnestEager(plans: BestPlans, context: LogicalPlanningContext): StepResult = {
    val unnest = EagerAnalyzer.unnestEager(
      context.planningAttributes.solveds,
      context.planningAttributes.cardinalities,
      context.planningAttributes.providedOrders,
      Attributes(context.idGen)
    )

    val unnestedPlans = plans.map(_.endoRewrite(unnest))
    (unnestedPlans, context)
  }
}

sealed trait PlannerType
object PlannerType {
  case object Match extends PlannerType
  case object Horizon extends PlannerType
}

trait MatchPlanner {
  protected def doPlan(query: SinglePlannerQuery, context: LogicalPlanningContext, rhsPart: Boolean): BestPlans

  final def plan(query: SinglePlannerQuery, context: LogicalPlanningContext, rhsPart: Boolean = false): BestPlans =
    doPlan(query, context.withActivePlanner(PlannerType.Match), rhsPart)
}

trait EventHorizonPlanner {
  protected def doPlanHorizon(plannerQuery: SinglePlannerQuery,
                              incomingPlans: BestResults[LogicalPlan],
                              prevInterestingOrder: Option[InterestingOrder],
                              context: LogicalPlanningContext): BestResults[LogicalPlan]

  final def planHorizon(plannerQuery: SinglePlannerQuery,
                        incomingPlans: BestResults[LogicalPlan],
                        prevInterestingOrder: Option[InterestingOrder],
                        context: LogicalPlanningContext): BestResults[LogicalPlan] =
    doPlanHorizon(plannerQuery, incomingPlans, prevInterestingOrder, context.withActivePlanner(PlannerType.Horizon))
}

trait HeadPlanner {
  def plan(headQuery: SinglePlannerQuery, context: LogicalPlanningContext): (BestPlans, LogicalPlanningContext)
}

trait TailPlanner {
  def plan(lhsPlans: BestPlans,
            tailQuery: SinglePlannerQuery,
            previousInterestingOrder: InterestingOrder,
            context: LogicalPlanningContext): (BestPlans, LogicalPlanningContext)
}

trait UpdatesPlanner {
  def plan(query: SinglePlannerQuery, in: LogicalPlan, firstPlannerQuery: Boolean, context: LogicalPlanningContext): LogicalPlan
}
