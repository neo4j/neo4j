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

import org.neo4j.cypher.internal.compiler.planner.logical.idp.BestResults
import org.neo4j.cypher.internal.compiler.planner.logical.limit.LimitSelectivityConfig
import org.neo4j.cypher.internal.compiler.planner.logical.steps.BestPlans
import org.neo4j.cypher.internal.ir.SinglePlannerQuery
import org.neo4j.cypher.internal.ir.ordering.InterestingOrder
import org.neo4j.cypher.internal.logical.plans.LogicalPlan

import scala.util.chaining.scalaUtilChainingOps

/*
This coordinates PlannerQuery planning and delegates work to the classes that do the actual planning of
QueryGraphs and EventHorizons
 */
case class PlanSingleQuery(headPlanner: HeadPlanner = PlanHead(), tailPlanner: TailPlanner = PlanWithTail()) {

  private type StepResult = (BestPlans, LogicalPlanningContext)

  def plan(query: SinglePlannerQuery, context: LogicalPlanningContext): LogicalPlan = {
    val limitSelectivityConfigs = LimitSelectivityConfig.forAllParts(query, context)

    val bestPlan = {
      context
        .pipe { context =>
          headPlanner.plan(
            query,
            context.withModifiedPlannerState(_.withLimitSelectivityConfig(limitSelectivityConfigs.head))
          )
        }
        .pipe { case (plans, context) =>
          planRemainingParts(plans, query, context, limitSelectivityConfigs)
        }
        .pipe { case (plans, context) =>
          (plans, context.withModifiedPlannerState(_.withLimitSelectivityConfig(LimitSelectivityConfig.default)))
        }
        .pipe { case (plans, context) =>
          val pickBest = context.plannerState.config.pickBestCandidate(context)
          val bestPlan = pickBest(plans.allResults.to(Iterable), s"best finalized plan for ${query.queryGraph}")
          bestPlan
        }
    }

    bestPlan.getOrElse(throw new IllegalStateException("Error planning single query, no best plan found"))
  }

  private def planRemainingParts(
    plans: BestPlans,
    query: SinglePlannerQuery,
    context: LogicalPlanningContext,
    limitSelectivityConfigs: List[LimitSelectivityConfig]
  ): StepResult = {
    val remainingPartsWithExtras = {
      val allParts = query.allPlannerQueries
      assert(
        allParts.length == limitSelectivityConfigs.length,
        "We should have limit selectivities for all query parts."
      )
      allParts.tail.lazyZip(limitSelectivityConfigs.tail).lazyZip(allParts)
    }

    remainingPartsWithExtras.foldLeft((plans, context)) {
      case ((plans, context), (plannerQuery, limitSelectivityConfig, prevPlannerQuery)) =>
        // If the current query graph is empty (except for arguments), that means there is no clause between
        // a previous WITH (that could have an ORDER BY) and the next WITH/RETURN.
        // In this case the next WITH/RETURN is allowed to leverage the ORDER BY of the previous one.
        val previousInterestingOrder =
          if (plannerQuery.queryGraph.withArgumentIds(Set.empty).isEmpty) Some(prevPlannerQuery.interestingOrder)
          else None

        tailPlanner.plan(
          plans,
          plannerQuery,
          previousInterestingOrder,
          context.withModifiedPlannerState(_
            .withLimitSelectivityConfig(limitSelectivityConfig)
            .withLastSolvedPlannerQuery(prevPlannerQuery))
        )
    }
  }
}

sealed trait PlannerType

object PlannerType {
  case object Match extends PlannerType
  case object Horizon extends PlannerType
}

trait MatchPlanner {
  protected def doPlan(query: SinglePlannerQuery, context: LogicalPlanningContext, rhsPart: Boolean): BestPlans

  final def plan(query: SinglePlannerQuery, context: LogicalPlanningContext, rhsPart: Boolean = false): BestPlans = {
    context.staticComponents.planningStepsLogger.log(
      s"Planning MATCH for ${query.queryGraph.withMutatingPattern(IndexedSeq.empty)}"
    )
    doPlan(query, context.withModifiedPlannerState(_.withActivePlanner(PlannerType.Match)), rhsPart)
  }
}

trait EventHorizonPlanner {

  /**
   * @param prevInterestingOrder The previous interesting order, if it exists, and only if the plannerQuery has an empty query graph.
   */
  protected def doPlanHorizon(
    plannerQuery: SinglePlannerQuery,
    incomingPlans: BestResults[LogicalPlan],
    prevInterestingOrder: Option[InterestingOrder],
    context: LogicalPlanningContext
  ): BestResults[LogicalPlan]

  /**
   * @param prevInterestingOrder The previous interesting order, if it exists, and only if the plannerQuery has an empty query graph.
   */
  final def planHorizon(
    plannerQuery: SinglePlannerQuery,
    incomingPlans: BestResults[LogicalPlan],
    prevInterestingOrder: Option[InterestingOrder],
    context: LogicalPlanningContext
  ): BestResults[LogicalPlan] = {
    context.staticComponents.planningStepsLogger.log(
      s"""Planning HORIZON
         |  ${plannerQuery.horizon}:
         |  on top of ${stringify(incomingPlans)}""".stripMargin
    )
    doPlanHorizon(
      plannerQuery,
      incomingPlans,
      prevInterestingOrder,
      context.withModifiedPlannerState(_.withActivePlanner(PlannerType.Horizon))
    )
  }

  def stringify(value: BestResults[LogicalPlan]): String = {
    val BestResults(bestResult, bestSortedResult, bestExtraPropertiesResult) = value
    val indent = "    "

    def stringify(plan: LogicalPlan): String =
      s"""Plan #${plan.debugId}
         |$indent${plan.toString.replace("\n", s"\n$indent")}""".stripMargin

    s"""BestResults(
       |  bestResult = ${stringify(bestResult)},
       |  bestSortedResult = ${bestSortedResult.map(stringify).getOrElse("None")},
       |  bestExtraPropertiesResult = ${bestExtraPropertiesResult.map(stringify).getOrElse("None")}
       |)""".stripMargin
  }
}

trait HeadPlanner {
  def plan(headQuery: SinglePlannerQuery, context: LogicalPlanningContext): (BestPlans, LogicalPlanningContext)
}

trait TailPlanner {

  /**
   * @param previousInterestingOrder The previous interesting order, if it exists, and only if the tailQuery has an empty query graph.
   */
  def plan(
    lhsPlans: BestPlans,
    tailQuery: SinglePlannerQuery,
    previousInterestingOrder: Option[InterestingOrder],
    context: LogicalPlanningContext
  ): (BestPlans, LogicalPlanningContext)
}

trait UpdatesPlanner {

  def plan(
    query: SinglePlannerQuery,
    in: LogicalPlan,
    firstPlannerQuery: Boolean,
    context: LogicalPlanningContext
  ): LogicalPlan
}
