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

import org.neo4j.cypher.internal.compiler.planner.logical.CandidateSelector
import org.neo4j.cypher.internal.compiler.planner.logical.CostModelMonitor
import org.neo4j.cypher.internal.compiler.planner.logical.LogicalPlanningContext
import org.neo4j.cypher.internal.compiler.planner.logical.SelectorHeuristic
import org.neo4j.cypher.internal.logical.plans.LogicalPlan

trait CandidateSelectorFactory {
  def apply(context: LogicalPlanningContext): CandidateSelector
}

object pickBestPlanUsingHintsAndCost extends CandidateSelectorFactory {

  override def apply(context: LogicalPlanningContext): CandidateSelector =
    new CandidateSelector {

      override def applyWithResolvedPerPlan[X](
        projector: X => LogicalPlan,
        input: Iterable[X],
        resolved: => String,
        resolvedPerPlan: LogicalPlan => String,
        heuristic: SelectorHeuristic
      ): Option[X] = {
        context.staticComponents.costComparisonListener.report(
          projector,
          input,
          (plan: X) => score(projector, plan, heuristic, context),
          context,
          resolved,
          resolvedPerPlan,
          heuristic
        )

        // don't run minBy for only one element, since that will unnecessary call score() for that element
        input.size match {
          case 0 => None
          case 1 => Some(input.head)
          case _ => Some(input.minBy(score(projector, _, heuristic, context)))
        }
      }
    }

  private def score[X](
    projector: X => LogicalPlan,
    input: X,
    heuristic: SelectorHeuristic,
    context: LogicalPlanningContext
  ) = {
    val costs = context.cost
    val plan = projector(input)
    val cost = costs.costFor(
      plan,
      context.plannerState.input,
      context.semanticTable,
      context.staticComponents.planningAttributes.cardinalities,
      context.staticComponents.planningAttributes.providedOrders,
      context.plannerState.accessedAndAggregatingProperties,
      context.statistics,
      CostModelMonitor.DEFAULT
    ).gummyBears
    val hints = context.staticComponents.planningAttributes.solveds.get(plan.id).numHints
    val tieBreaker = heuristic.tieBreaker(plan)
    (-hints, cost, -tieBreaker)
  }
}
