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
        context.costComparisonListener.report(
          projector,
          input,
          (plan: X, monitor) => score(projector, plan, heuristic, context, monitor),
          context,
          resolved,
          resolvedPerPlan,
          heuristic
        )

        input.minByOption(score(projector, _, heuristic, context, CostModelMonitor.DEFAULT))
      }
    }

  private def score[X](
    projector: X => LogicalPlan,
    input: X,
    heuristic: SelectorHeuristic,
    context: LogicalPlanningContext,
    costModelMonitor: CostModelMonitor
  ) = {
    val costs = context.cost
    val plan = projector(input)
    val cost = costs.costFor(
      plan,
      context.input,
      context.semanticTable,
      context.planningAttributes.cardinalities,
      context.planningAttributes.providedOrders,
      costModelMonitor
    ).gummyBears
    val hints = context.planningAttributes.solveds.get(plan.id).numHints
    val tieBreaker = heuristic.tieBreaker(plan)
    (-hints, cost, -tieBreaker)
  }
}
