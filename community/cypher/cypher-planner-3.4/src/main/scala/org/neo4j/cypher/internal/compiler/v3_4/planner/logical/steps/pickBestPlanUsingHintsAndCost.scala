/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.cypher.internal.compiler.v3_4.planner.logical.steps

import org.neo4j.cypher.internal.compiler.v3_4.planner.logical.{CandidateSelector, LogicalPlanningContext}
import org.neo4j.cypher.internal.planner.v3_4.spi.PlanningAttributes.{Cardinalities, Solveds}
import org.neo4j.cypher.internal.v3_4.logical.plans.LogicalPlan

trait CandidateSelectorFactory extends ((LogicalPlanningContext, Solveds, Cardinalities) => CandidateSelector)

object pickBestPlanUsingHintsAndCost extends CandidateSelectorFactory {
  private val baseOrdering = implicitly[Ordering[(Int, Double, Int)]]

  override def apply(context: LogicalPlanningContext, solveds: Solveds, cardinalities: Cardinalities): CandidateSelector =
    new CandidateSelector {
      override def apply[X](projector: X => LogicalPlan, input: Iterable[X]): Option[X] = {

        val inputOrdering = new Ordering[X] {
          override def compare(x: X, y: X): Int = {
            val xCost = score(projector, x, context, solveds, cardinalities)
            val yCost = score(projector, y, context, solveds, cardinalities)
            baseOrdering.compare(xCost, yCost)
          }
        }

        context.costComparisonListener.report(projector, input, inputOrdering, context, solveds, cardinalities)

        if (input.isEmpty) None else Some(input.min(inputOrdering))
      }
    }

  private def score[X](projector: X => LogicalPlan, input: X, context: LogicalPlanningContext, solveds: Solveds, cardinalities: Cardinalities) = {
    val costs = context.cost
    val plan = projector(input)
    (-solveds.get(plan.id).numHints, costs(plan, context.input, cardinalities).gummyBears, -plan.availableSymbols.size)
  }
}
