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
package org.neo4j.cypher.internal.compiler.v2_3.planner.logical.steps

import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.{CandidateSelector, LogicalPlanningContext, LogicalPlanningFunction0}

object pickBestPlanUsingHintsAndCost extends LogicalPlanningFunction0[CandidateSelector] {
  val VERBOSE = java.lang.Boolean.getBoolean("pickBestPlan.VERBOSE")
  private val baseOrdering = implicitly[Ordering[(Int, Double, Int)]]

  override def apply(implicit context: LogicalPlanningContext): CandidateSelector = new CandidateSelector {
    override def apply[X](projector: (X) => LogicalPlan, input: Iterable[X]): Option[X] = {
      val inputOrdering = new Ordering[X] {
        override def compare(x: X, y: X): Int = baseOrdering.compare(score(projector, x), score(projector, y))
      }

      if (VERBOSE) {
        val costs = context.cost
        val sortedPlans = input.toSeq.sorted(inputOrdering).map(projector)

        if (sortedPlans.size > 1) {
          println("- Get best of:")
          for (plan <- sortedPlans) {
            println(s"\t* Plan #${plan.debugId}")
            println(s"\t\t${plan.toString}")
            println(s"\t\t\t${costs(plan, context.input)}")
            println(s"\t\t\t${plan.solved.estimatedCardinality}")
            println(s"\t\t\tHints(${plan.solved.numHints})")
            println(s"\t\t\tlhs: ${plan.lhs}")
          }

          val best = sortedPlans.head
          println("- Best is:")
          println(s"\tPlan #${best.debugId}")
          println(s"\t\t${best.toString}")
          println(s"\t\t\t${costs(best, context.input)}")
          println(s"\t\t\t${best.solved.estimatedCardinality}")
          println(s"\t\t\tHints(${best.solved.numHints})")
          println(s"\t\t\tlhs: ${best.lhs}")
          println()
        }
      }

      if (input.isEmpty) None else Some(input.min(inputOrdering))
    }
  }

  private def score[X](projector: (X) => LogicalPlan, input: X)(implicit context: LogicalPlanningContext) = {
    val costs = context.cost
    val plan = projector(input)
    (-plan.solved.numHints, costs(plan, context.input).gummyBears, -plan.availableSymbols.size)
  }
}
