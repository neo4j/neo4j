/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.cypher.internal.compiler.v3_2.planner.logical.steps

import org.neo4j.cypher.internal.compiler.v3_2.planner.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.compiler.v3_2.planner.logical.{CandidateSelector, LogicalPlanningContext, LogicalPlanningFunction0}

object pickBestPlanUsingHintsAndCost extends LogicalPlanningFunction0[CandidateSelector] {
  val VERBOSE = java.lang.Boolean.getBoolean("pickBestPlan.VERBOSE")
  private val baseOrdering = implicitly[Ordering[(Int, Double, Int)]]

  override def apply(implicit context: LogicalPlanningContext): CandidateSelector = new CandidateSelector {
    override def apply[X](projector: (X) => LogicalPlan, input: Iterable[X]): Option[X] = {

      def stringTo(level: Int, plan: LogicalPlan): String = {
        def indent(level: Int, in: String): String = level match {
          case 0 => in
          case _ => "\n" + "  " * level + in
        }

        val cost = context.cost(plan, context.input)
        val thisPlan = indent(level, s"${plan.getClass.getSimpleName} costs $cost cardinality ${plan.solved.estimatedCardinality}")
        val l = plan.lhs.map(p => stringTo(level + 1, p)).getOrElse("")
        val r = plan.rhs.map(p => stringTo(level + 1, p)).getOrElse("")
        thisPlan + l + r
      }

      val inputOrdering = new Ordering[X] {
        override def compare(x: X, y: X): Int = baseOrdering.compare(score(projector, x), score(projector, y))
      }

      if (VERBOSE) {
        val sortedPlans = input.toIndexedSeq.sorted(inputOrdering).map(projector)

        if (sortedPlans.size > 1) {
          println("- Get best of:")
          for (plan <- sortedPlans) {
            val planTextWithCosts = stringTo(0, plan).replaceAll(System.lineSeparator(), System.lineSeparator() + "\t\t")
            val planText = plan.toString.replaceAll(System.lineSeparator(), System.lineSeparator() + "\t\t")
            println("=-" * 10)
            println(s"* Plan #${plan.debugId}")
            println(s"\t$planTextWithCosts")
            println(s"\t$planText")
            println(s"\t\tHints(${plan.solved.numHints})")
            println(s"\t\tlhs: ${plan.lhs}")
          }

          val best = sortedPlans.head
          println("!¡" * 10)
          println("- Best is:")
          println(s"Plan #${best.debugId}")
          println(s"\t${best.toString}")
          val planTextWithCosts = stringTo(0, best)
          println(s"\t$planTextWithCosts")
          println(s"\t\tHints(${best.solved.numHints})")
          println(s"\t\tlhs: ${best.lhs}")
          println("!¡" * 10)
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
