/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

import java.util.concurrent.atomic.AtomicLong

import org.neo4j.cypher.internal.compiler.planner.logical.LogicalPlanningContext
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.LogicalPlanToPlanBuilderString

import scala.io.AnsiColor

trait CostComparisonListener {
  def report[X](projector: X => LogicalPlan,
                input: Iterable[X],
                inputOrdering: Ordering[X],
                context: LogicalPlanningContext,
                resolved: => String,
                resolvedPerPlan: LogicalPlan => String = _ => ""
               ): Unit
}

object devNullListener extends CostComparisonListener {
  override def report[X](projector: X => LogicalPlan,
                         input: Iterable[X],
                         inputOrdering: Ordering[X],
                         context: LogicalPlanningContext,
                         resolved: => String,
                         resolvedPerPlan: LogicalPlan => String = _ => ""
                        ): Unit = {}
}

object SystemOutCostLogger extends CostComparisonListener {

  private val comparisonId = new AtomicLong()
  private val prefix = "\t"
  private def blue(str: String) = AnsiColor.BLUE + str + AnsiColor.RESET
  private def cyan(str: String) = AnsiColor.CYAN + str + AnsiColor.RESET
  private def green(str: String) = AnsiColor.GREEN + str + AnsiColor.RESET
  private def magenta(str: String) = AnsiColor.MAGENTA + str + AnsiColor.RESET
  private def magenta_bold(str: String) = AnsiColor.MAGENTA + AnsiColor.BOLD + AnsiColor.UNDERLINED + str + AnsiColor.RESET
  private def indent(level: Int, str: String) = {
    val ind = prefix * level
    ind + str.replaceAll(System.lineSeparator(), System.lineSeparator() + ind)
  }

  def report[X](projector: X => LogicalPlan,
                input: Iterable[X],
                inputOrdering: Ordering[X],
                context: LogicalPlanningContext,
                resolved: => String,
                resolvedPerPlan: LogicalPlan => String = _ => ""
               ): Unit = {

    def costString(plan: LogicalPlan) = {
      val cost = context.cost.costFor(plan, context.input, context.semanticTable, context.planningAttributes.cardinalities, context.planningAttributes.providedOrders).gummyBears
      val cardinality = context.planningAttributes.cardinalities.get(plan.id).amount
      magenta(" // cost ") + magenta_bold(cost.toString) + magenta(" cardinality ") + magenta_bold(cardinality.toString)
    }

    val plansInOrder = input.toIndexedSeq.sorted(inputOrdering).map(projector)

    if (plansInOrder.size > 1) {
      val id = comparisonId.getAndIncrement()
      println(cyan(s"$id: Resolving $resolved"))
      println(s"Get best of:")
      for ((plan, index) <- plansInOrder.zipWithIndex) {
        val winner = if (index == 0) green(" [winner]") else ""
        val resolvedStr = cyan(s" ${resolvedPerPlan(plan)}")
        val header = blue(s"$index: Plan #${plan.debugId}") + winner + resolvedStr
        val planWithCosts = LogicalPlanToPlanBuilderString(plan, extra = costString)
        val hints = s"(hints: ${context.planningAttributes.solveds.get(plan.id).numHints})"

        println(indent(1, header))
        println(indent(2, planWithCosts))
        println(indent(2, hints))
        println()
      }
    }
  }
}
