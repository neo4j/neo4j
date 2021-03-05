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

import org.neo4j.cypher.internal.compiler.planner.logical.CostModelMonitor
import org.neo4j.cypher.internal.compiler.planner.logical.LogicalPlanningContext
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.LogicalPlanToPlanBuilderString
import org.neo4j.cypher.internal.util.Cardinality
import org.neo4j.cypher.internal.util.Cost
import org.neo4j.cypher.internal.util.attribution.Id

import java.util.concurrent.atomic.AtomicLong
import scala.collection.mutable
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
  private def blue_bold(str: String) = AnsiColor.BLUE + AnsiColor.BOLD + str + AnsiColor.RESET
  private def cyan(str: String) = AnsiColor.CYAN + str + AnsiColor.RESET
  private def cyan_bold(str: String) = AnsiColor.CYAN + AnsiColor.UNDERLINED + AnsiColor.BOLD + str + AnsiColor.RESET
  private def cyan_background(str: String) = AnsiColor.CYAN_B + str + AnsiColor.RESET
  private def green(str: String) = AnsiColor.GREEN + str + AnsiColor.RESET
  private def magenta(str: String) = AnsiColor.MAGENTA + str + AnsiColor.RESET
  private def magenta_bold(str: String) = AnsiColor.MAGENTA + AnsiColor.UNDERLINED + AnsiColor.BOLD + str + AnsiColor.RESET
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
    // Key is a tuple of (root plan ID, plan ID)
    val planCost: mutable.Map[(Id, Id), Cost] = mutable.Map.empty
    val planEffectiveCardinality: mutable.Map[(Id, Id), Cardinality] = mutable.Map.empty

    val monitor = new CostModelMonitor {
      override def reportPlanCost(rootPlan: LogicalPlan, plan: LogicalPlan, cost: Cost): Unit = planCost += ((rootPlan.id, plan.id) -> cost)
      override def reportPlanEffectiveCardinality(rootPlan: LogicalPlan, plan: LogicalPlan, cardinality: Cardinality): Unit = planEffectiveCardinality += ((rootPlan.id, plan.id) -> cardinality)
    }

    def costString(rootPlan: LogicalPlan)(plan: LogicalPlan) = {
      val cost = planCost((rootPlan.id, plan.id)).gummyBears
      val cardinality = context.planningAttributes.cardinalities.get(plan.id).amount
      val effectiveCardinality = planEffectiveCardinality((rootPlan.id, plan.id)).amount
      val costStr = magenta(" // cost ") + magenta_bold(cost.toString)
      val cardStr = magenta(", cardinality ") + magenta_bold(cardinality.toString)
      val effCardStr =  if (cardinality > effectiveCardinality) cyan(" (effective cardinality ") + cyan_bold(effectiveCardinality.toString) + cyan(")") else ""
      costStr + cardStr + effCardStr
    }

    def planPrefixDotString(rootPlan: LogicalPlan)(plan: LogicalPlan) = {
      val cardinality = context.planningAttributes.cardinalities.get(plan.id).amount
      val effectiveCardinality = planEffectiveCardinality((rootPlan.id, plan.id)).amount
      if (cardinality > effectiveCardinality) cyan_background(".") else "."
    }

    val plansInOrder = input.toIndexedSeq.distinct.sorted(inputOrdering).map(projector)

    // Update cost and effective cardinality for each subplan
    plansInOrder.foreach(
      plan =>
        context.cost.costFor(plan, context.input, context.semanticTable, context.planningAttributes.cardinalities, context.planningAttributes.providedOrders, monitor)
    )

    if (plansInOrder.nonEmpty) {
      val id = comparisonId.getAndIncrement()
      println(blue_bold(s"$id: Resolving $resolved"))
      println(s"Get best of:")
      for ((plan, index) <- plansInOrder.zipWithIndex) {
        val winner = if (index == 0) green(" [winner]") else ""
        val resolvedStr = cyan(s" ${resolvedPerPlan(plan)}")
        val header = blue(s"$index: Plan #${plan.debugId}") + winner + resolvedStr
        val planWithCosts = LogicalPlanToPlanBuilderString(plan, extra = costString(plan), planPrefixDot = planPrefixDotString(plan))
        val hints = s"(hints: ${context.planningAttributes.solveds.get(plan.id).numHints})"

        println(indent(1, header))
        println(indent(2, planWithCosts))
        println(indent(2, hints))
        println()
      }
    }
  }
}
