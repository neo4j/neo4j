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

import org.neo4j.cypher.internal.compiler.planner.logical.CostModelMonitor
import org.neo4j.cypher.internal.compiler.planner.logical.LogicalPlanningContext
import org.neo4j.cypher.internal.compiler.planner.logical.SelectorHeuristic
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.LogicalPlanToPlanBuilderString
import org.neo4j.cypher.internal.options.CypherDebugOptions
import org.neo4j.cypher.internal.util.Cardinality
import org.neo4j.cypher.internal.util.Cost
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.logging.Log

import java.util.concurrent.atomic.AtomicLong

import scala.collection.mutable
import scala.io.AnsiColor

trait CostComparisonListener {

  def report[X, Score: Ordering](
    projector: X => LogicalPlan,
    input: Iterable[X],
    calculateScore: X => Score,
    context: LogicalPlanningContext,
    resolved: => String,
    resolvedPerPlan: LogicalPlan => String = _ => "",
    heuristic: SelectorHeuristic
  ): Unit
}

object CostComparisonListener {

  def givenDebugOptions(options: CypherDebugOptions, log: Log): CostComparisonListener =
    List(
      Option.when(
        options.printCostComparisonsEnabled || java.lang.Boolean.getBoolean("pickBestPlan.VERBOSE")
      )(new SystemOutCostLogger()),
      Option.when(options.logCostComparisonsEnabled)(new DebugCostLogger(log))
    ).flatten match {
      case Nil             => devNullListener
      case listener :: Nil => listener
      case listeners       => new CombinedListener(listeners)
    }
}

object devNullListener extends CostComparisonListener {

  override def report[X, Score: Ordering](
    projector: X => LogicalPlan,
    input: Iterable[X],
    calculateScore: X => Score,
    context: LogicalPlanningContext,
    resolved: => String,
    resolvedPerPlan: LogicalPlan => String = _ => "",
    heuristic: SelectorHeuristic
  ): Unit = {}
}

final class CombinedListener(listeners: List[CostComparisonListener]) extends CostComparisonListener {

  override def report[X, Score: Ordering](
    projector: X => LogicalPlan,
    input: Iterable[X],
    calculateScore: X => Score,
    context: LogicalPlanningContext,
    resolved: => String,
    resolvedPerPlan: LogicalPlan => String = _ => "",
    heuristic: SelectorHeuristic
  ): Unit =
    listeners.foreach(_.report(projector, input, calculateScore, context, resolved, resolvedPerPlan, heuristic))
}

abstract class CostLogger extends CostComparisonListener {
  private val comparisonId = new AtomicLong()

  private val prefix = "\t"

  private def indent(level: Int, str: String): String = {
    val ind = prefix * level
    ind + str.replace(System.lineSeparator(), System.lineSeparator() + ind)
  }

  protected def blue(str: String): String
  protected def blue_bold(str: String): String
  protected def cyan(str: String): String
  protected def cyan_bold(str: String): String
  protected def cyan_background(str: String): String
  protected def green(str: String): String
  protected def magenta(str: String): String
  protected def magenta_bold(str: String): String

  protected def printLine(str: String): Unit

  override def report[X, Score: Ordering](
    projector: X => LogicalPlan,
    input: Iterable[X],
    calculateScore: X => Score,
    context: LogicalPlanningContext,
    resolved: => String,
    resolvedPerPlan: LogicalPlan => String = _ => "",
    heuristic: SelectorHeuristic
  ): Unit = {
    // Key is a tuple of (root plan ID, plan ID)
    val planCost: mutable.Map[(Id, Id), Cost] = mutable.Map.empty
    val planEffectiveCardinality: mutable.Map[(Id, Id), Cardinality] = mutable.Map.empty

    val monitor = new CostModelMonitor {
      override def reportPlanCost(rootPlan: LogicalPlan, plan: LogicalPlan, cost: Cost): Unit =
        planCost += ((rootPlan.id, plan.id) -> cost)
      override def reportPlanEffectiveCardinality(
        rootPlan: LogicalPlan,
        plan: LogicalPlan,
        cardinality: Cardinality
      ): Unit = planEffectiveCardinality += ((rootPlan.id, plan.id) -> cardinality)
    }

    def costString(rootPlan: LogicalPlan)(plan: LogicalPlan) = {
      val cost = planCost((rootPlan.id, plan.id)).gummyBears
      val cardinality = context.staticComponents.planningAttributes.cardinalities.get(plan.id).amount
      val effectiveCardinality = planEffectiveCardinality((rootPlan.id, plan.id)).amount
      val costStr = magenta(" // cost ") + magenta_bold(cost.toString)
      val cardStr = magenta(", cardinality ") + magenta_bold(cardinality.toString)
      val effCardStr =
        if (cardinality > effectiveCardinality)
          cyan(" (effective cardinality ") + cyan_bold(effectiveCardinality.toString) + cyan(")")
        else ""
      costStr + cardStr + effCardStr
    }

    def planPrefixDotString(rootPlan: LogicalPlan)(plan: LogicalPlan) = {
      val cardinality = context.staticComponents.planningAttributes.cardinalities.get(plan.id).amount
      val effectiveCardinality = planEffectiveCardinality((rootPlan.id, plan.id)).amount
      if (cardinality > effectiveCardinality) cyan_background(".") else "."
    }

    if (input.nonEmpty) {
      val id = comparisonId.getAndIncrement()
      printLine(blue_bold(s"$id: Resolving $resolved"))
      printLine(s"Get best of:")

      input
        .to(LazyList) // working lazily as much as possible to traverse input as few times as necessary
        .distinct // lazy via View.DistinctBy
        .map(x =>
          x -> calculateScore(x)
        ) // calculate the score for each value, only once, populating `planCost` and `planEffectiveCardinality`
        .sortBy(_._2) // sort by score, actually a strict operation under the hood so `calculateScore` actually gets called at this point
        .map(_._1) // ditch the score
        .map(projector) // get the LogicalPlan
        .zipWithIndex
        .foreach { case (plan, index) =>
          // Update cost and effective cardinality for each subplan
          context.cost.costFor(
            plan,
            context.plannerState.input,
            context.semanticTable,
            context.staticComponents.planningAttributes.cardinalities,
            context.staticComponents.planningAttributes.providedOrders,
            context.plannerState.accessedAndAggregatingProperties,
            context.statistics,
            monitor
          )
          val winner = if (index == 0) green(" [winner]") else ""
          val resolvedStr = cyan(s" ${resolvedPerPlan(plan)}")
          val header = blue(s"$index: Plan #${plan.debugId}") + winner + resolvedStr
          val planWithCosts =
            LogicalPlanToPlanBuilderString(plan, extra = costString(plan), planPrefixDot = planPrefixDotString(plan))
          val hints = context.staticComponents.planningAttributes.solveds.get(plan.id).numHints
          val heuristicValue = heuristic.tieBreaker(plan)
          val extra = s"(hints: $hints, heuristic: $heuristicValue)"

          printLine(indent(1, header))
          printLine(indent(2, planWithCosts))
          printLine(indent(2, extra))
          printLine("")
        }
    }
  }
}

final class SystemOutCostLogger() extends CostLogger {
  override protected def blue(str: String): String = AnsiColor.BLUE + str + AnsiColor.RESET
  override protected def blue_bold(str: String): String = AnsiColor.BLUE + AnsiColor.BOLD + str + AnsiColor.RESET
  override protected def cyan(str: String): String = AnsiColor.CYAN + str + AnsiColor.RESET

  override protected def cyan_bold(str: String): String =
    AnsiColor.CYAN + AnsiColor.UNDERLINED + AnsiColor.BOLD + str + AnsiColor.RESET
  override protected def cyan_background(str: String): String = AnsiColor.CYAN_B + str + AnsiColor.RESET
  override protected def green(str: String): String = AnsiColor.GREEN + str + AnsiColor.RESET
  override protected def magenta(str: String): String = AnsiColor.MAGENTA + str + AnsiColor.RESET

  override protected def magenta_bold(str: String): String =
    AnsiColor.MAGENTA + AnsiColor.UNDERLINED + AnsiColor.BOLD + str + AnsiColor.RESET

  override protected def printLine(str: String): Unit = println(str)
}

final class DebugCostLogger(log: Log) extends CostLogger {
  override protected def blue(str: String): String = str
  override protected def blue_bold(str: String): String = str
  override protected def cyan(str: String): String = str
  override protected def cyan_bold(str: String): String = str
  override protected def cyan_background(str: String): String = str
  override protected def green(str: String): String = str
  override protected def magenta(str: String): String = str
  override protected def magenta_bold(str: String): String = str

  override protected def printLine(str: String): Unit = log.debug(str)
}
