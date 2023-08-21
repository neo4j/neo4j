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
import org.neo4j.cypher.internal.logical.plans.LogicalPlan

trait ProjectingSelector[P] {

  def apply(plans: Iterable[P], resolved: => String): Option[P] =
    applyWithResolvedPerPlan[P](identity, plans, resolved, _ => "", SelectorHeuristic.constant)

  def apply(plans: Iterable[P], heuristic: SelectorHeuristic, resolved: => String): Option[P] =
    applyWithResolvedPerPlan[P](identity, plans, resolved, _ => "", heuristic)

  def applyWithResolvedPerPlan(
    plans: Iterable[P],
    resolved: => String,
    resolvedPerPlan: LogicalPlan => String
  ): Option[P] = applyWithResolvedPerPlan[P](identity, plans, resolved, resolvedPerPlan, SelectorHeuristic.constant)

  def apply[X](projector: X => P, input: Iterable[X], resolved: => String): Option[X] =
    applyWithResolvedPerPlan[X](projector, input, resolved, _ => "", SelectorHeuristic.constant)

  def applyWithResolvedPerPlan[X](
    projector: X => P,
    input: Iterable[X],
    resolved: => String,
    resolvedPerPlan: LogicalPlan => String,
    heuristic: SelectorHeuristic
  ): Option[X]

  def ofBestResults(
    plans: Iterable[BestResults[P]],
    resolved: => String,
    resolvedPerPan: LogicalPlan => String
  ): Option[BestResults[P]] = {
    val best = applyWithResolvedPerPlan(plans.map(_.bestResult), s"overall $resolved", resolvedPerPan)
    val bestFulfillingReq =
      applyWithResolvedPerPlan(plans.flatMap(_.bestResultFulfillingReq), s"sorted $resolved", resolvedPerPan)
    best.map(BestResults(_, bestFulfillingReq))
  }
}

object SelectorHeuristic {
  val constant: SelectorHeuristic = (_: LogicalPlan) => 0
}

@FunctionalInterface
trait SelectorHeuristic {

  /** Heuristic used to break ties between plans with the same cost */
  def tieBreaker(plan: LogicalPlan): Int
}
