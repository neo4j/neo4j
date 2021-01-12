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
package org.neo4j.cypher.internal.compiler.planner.logical

import org.neo4j.cypher.internal.compiler.planner.logical.ordering.InterestingOrderConfig
import org.neo4j.cypher.internal.compiler.planner.logical.steps.BestPlans
import org.neo4j.cypher.internal.ir.QueryProjection
import org.neo4j.cypher.internal.ir.SinglePlannerQuery
import org.neo4j.cypher.internal.ir.ordering.InterestingOrder
import org.neo4j.cypher.internal.util.Selectivity

import scala.annotation.tailrec

case object planMatch extends MatchPlanner {

  def apply(query: SinglePlannerQuery, context: LogicalPlanningContext, rhsPart: Boolean = false): BestPlans = {
    val limitSelectivity = limitSelectivityForRestOfQuery(query, context)

    context.strategy.plan(
      query.queryGraph,
      interestingOrderForPart(query, rhsPart),
      context.withLimitSelectivity(limitSelectivity))
  }

  // Extract the interesting InterestingOrder for this part of the query
  // If the required order has dependency on argument, then it should not solve the ordering here
  // If we have a mutating pattern that depends on the sorting variables, we cannot solve ordering here
  private def interestingOrderForPart(query: SinglePlannerQuery, isRhs: Boolean): InterestingOrderConfig = {
    if (isRhs) {
      InterestingOrderConfig(query.interestingOrder.asInteresting)
    }
    else {
      val orderToReport = query.interestingOrder
      val orderToSolve = query.findFirstRequiredOrder.fold(orderToReport) { order =>
        // merge interesting order candidates
        val existing = order.interestingOrderCandidates.toSet
        val extraCandidates = orderToReport.interestingOrderCandidates.filterNot(existing.contains)
        order.copy(interestingOrderCandidates = order.interestingOrderCandidates ++ extraCandidates)
      }
      val mutatingDependencies = query.queryGraph.mutatingPatterns.flatMap(_.dependencies)
      if (hasDependencies(orderToReport, mutatingDependencies) || hasDependencies(orderToSolve, mutatingDependencies)) {
        InterestingOrderConfig(orderToReport.asInteresting)
      } else {
        InterestingOrderConfig(orderToReport = orderToReport, orderToSolve = orderToSolve)
      }
    }
  }

  private def hasDependencies(interestingOrder: InterestingOrder, dependencies: Seq[String]): Boolean = {
    val orderCandidate = interestingOrder.requiredOrderCandidate.order
    val orderingDependencies = orderCandidate.flatMap(_.projections).flatMap(_._2.dependencies) ++ orderCandidate.flatMap(_.expression.dependencies)
    orderingDependencies.exists(dep => dependencies.contains(dep.name))
  }

  private[logical] def limitSelectivityForRestOfQuery(query: SinglePlannerQuery, context: LogicalPlanningContext): Selectivity = {
    @tailrec
    def recurse(query: SinglePlannerQuery, context: LogicalPlanningContext, parentLimitSelectivity: Selectivity): Selectivity = {
      val lastPartSelectivity = limitSelectivityForPart(query, context, parentLimitSelectivity)

      query.withoutLast match {
        case None => lastPartSelectivity
        case Some(withoutLast) =>
          val currentSelectivity = lastPartSelectivity
          recurse(withoutLast, context, currentSelectivity)
      }
    }

    recurse(query, context, Selectivity.ONE)
  }

  private[logical] def limitSelectivityForPart(query: SinglePlannerQuery, context: LogicalPlanningContext, parentLimitSelectivity: Selectivity): Selectivity = {
    if (!query.readOnly) {
       Selectivity.ONE
    } else {
      query.lastQueryHorizon match {
        case proj: QueryProjection if proj.queryPagination.limit.isDefined =>
          val queryWithoutLimit = query.updateTailOrSelf(_.updateQueryProjection(_ => proj.withPagination(proj.queryPagination.withLimit(None))))
          val cardinalityModel = context.metrics.cardinality(_, context.input, context.semanticTable)

          val cardinalityWithoutLimit = cardinalityModel(queryWithoutLimit)
          val cardinalityWithLimit = cardinalityModel(query)

          CardinalityCostModel.limitingPlanSelectivity(cardinalityWithoutLimit, cardinalityWithLimit, parentLimitSelectivity)

        case _ => parentLimitSelectivity
      }
    }
  }
}
