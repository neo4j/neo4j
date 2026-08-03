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

import org.neo4j.cypher.internal.compiler.planner.logical.ordering.InterestingOrderConfig
import org.neo4j.cypher.internal.compiler.planner.logical.steps.FulltextSearchLeafPlanner
import org.neo4j.cypher.internal.compiler.planner.logical.steps.VectorSearchLeafPlanner
import org.neo4j.cypher.internal.compiler.planner.logical.steps.planShortestRelationships
import org.neo4j.cypher.internal.ir.FulltextSearchClause
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.ir.ShortestRelationshipPattern
import org.neo4j.cypher.internal.ir.VectorSearchClause
import org.neo4j.cypher.internal.logical.plans.LogicalPlan

sealed trait SelectionPlanner {
  def apply(initialPlan: LogicalPlan, queryGraph: QueryGraph): LogicalPlan
}

object SelectionPlanner {

  case class PlanSelections(
    applySelections: PlanSelector,
    interestingOrderConfig: InterestingOrderConfig,
    context: LogicalPlanningContext
  ) extends SelectionPlanner {

    override def apply(initialPlan: LogicalPlan, queryGraph: QueryGraph): LogicalPlan =
      applySelections(initialPlan, queryGraph, interestingOrderConfig, context)
  }

  case class ShortestPathDecorator(wrappedSelectionPlanner: SelectionPlanner, context: LogicalPlanningContext)
      extends SelectionPlanner {

    override def apply(initialPlan: LogicalPlan, qg: QueryGraph): LogicalPlan = {
      val initialSolved = context.staticComponents.planningAttributes.solveds.get(initialPlan.id).asSinglePlannerQuery
      def alreadySolved(sp: ShortestRelationshipPattern): Boolean =
        initialSolved.exists(_.queryGraph.shortestRelationshipPatterns.contains(sp))

      qg.shortestRelationshipPatterns.foldLeft(wrappedSelectionPlanner(initialPlan, qg)) {
        case (plan, sp) if !alreadySolved(sp) && sp.isFindableFrom(plan.availableSymbols) =>
          val shortestPath = planShortestRelationships(plan, qg, sp, context)
          wrappedSelectionPlanner(shortestPath, qg)
        case (plan, _) => plan
      }
    }
  }

  case class SearchDecorator(wrappedSelectionPlanner: SelectionPlanner, context: LogicalPlanningContext)
      extends SelectionPlanner {

    override def apply(initialPlan: LogicalPlan, qg: QueryGraph): LogicalPlan = {
      val initialSolved = context.staticComponents.planningAttributes.solveds.get(initialPlan.id).asSinglePlannerQuery
      val alreadySolved = initialSolved.queryGraph.searchClause.nonEmpty
      val symbols = initialPlan.availableSymbols

      qg.searchClause.foldLeft(wrappedSelectionPlanner(initialPlan, qg)) {
        case (plan, search) if !alreadySolved && search.isSolvableGivenSymbols(symbols) =>
          val restrictedQueryGraph = qg.withArgumentIds(symbols.intersect(search.dependencies + search.resultVariable))
          val searchLeaves = search match {
            case _: VectorSearchClause =>
              VectorSearchLeafPlanner.apply(restrictedQueryGraph, InterestingOrderConfig.empty, context)
            case _: FulltextSearchClause =>
              FulltextSearchLeafPlanner.apply(restrictedQueryGraph, InterestingOrderConfig.empty, context)
          }
          assert(searchLeaves.size == 1, "Expected exactly one search leaf")
          val planWithSearch = searchLeaves.headOption.fold(plan) {
            rhs => context.staticComponents.logicalPlanProducer.planApply(plan, rhs, context)
          }
          wrappedSelectionPlanner(planWithSearch, qg)
        case (plan, _) => plan
      }
    }
  }
}
