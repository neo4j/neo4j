/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_1.planner.logical

import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.{SingleRow, QueryPlan}
import org.neo4j.cypher.internal.helpers.Converge.iterateUntilConverged
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.steps.cartesianProduct

class GreedyQueryGraphSolver(config: PlanningStrategyConfiguration = PlanningStrategyConfiguration.default)
  extends QueryGraphSolver{

  def plan(implicit context: QueryGraphSolvingContext, leafPlan: Option[QueryPlan] = None): QueryPlan = {
    val select = config.applySelections.asFunctionInContext
    val pickBest = config.pickBestCandidate.asFunctionInContext

    def generateLeafPlanTable() = {
      val leafPlanCandidateLists = config.leafPlanners.candidateLists(context.queryGraph)
      val leafPlanCandidateListsWithSelections = leafPlanCandidateLists.map(_.map(select))
      val bestLeafPlans: Iterable[QueryPlan] = leafPlanCandidateListsWithSelections.flatMap(pickBest(_))
      val startTable: PlanTable = leafPlan.foldLeft(PlanTable.empty)(_ + _)
      bestLeafPlans.foldLeft(startTable)(_ + _)
    }

    def findBestPlan(planGenerator: CandidateGenerator[PlanTable]) = {
      (planTable: PlanTable) =>
        val generated = planGenerator(planTable).plans.toList
        val selected = generated.map(select)
        val best = pickBest(CandidateList(selected))
        best.fold(planTable)(planTable + _)
    }

    val leaves: PlanTable = generateLeafPlanTable()
    val afterExpandOrJoin = iterateUntilConverged(findBestPlan(expandsOrJoins))(leaves)
    val afterOptionalApplies = iterateUntilConverged(findBestPlan(optionalMatches))(afterExpandOrJoin)
    val afterCartesianProduct = iterateUntilConverged(findBestPlan(cartesianProduct))(afterOptionalApplies)

    afterCartesianProduct.uniquePlan
  }
}
