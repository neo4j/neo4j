/**
 * Copyright (c) 2002-2016 "Neo Technology,"
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

import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.QueryPlan
import org.neo4j.cypher.internal.helpers.Converge.iterateUntilConverged
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.steps.cartesianProduct
import org.neo4j.cypher.internal.compiler.v2_1.planner.QueryGraph
import org.neo4j.cypher.internal.compiler.v2_1.ast.PatternExpression

class GreedyQueryGraphSolver(config: PlanningStrategyConfiguration = PlanningStrategyConfiguration.default)
  extends QueryGraphSolver {

  def plan(queryGraph: QueryGraph)(implicit context: LogicalPlanningContext, subQueryLookupTable: Map[PatternExpression, QueryGraph], leafPlan: Option[QueryPlan] = None) = {
  import CandidateGenerator._

    val select = config.applySelections.asFunctionInContext
    val pickBest = config.pickBestCandidate.asFunctionInContext

    def generateLeafPlanTable(): PlanTable = {
      val leafPlanCandidateLists = config.leafPlanners.candidateLists(queryGraph)
      val leafPlanCandidateListsWithSelections = leafPlanCandidateLists.map(_.map(select(_, queryGraph)))
      val bestLeafPlans: Iterable[QueryPlan] = leafPlanCandidateListsWithSelections.flatMap(pickBest(_))
      val startTable: PlanTable = leafPlan.foldLeft(PlanTable.empty)(_ + _)
      bestLeafPlans.foldLeft(startTable)(_ + _)
    }

    def findBestPlan(planGenerator: CandidateGenerator[PlanTable]): PlanTable => PlanTable = {
      (planTable: PlanTable) =>
        val step = planGenerator +||+ findShortestPaths
        val generated = step(planTable, queryGraph).plans
        val selected = generated.map(select(_, queryGraph))
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
