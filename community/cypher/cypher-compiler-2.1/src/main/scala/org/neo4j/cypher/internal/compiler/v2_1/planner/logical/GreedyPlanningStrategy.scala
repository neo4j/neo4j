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

import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.steps._
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.QueryPlan
import org.neo4j.cypher.internal.helpers.Converge.iterateUntilConverged
import org.neo4j.cypher.internal.compiler.v2_1.planner.QueryGraph

class GreedyPlanningStrategy(config: PlanningStrategyConfiguration = PlanningStrategyConfiguration.default) extends PlanningStrategy {
  def plan(implicit context: LogicalPlanContext, leafPlan: Option[QueryPlan] = None): QueryPlan = {
    val select = config.applySelections.asFunctionInContext
    val pickBest = config.pickBestCandidate.asFunctionInContext

    def generateLeafPlanTable() = {
      val leafPlanCandidateLists = config.leafPlanners.candidateLists(context.queryGraph)
      val leafPlanCandidateListsWithSelections = leafPlanCandidateLists.map(_.map(select))
      val bestLeafPlans: Iterable[QueryPlan] = leafPlanCandidateListsWithSelections.flatMap(pickBest(_))
      val startTable: PlanTable = leafPlan.foldLeft(PlanTable.empty)(_ + _)
      bestLeafPlans.foldLeft(startTable)(_ + _)
    }

    def findBestPlan(planGenerator: CandidateGenerator[PlanTable]) =
      (planTable: PlanTable) => pickBest(planGenerator(planTable).map(select)).fold(planTable)(planTable + _)

    val leaves: PlanTable = generateLeafPlanTable()

    val afterExpandOrJoin = iterateUntilConverged(findBestPlan(expandsOrJoins))(leaves)
    val afterOptionalApplies = iterateUntilConverged(findBestPlan(optionalMatches))(afterExpandOrJoin)
    val afterCartesianProduct = iterateUntilConverged(findBestPlan(cartesianProduct))(afterOptionalApplies)

    val afterAggregation = aggregation(afterCartesianProduct.uniquePlan)
    val bestPlan = projection(afterAggregation)

    val finalPlan: QueryPlan = context.queryGraph.tail match {
      case Some(tail) =>
        val finalPlan = plan(context.copy(queryGraph = tail), Some(QueryPlan(bestPlan.plan, QueryGraph.empty)))
        finalPlan.copy(solved = bestPlan.solved.withTail(tail))
      case _ => bestPlan
    }

    verifyBestPlan(finalPlan)
    finalPlan
  }
}
