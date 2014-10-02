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
package org.neo4j.cypher.internal.compiler.v2_2.planner.logical

import org.neo4j.cypher.internal.compiler.v2_2.planner.QueryGraph
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.plans.{IdName, LogicalPlan}
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.steps.{cartesianProduct, solveOptionalMatches}
import org.neo4j.cypher.internal.helpers.Converge.iterateUntilConverged

class GreedyQueryGraphSolver(config: PlanningStrategyConfiguration = PlanningStrategyConfiguration.default)
  extends QueryGraphSolver {

  def plan(queryGraph: QueryGraph)(implicit context: LogicalPlanningContext, leafPlan: Option[LogicalPlan] = None) = {
  import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.CandidateGenerator._

    val select = config.applySelections.asFunctionInContext
    val pickBest = config.pickBestCandidate.asFunctionInContext

    def generateLeafPlanTable(): PlanTable = {
      val leafPlanCandidateLists = config.leafPlanners.candidateLists(queryGraph)
      val leafPlanCandidateListsWithSelections = leafPlanCandidateLists.map(_.map(select(_, queryGraph)))
      val bestLeafPlans: Iterable[LogicalPlan] = leafPlanCandidateListsWithSelections.flatMap(pickBest(_))
      val startTable: PlanTable = leafPlan.foldLeft(PlanTable.empty)(_ + _)
      bestLeafPlans.foldLeft(startTable)(_ + _)
    }

    def findBestPlan(planGenerator: CandidateGenerator[PlanTable]): PlanTable => PlanTable = {
      (planTable: PlanTable) =>
        val step: CandidateGenerator[PlanTable] = planGenerator +||+ findShortestPaths
        val generated: Seq[LogicalPlan] = step(planTable, queryGraph).plans

        if (generated.nonEmpty) {
          val selected: Seq[LogicalPlan] = generated.map(select(_, queryGraph))
          //          println("Building on top of " + planTable.plans.map(_.availableSymbols).mkString(" | "))
          //          println("Produced: " + selected.map(_.availableSymbols).mkString(" | "))

          // We want to keep the best plan per set of covered ids.
          val candidatesPerIds: Map[Set[IdName], CandidateList] =
            selected.foldLeft(Map.empty[Set[IdName], CandidateList]) {
              case (acc, plan) =>
                val ids = plan.availableSymbols.filterNot(idName => idName.name.endsWith("$$$_") || idName.name.endsWith("$$$"))
                val candidates = acc.getOrElse(ids, context.metrics.candidateListCreator(Seq.empty)) + plan
                acc + (ids -> candidates)
            }

          val best: Iterable[LogicalPlan] = candidatesPerIds.values.map(pickBest).flatten

          //          println(s"best: ${best.map(_.availableSymbols)}")
          val result = best.foldLeft(planTable)(_ + _)

          //          println(s"result: ${result.plans.map(_.availableSymbols).toList}")
          //          println("*-+*-+*-+*-+*-+*-+*-+*-+*-+*-+*-+*-+*-+*-+*-+*-+*-+*-+*-+*-+*-+*-+*-+*-+*-+*-+*-+*-+*-+*-+*-+*-+*-+*-+")
          result
        } else planTable
    }

    def solveOptionalAndCartesianProducts: PlanTable => PlanTable = { incoming: PlanTable =>
        val solvedOptionalMatches = solveOptionalMatches(incoming, queryGraph)
        findBestPlan(cartesianProduct)(solvedOptionalMatches)
    }

    val leaves: PlanTable = generateLeafPlanTable()
    val afterExpandOrJoin = iterateUntilConverged(findBestPlan(expandsOrJoins))(leaves)

    val afterCartesianProduct = iterateUntilConverged(solveOptionalAndCartesianProducts)(afterExpandOrJoin)
    afterCartesianProduct.uniquePlan
  }
}
