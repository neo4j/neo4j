/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_3.planner.logical.greedy

import org.neo4j.cypher.internal.compiler.v2_3.helpers.Converge.iterateUntilConverged
import org.neo4j.cypher.internal.compiler.v2_3.planner.QueryGraph
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical._
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.plans.{IdName, LogicalPlan}
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.steps.solveOptionalMatches
import org.neo4j.cypher.internal.frontend.v2_3.HintException

class GreedyQueryGraphSolver(planCombiner: CandidateGenerator[GreedyPlanTable],
                             val config: QueryPlannerConfiguration = QueryPlannerConfiguration.default)
  extends TentativeQueryGraphSolver {

  def tryPlan(queryGraph: QueryGraph)(implicit context: LogicalPlanningContext, leafPlan: Option[LogicalPlan] = None) = {

    import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.CandidateGenerator._

    val kit = config.toKit()
    val optionalMatchesSolver = solveOptionalMatches(config.optionalSolvers, kit.pickBest)

    def generateLeafPlanTable(): GreedyPlanTable = {
      val leafPlanCandidateLists = config.leafPlanners.candidates(queryGraph, kit.projectAllEndpoints)
      val leafPlanCandidateListsWithSelections = kit.select(leafPlanCandidateLists, queryGraph).iterator
      val bestLeafPlans: Iterator[LogicalPlan] = leafPlanCandidateListsWithSelections.flatMap(kit.pickBest(_))
      val leafPlanWithSelections = leafPlan.map(kit.select.apply(_, queryGraph))
      val startTable: GreedyPlanTable = leafPlanWithSelections.foldLeft(GreedyPlanTable.empty)(_ + _)
      bestLeafPlans.foldLeft(startTable)(_ + _)
    }

    def findBestPlan(planGenerator: CandidateGenerator[GreedyPlanTable]): GreedyPlanTable => GreedyPlanTable = {
      (planTable: GreedyPlanTable) =>
        val step: CandidateGenerator[GreedyPlanTable] = planGenerator +||+ findShortestPaths
        val generated: Seq[LogicalPlan] = step(planTable, queryGraph)

        if (generated.nonEmpty) {
          val selected: Seq[LogicalPlan] = generated.map(plan => kit.select(plan, queryGraph))
          //          println("Building on top of " + planTable.plans.map(_.availableSymbols).mkString(" | "))
          //          println("Produced: " + selected.map(_.availableSymbols).mkString(" | "))

          // We want to keep the best plan per set of covered ids.
          val candidatesPerIds: Map[Set[IdName], Seq[LogicalPlan]] =
            selected.foldLeft(Map.empty[Set[IdName], Seq[LogicalPlan]]) {
              case (acc, plan) =>
                val ids = plan.availableSymbols
                val candidates = acc.getOrElse(ids, Seq.empty) :+ plan
                acc + (ids -> candidates)
            }

          val best: Iterable[LogicalPlan] = candidatesPerIds.values.flatMap(kit.pickBest(_))

          //          println(s"best: ${best.map(_.availableSymbols)}")
          val result = best.foldLeft(planTable)(_ + _)
          //          println(result.toString)

          //          println(s"result: ${result.plans.map(_.availableSymbols).toList}")
          //          println("*-+*-+*-+*-+*-+*-+*-+*-+*-+*-+*-+*-+*-+*-+*-+*-+*-+*-+*-+*-+*-+*-+*-+*-+*-+*-+*-+*-+*-+*-+*-+*-+*-+*-+")
          result
        } else planTable
    }

    def solveOptionalAndCartesianProducts: GreedyPlanTable => GreedyPlanTable = { incoming: GreedyPlanTable =>
      val solvedOptionalMatches = optionalMatchesSolver(incoming, queryGraph)
      findBestPlan(cartesianProduct)(solvedOptionalMatches)
    }

    val leaves: GreedyPlanTable = generateLeafPlanTable()
    val afterCombiningPlans = iterateUntilConverged(findBestPlan(planCombiner))(leaves)

    if (stillHasOverlappingPlans(afterCombiningPlans, leafPlan.map(_.availableSymbols).getOrElse(Set.empty))) {
      if (!afterCombiningPlans.m.keys.forall(_.allHints == queryGraph.allHints) )
        throw new HintException("The current planner cannot satisfy all hints in the query, please try removing hints or try with another planner")
      else
        None
    } else {
      val afterCartesianProduct = iterateUntilConverged(solveOptionalAndCartesianProducts)(afterCombiningPlans)
      Some(afterCartesianProduct.uniquePlan)
    }
  }

  private def stillHasOverlappingPlans(afterCombiningPlans: GreedyPlanTable, arguments: Set[IdName]): Boolean =
    afterCombiningPlans.plans.exists {
      p1 => afterCombiningPlans.plans.exists {
        p2 =>
          val overlap = (p1.availableSymbols intersect p2.availableSymbols) -- arguments
          p1 != p2 && overlap.nonEmpty
      }
    }
}
