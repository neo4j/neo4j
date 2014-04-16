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
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.LogicalPlan

import org.neo4j.cypher.internal.helpers.Converge.iterateUntilConverged
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.steps.pickBestPlan
import org.neo4j.cypher.internal.compiler.v2_1.planner.{MainQueryGraph, OptionalQueryGraph, CantHandleQueryException}

class GreedyPlanningStrategy extends PlanningStrategy {
  def plan(implicit context: LogicalPlanContext): LogicalPlan = {

    if ( context.queryGraph.namedPaths.nonEmpty )
      throw new CantHandleQueryException

    val select = selectPlan.asFunction
    val pickBest = pickBestPlan.asFunction

    def generateLeafPlanTable() = {
      val leafPlanCandidateLists = generateLeafPlanCandidateLists()
      val leafPlanCandidateListsWithSelections = leafPlanCandidateLists.map(_.map(select))
      val topLeafPlans: Iterable[LogicalPlan] = leafPlanCandidateListsWithSelections.flatMap(_.bestPlan(context.cost))
      topLeafPlans.foldLeft(PlanTable())(_ + _)
    }

    def solveExpandsOrJoins(planTable: PlanTable) = {
      val expansions = expand(planTable)
      val expansionsWithSelections = expansions.map(select)
      val joins = join(planTable)
      val joinsWithSelections = joins.map(select)
      planTable + pickBest(expansionsWithSelections ++ joinsWithSelections)
    }

    def solveCartesianProducts(planTable: PlanTable) = {
      val cartesianProducts = cartesianProduct(planTable)
      val cartesianProductsWithSelections = cartesianProducts.map(select)
      planTable + pickBest(cartesianProductsWithSelections)
    }

    def solveOptionalMatches(planTable: PlanTable)(implicit context: LogicalPlanContext) = {
      val optionalApplies = applyOptional(planTable)
      val optionals = optional(planTable)
      val outerJoins = outerJoin(planTable)
      val optionalExpands = optionalExpand(planTable)
      val optionalSolutionsWithSelections = (optionalApplies ++ optionals ++ outerJoins ++ optionalExpands).map(select)
      planTable + pickBest(optionalSolutionsWithSelections)
    }

    val leafPlanTable = generateLeafPlanTable()

    val planTableAfterExpandOrJoin = iterateUntilConverged(solveExpandsOrJoins)(leafPlanTable)
    val planTableAfterCartesianProduct = iterateUntilConverged(solveCartesianProducts)(planTableAfterExpandOrJoin)

    val bestPlan = context.queryGraph match {
      case main: MainQueryGraph =>
        val planTableAfterOptionalApplies = iterateUntilConverged(solveOptionalMatches)(planTableAfterCartesianProduct)
        projectPlan(planTableAfterOptionalApplies.uniquePlan)

      case optionalMatch: OptionalQueryGraph =>
        planTableAfterCartesianProduct.uniquePlan
    }

    verifyBestPlan(bestPlan)
  }
}
