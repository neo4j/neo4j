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
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.steps.includeBestPlan
import org.neo4j.cypher.internal.compiler.v2_1.planner.CantHandleQueryException

class GreedyPlanningStrategy {
  def plan(implicit context: LogicalPlanContext): LogicalPlan = {

    if (context.queryGraph.optionalMatches.nonEmpty)
      throw new CantHandleQueryException

    val leafPlans = generateLeafPlans()
    val leafPlansWithSelections = applySelections(leafPlans)
    val plansAfterExpansionsAndJoins = iterateUntilConverged(findExpandOrJoin)(leafPlansWithSelections)
    val plansAfterCartesianProducts = iterateUntilConverged(findCartesianProduct)(plansAfterExpansionsAndJoins)
    val bestPlan = extractBestPlan(plansAfterCartesianProducts)
    project(bestPlan)
  }

  def findExpandOrJoin(planTable: PlanTable)(implicit context: LogicalPlanContext) = {
    val expansions = expand(planTable)
    val expansionsWithSelections = applySelections(expansions)
    val joins = join(planTable)
    val joinsWithSelections = applySelections(joins)
    includeBestPlan(planTable)(expansionsWithSelections ++ joinsWithSelections)
  }

  def findCartesianProduct(planTable: PlanTable)(implicit context: LogicalPlanContext) = {
    val cartesianProducts = cartesianProduct(planTable)
    val cartesianProductsWithSelections = applySelections(cartesianProducts)
    includeBestPlan(planTable)(cartesianProductsWithSelections)
  }
}
