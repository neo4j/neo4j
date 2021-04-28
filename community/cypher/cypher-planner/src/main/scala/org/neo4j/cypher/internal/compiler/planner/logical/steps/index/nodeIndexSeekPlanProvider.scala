/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.cypher.internal.compiler.planner.logical.steps.index

import org.neo4j.cypher.internal.ast.Hint
import org.neo4j.cypher.internal.compiler.planner.logical.LeafPlanRestrictions
import org.neo4j.cypher.internal.compiler.planner.logical.LogicalPlanningContext
import org.neo4j.cypher.internal.compiler.planner.logical.steps.index.EntityIndexSeekPlanProvider.isAllowedByRestrictions
import org.neo4j.cypher.internal.compiler.planner.logical.steps.index.NodeIndexLeafPlanner.IndexMatch
import org.neo4j.cypher.internal.logical.plans.LogicalPlan

object nodeIndexSeekPlanProvider extends AbstractNodeIndexSeekPlanProvider with NodeIndexPlanProviderPeek {

  override def createPlans(indexMatches: Set[IndexMatch], hints: Set[Hint], argumentIds: Set[String], restrictions: LeafPlanRestrictions, context: LogicalPlanningContext): Set[LogicalPlan] = for {
    solution <- createSolutions(indexMatches, hints, argumentIds, restrictions, context)
  } yield constructPlan(solution, context)

  def wouldCreatePlan(indexMatch: IndexMatch, hints: Set[Hint], argumentIds: Set[String], restrictions: LeafPlanRestrictions, context: LogicalPlanningContext): Boolean = {
    createSolutions(Set(indexMatch), hints, argumentIds, restrictions, context).nonEmpty
  }

  // This is a temporary hack to get rid of some plans that we did not produce in the old implementation
  private def hasNoImplicitPredicates(indexMatch: IndexMatch): Boolean =
    !indexMatch.propertyPredicates.exists(_.isImplicit)

  private def createSolutions(indexMatches: Set[IndexMatch], hints: Set[Hint], argumentIds: Set[String], restrictions: LeafPlanRestrictions, context: LogicalPlanningContext): Set[Solution] = for {
    indexMatch <- indexMatches
    if isAllowedByRestrictions(indexMatch.propertyPredicates, restrictions)
    if hasNoImplicitPredicates(indexMatch)
    solution <- createSolution(indexMatch, hints, argumentIds, context)
  } yield solution

  override protected def constructPlan(solution: Solution, context: LogicalPlanningContext): LogicalPlan =
    if (solution.isUnique) {
      context.logicalPlanProducer.planNodeUniqueIndexSeek(
        solution.idName,
        solution.label,
        solution.properties,
        solution.valueExpr,
        solution.solvedPredicates,
        solution.predicatesForCardinalityEstimation,
        solution.hint,
        solution.argumentIds,
        solution.providedOrder,
        solution.indexOrder,
        context
      )
    } else {
      context.logicalPlanProducer.planNodeIndexSeek(
        solution.idName,
        solution.label,
        solution.properties,
        solution.valueExpr,
        solution.solvedPredicates,
        solution.predicatesForCardinalityEstimation,
        solution.hint,
        solution.argumentIds,
        solution.providedOrder,
        solution.indexOrder,
        context)
    }
}

