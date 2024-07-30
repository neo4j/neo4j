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
package org.neo4j.cypher.internal.compiler.planner.logical.steps.index

import org.neo4j.cypher.internal.ast.Hint
import org.neo4j.cypher.internal.compiler.planner.logical.LeafPlanRestrictions
import org.neo4j.cypher.internal.compiler.planner.logical.LogicalPlanningContext
import org.neo4j.cypher.internal.compiler.planner.logical.steps.index.EntityIndexLeafPlanner.IndexCompatiblePredicate
import org.neo4j.cypher.internal.compiler.planner.logical.steps.index.NodeIndexLeafPlanner.NodeIndexMatch
import org.neo4j.cypher.internal.expressions.Contains
import org.neo4j.cypher.internal.expressions.EndsWith
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.logical.plans.LogicalPlan

object nodeIndexStringSearchScanPlanProvider extends NodeIndexPlanProvider {

  override def createPlans(
    indexMatches: Set[NodeIndexMatch],
    hints: Set[Hint],
    argumentIds: Set[LogicalVariable],
    restrictions: LeafPlanRestrictions,
    context: LogicalPlanningContext
  ): Set[LogicalPlan] = for {
    indexMatch <- indexMatches
    // Use isAllowedByRestrictions from EntityIndexSeekPlanProvider, since we also want to plan Nested-Index-Joins
    // with nodeIndexStringSearchScanPlans.
    if EntityIndexSeekPlanProvider.isAllowedByRestrictions(
      indexMatch.propertyPredicates,
      restrictions
    ) && indexMatch.indexDescriptor.properties.size == 1
    plan <- doCreatePlans(indexMatch, hints, argumentIds, context)
  } yield plan

  private def doCreatePlans(
    indexMatch: NodeIndexMatch,
    hints: Set[Hint],
    argumentIds: Set[LogicalVariable],
    context: LogicalPlanningContext
  ): Set[LogicalPlan] = {
    def indexableExprWithSearchMode(indexPredicate: IndexCompatiblePredicate): Option[(Expression, StringSearchMode)] =
      indexPredicate.predicate match {
        case Contains(_, valueExpr) if valueExpr.dependencies.subsetOf(argumentIds) =>
          Some((valueExpr, ContainsSearchMode))
        case EndsWith(_, valueExpr) if valueExpr.dependencies.subsetOf(argumentIds) =>
          Some((valueExpr, EndsWithSearchMode))
        case _ => None
      }

    indexMatch.propertyPredicates.flatMap { indexPredicate =>
      indexableExprWithSearchMode(indexPredicate).map {
        case (valueExpr: Expression, stringSearchMode: StringSearchMode) =>
          val singlePredicateSet = indexMatch.predicateSet(Seq(indexPredicate), exactPredicatesCanGetValue = false)

          val hint = singlePredicateSet
            .fulfilledHints(hints, indexMatch.indexDescriptor.indexType, planIsScan = true)
            .headOption

          context.staticComponents.logicalPlanProducer.planNodeIndexStringSearchScan(
            variable = indexMatch.variable,
            label = indexMatch.labelToken,
            properties = singlePredicateSet.indexedProperties(context),
            stringSearchMode = stringSearchMode,
            solvedPredicates = singlePredicateSet.allSolvedPredicates,
            solvedHint = hint,
            valueExpr = valueExpr,
            argumentIds = argumentIds,
            providedOrder = indexMatch.providedOrder,
            indexOrder = indexMatch.indexOrder,
            context = context,
            indexType = indexMatch.indexDescriptor.indexType
          )
      }
    }.toSet
  }
}
