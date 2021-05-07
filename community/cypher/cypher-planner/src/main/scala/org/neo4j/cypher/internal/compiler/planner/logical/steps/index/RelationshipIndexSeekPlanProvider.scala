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
import org.neo4j.cypher.internal.compiler.planner.logical.steps.index.EntityIndexSeekPlanProvider.mergeQueryExpressionsToSingleOne
import org.neo4j.cypher.internal.compiler.planner.logical.steps.index.EntityIndexSeekPlanProvider.predicatesForIndexSeek
import org.neo4j.cypher.internal.compiler.planner.logical.steps.index.RelationshipIndexLeafPlanner.IndexMatch
import org.neo4j.cypher.internal.compiler.planner.logical.steps.index.RelationshipIndexLeafPlanner.PredicateSet
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.QueryExpression

object RelationshipIndexSeekPlanProvider extends RelationshipIndexPlanProvider with RelationshipIndexPlanProviderPeek {

  override def createPlans(indexMatches: Set[RelationshipIndexLeafPlanner.IndexMatch],
                           hints: Set[Hint],
                           argumentIds: Set[String],
                           restrictions: LeafPlanRestrictions,
                           context: LogicalPlanningContext): Set[LogicalPlan] = for {
    indexMatch <- indexMatches
    if isAllowedByRestrictions(indexMatch.propertyPredicates, restrictions)
    plan <- doCreatePlans(indexMatch, hints, argumentIds, context)
  } yield plan

  private def predicateSetToSolve(indexMatch: IndexMatch): Option[PredicateSet] = {
    val predicateSet = indexMatch.predicateSet(predicatesForIndexSeek(indexMatch.propertyPredicates), exactPredicatesCanGetValue = true)
    if (predicateSet.propertyPredicates.forall(_.isExists))
      None
    else
      Some(predicateSet)
  }

  private def doCreatePlans(indexMatch: IndexMatch, hints: Set[Hint], argumentIds: Set[String], context: LogicalPlanningContext): Set[LogicalPlan] = {
    val predicateSet = predicateSetToSolve(indexMatch)
    predicateSet.map(constructPlan(_, indexMatch, hints, argumentIds, context)).toSet
  }

  private def constructPlan(predicateSet: PredicateSet,
                            indexMatch: IndexMatch,
                            hints: Set[Hint],
                            argumentIds: Set[String],
                            context: LogicalPlanningContext): LogicalPlan = {

    val queryExpression: QueryExpression[Expression] = mergeQueryExpressionsToSingleOne(predicateSet.propertyPredicates)
    val hint = predicateSet.matchingHints(hints).headOption

    context.logicalPlanProducer.planRelationshipIndexSeek(
      idName = indexMatch.variableName,
      typeToken = indexMatch.relationshipTypeToken,
      properties = predicateSet.indexedProperties(context),
      valueExpr = queryExpression,
      argumentIds = argumentIds,
      indexOrder = indexMatch.indexOrder,
      pattern = indexMatch.patternRelationship,
      solvedPredicates = predicateSet.allSolvedPredicates,
      solvedHint = hint,
      providedOrder = indexMatch.providedOrder,
      context = context,
    )
  }

  override def wouldCreatePlan(indexMatch: IndexMatch): Boolean =
    predicateSetToSolve(indexMatch).nonEmpty
}
