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
import org.neo4j.cypher.internal.compiler.planner.logical.steps.RelationshipLeafPlanner.planHiddenSelectionAndRelationshipLeafPlan
import org.neo4j.cypher.internal.compiler.planner.logical.steps.index.EntityIndexSeekPlanProvider.isAllowedByRestrictions
import org.neo4j.cypher.internal.compiler.planner.logical.steps.index.EntityIndexSeekPlanProvider.mergeQueryExpressionsToSingleOne
import org.neo4j.cypher.internal.compiler.planner.logical.steps.index.EntityIndexSeekPlanProvider.predicatesForIndexSeek
import org.neo4j.cypher.internal.compiler.planner.logical.steps.index.RelationshipIndexLeafPlanner.RelationshipIndexMatch
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.ir.PatternRelationship
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.QueryExpression
import org.neo4j.cypher.internal.logical.plans.RangeQueryExpression
import org.neo4j.cypher.internal.logical.plans.SingleQueryExpression
import org.neo4j.internal.kernel.api.PropertyIndexQuery.allEntries

object RelationshipIndexSeekPlanProvider extends RelationshipIndexPlanProvider {

  override def createPlans(
    indexMatches: Set[RelationshipIndexMatch],
    hints: Set[Hint],
    argumentIds: Set[LogicalVariable],
    restrictions: LeafPlanRestrictions,
    context: LogicalPlanningContext
  ): Set[LogicalPlan] = for {
    indexMatch <- indexMatches
    if isAllowedByRestrictions(indexMatch.propertyPredicates, restrictions)
    plan <- doCreatePlans(indexMatch, hints, argumentIds, context)
  } yield plan

  private def predicateSetToSolve(indexMatch: RelationshipIndexMatch): Option[PredicateSet] = {
    val predicateSet =
      indexMatch.predicateSet(predicatesForIndexSeek(indexMatch.propertyPredicates), exactPredicatesCanGetValue = true)
    if (predicateSet.propertyPredicates.forall(_.isExists))
      None
    else
      Some(predicateSet)
  }

  private def doCreatePlans(
    indexMatch: RelationshipIndexMatch,
    hints: Set[Hint],
    argumentIds: Set[LogicalVariable],
    context: LogicalPlanningContext
  ): Set[LogicalPlan] = {
    val predicateSet = predicateSetToSolve(indexMatch)
    predicateSet.map(constructPlan(_, indexMatch, hints, argumentIds, context)).toSet
  }

  private def constructPlan(
    predicateSet: PredicateSet,
    indexMatch: RelationshipIndexMatch,
    hints: Set[Hint],
    argumentIds: Set[LogicalVariable],
    context: LogicalPlanningContext
  ): LogicalPlan = {

    val queryExpression: QueryExpression[Expression] = mergeQueryExpressionsToSingleOne(predicateSet.propertyPredicates)

    val hint = predicateSet
      .fulfilledHints(hints, indexMatch.indexDescriptor.indexType, planIsScan = false)
      .headOption
    val supportsPartitionedScans = queryExpression match {
      case _: SingleQueryExpression[_] | _: RangeQueryExpression[_] =>
        // NOTE: we still need to check at runtime if we can use a partitioned scan since it is dependent on
        // values etc
        indexMatch.indexDescriptor.maybeKernelIndexCapability.exists(_.supportPartitionedScan(allEntries()))
      case _ => false
    }

    def getRelationshipLeafPlan(
      patternForLeafPlan: PatternRelationship,
      originalPattern: PatternRelationship,
      hiddenSelections: Seq[Expression]
    ): LogicalPlan = context.staticComponents.logicalPlanProducer.planRelationshipIndexSeek(
      variable = indexMatch.variable,
      typeToken = indexMatch.relationshipTypeToken,
      properties = predicateSet.indexedProperties(context),
      valueExpr = queryExpression,
      argumentIds = argumentIds,
      indexOrder = indexMatch.indexOrder,
      patternForLeafPlan = patternForLeafPlan,
      originalPattern = originalPattern,
      solvedPredicates = predicateSet.allSolvedPredicates,
      solvedHint = hint,
      hiddenSelections = hiddenSelections,
      providedOrder = indexMatch.providedOrder,
      context = context,
      indexType = indexMatch.indexDescriptor.indexType,
      unique = indexMatch.indexDescriptor.isUnique,
      supportsPartitionedScans
    )

    planHiddenSelectionAndRelationshipLeafPlan(
      argumentIds,
      indexMatch.patternRelationship,
      context,
      getRelationshipLeafPlan
    )

  }
}
