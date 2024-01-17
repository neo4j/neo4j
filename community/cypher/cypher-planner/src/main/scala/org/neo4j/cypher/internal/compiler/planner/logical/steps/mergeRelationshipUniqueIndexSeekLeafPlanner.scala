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
package org.neo4j.cypher.internal.compiler.planner.logical.steps

import org.neo4j.cypher.internal.ast.Hint
import org.neo4j.cypher.internal.ast.UsingIndexHint
import org.neo4j.cypher.internal.compiler.planner.logical.LeafPlanRestrictions
import org.neo4j.cypher.internal.compiler.planner.logical.LeafPlanner
import org.neo4j.cypher.internal.compiler.planner.logical.LogicalPlanningContext
import org.neo4j.cypher.internal.compiler.planner.logical.ordering.InterestingOrderConfig
import org.neo4j.cypher.internal.compiler.planner.logical.steps.RelationshipLeafPlanner.planHiddenSelectionAndRelationshipLeafPlan
import org.neo4j.cypher.internal.compiler.planner.logical.steps.index.EntityIndexLeafPlanner.IndexCompatiblePredicate
import org.neo4j.cypher.internal.compiler.planner.logical.steps.index.EntityIndexSeekPlanProvider.isAllowedByRestrictions
import org.neo4j.cypher.internal.compiler.planner.logical.steps.index.EntityIndexSeekPlanProvider.predicatesForIndexSeek
import org.neo4j.cypher.internal.compiler.planner.logical.steps.index.RelationshipIndexLeafPlanner
import org.neo4j.cypher.internal.compiler.planner.logical.steps.index.RelationshipIndexLeafPlanner.RelationshipIndexMatch
import org.neo4j.cypher.internal.compiler.planner.logical.steps.index.RelationshipIndexPlanProvider
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.RelationshipTypeToken
import org.neo4j.cypher.internal.ir.PatternRelationship
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.logical.plans.CompositeQueryExpression
import org.neo4j.cypher.internal.logical.plans.ExistenceQueryExpression
import org.neo4j.cypher.internal.logical.plans.IndexOrder
import org.neo4j.cypher.internal.logical.plans.IndexedProperty
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.ManyQueryExpression
import org.neo4j.cypher.internal.logical.plans.QueryExpression
import org.neo4j.cypher.internal.logical.plans.RangeQueryExpression
import org.neo4j.cypher.internal.logical.plans.SingleQueryExpression
import org.neo4j.cypher.internal.logical.plans.ordering.ProvidedOrder
import org.neo4j.cypher.internal.macros.AssertMacros
import org.neo4j.cypher.internal.planner.spi.IndexDescriptor.IndexType

import scala.annotation.tailrec

object mergeRelationshipUniqueIndexSeekLeafPlanner extends LeafPlanner {

  private val relationshipIndexLeafPlanner = RelationshipIndexLeafPlanner(
    planProviders = Seq(relationshipSingleUniqueIndexSeekPlanProvider),
    restrictions = LeafPlanRestrictions.NoRestrictions
  )

  override def apply(
    queryGraph: QueryGraph,
    interestingOrderConfig: InterestingOrderConfig,
    context: LogicalPlanningContext
  ): Set[LogicalPlan] = {
    def solvedQueryGraph(plan: LogicalPlan): QueryGraph =
      context.staticComponents.planningAttributes.solveds.get(plan.id).asSinglePlannerQuery.tailOrSelf.queryGraph

    val resultPlans: Set[LogicalPlan] = relationshipIndexLeafPlanner.apply(queryGraph, interestingOrderConfig, context)

    val grouped: Map[PatternRelationship, Set[LogicalPlan]] = resultPlans.groupBy { p =>
      val solvedQG = solvedQueryGraph(p)
      val patternRelationships = solvedQG.patternRelationships

      AssertMacros.checkOnlyWhenAssertionsAreEnabled(
        patternRelationships.size == 1,
        "Relationship unique index plan solved more than one pattern relationship."
      )
      patternRelationships.head
    }

    grouped.map {
      case (relationship, plans) =>
        plans.reduce[LogicalPlan] {
          case (p1, p2) =>
            context.staticComponents.logicalPlanProducer.planAssertSameRelationship(relationship, p1, p2, context)
        }
    }.toSet
  }
}

object relationshipSingleUniqueIndexSeekPlanProvider extends RelationshipIndexPlanProvider {

  override def createPlans(
    indexMatches: Set[RelationshipIndexMatch],
    hints: Set[Hint],
    argumentIds: Set[LogicalVariable],
    restrictions: LeafPlanRestrictions,
    context: LogicalPlanningContext
  ): Set[LogicalPlan] =
    for {
      indexMatch <- indexMatches
      if isAllowedByRestrictions(indexMatch.propertyPredicates, restrictions) && indexMatch.indexDescriptor.isUnique
      propertyPredicates = predicatesForIndexSeek(indexMatch.propertyPredicates)
      queryExpression <- propertyPredicatesQueryExpression(propertyPredicates)
      predicateSet = indexMatch.predicateSet(propertyPredicates, exactPredicatesCanGetValue = true)
      indexType = indexMatch.indexDescriptor.indexType
    } yield createPlan(
      argumentIds = argumentIds,
      patternRelationship = indexMatch.patternRelationship,
      variable = indexMatch.variable,
      relationshipTypeToken = indexMatch.relationshipTypeToken,
      properties = predicateSet.indexedProperties(context),
      queryExpression = queryExpression,
      indexOrder = indexMatch.indexOrder,
      solvedPredicates = predicateSet.allSolvedPredicates,
      solvedHint = predicateSet.fulfilledHints(hints, indexType, planIsScan = false).headOption,
      providedOrder = indexMatch.providedOrder,
      indexType = indexType,
      context = context
    )

  private def propertyPredicatesQueryExpression(
    propertyPredicates: Seq[IndexCompatiblePredicate]
  ): Option[QueryExpression[Expression]] =
    Option(propertyPredicates.map(_.queryExpression))
      .filter(allSingleQueryExpressions)
      .map {
        case Seq(queryExpression) => queryExpression
        case queryExpressions     => CompositeQueryExpression(queryExpressions)
      }

  @tailrec
  private def allSingleQueryExpressions(queryExpressions: Seq[QueryExpression[Expression]]): Boolean =
    queryExpressions.isEmpty ||
      (queryExpressions.head match {
        case SingleQueryExpression(_)        => allSingleQueryExpressions(queryExpressions.tail)
        case ManyQueryExpression(_)          => false
        case RangeQueryExpression(_)         => false
        case CompositeQueryExpression(inner) => allSingleQueryExpressions(inner ++ queryExpressions.tail)
        case ExistenceQueryExpression()      => false
      })

  private def createPlan(
    argumentIds: Set[LogicalVariable],
    patternRelationship: PatternRelationship,
    variable: LogicalVariable,
    relationshipTypeToken: RelationshipTypeToken,
    properties: Seq[IndexedProperty],
    queryExpression: QueryExpression[Expression],
    indexOrder: IndexOrder,
    solvedPredicates: Seq[Expression],
    solvedHint: Option[UsingIndexHint],
    providedOrder: ProvidedOrder,
    indexType: IndexType,
    context: LogicalPlanningContext
  ): LogicalPlan = {

    def getRelationshipLeafPlan(
      patternForLeafPlan: PatternRelationship,
      originalPattern: PatternRelationship,
      hiddenSelections: Seq[Expression]
    ): LogicalPlan =
      context.staticComponents.logicalPlanProducer.planRelationshipIndexSeek(
        variable = variable,
        typeToken = relationshipTypeToken,
        properties = properties,
        valueExpr = queryExpression,
        argumentIds = argumentIds,
        indexOrder = indexOrder,
        patternForLeafPlan = patternForLeafPlan,
        originalPattern = originalPattern,
        solvedPredicates = solvedPredicates,
        solvedHint = solvedHint,
        hiddenSelections = hiddenSelections,
        providedOrder = providedOrder,
        context = context,
        indexType = indexType,
        unique = true,
        supportPartitionedScan = false
      )

    planHiddenSelectionAndRelationshipLeafPlan(
      argumentIds,
      patternRelationship,
      context,
      getRelationshipLeafPlan
    )
  }
}
