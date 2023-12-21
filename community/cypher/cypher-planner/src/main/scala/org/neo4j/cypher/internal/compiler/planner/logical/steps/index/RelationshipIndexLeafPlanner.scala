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

import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.compiler.planner.logical.LeafPlanRestrictions
import org.neo4j.cypher.internal.compiler.planner.logical.LeafPlanner
import org.neo4j.cypher.internal.compiler.planner.logical.LogicalPlanningContext
import org.neo4j.cypher.internal.compiler.planner.logical.ordering.InterestingOrderConfig
import org.neo4j.cypher.internal.compiler.planner.logical.steps.DynamicPropertyNotifier
import org.neo4j.cypher.internal.compiler.planner.logical.steps.index.EntityIndexLeafPlanner.IndexCompatiblePredicate
import org.neo4j.cypher.internal.compiler.planner.logical.steps.index.EntityIndexLeafPlanner.getValueBehaviors
import org.neo4j.cypher.internal.compiler.planner.logical.steps.index.EntityIndexLeafPlanner.implicitIsNotNullPredicates
import org.neo4j.cypher.internal.compiler.planner.logical.steps.index.EntityIndexLeafPlanner.predicatesForIndex
import org.neo4j.cypher.internal.compiler.planner.logical.steps.index.RelationshipIndexLeafPlanner.findIndexMatchesForQueryGraph
import org.neo4j.cypher.internal.expressions.EntityType
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.RELATIONSHIP_TYPE
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.expressions.RelationshipTypeToken
import org.neo4j.cypher.internal.ir.PatternRelationship
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.ir.SimplePatternLength
import org.neo4j.cypher.internal.ir.ordering.NoProvidedOrderFactory
import org.neo4j.cypher.internal.ir.ordering.ProvidedOrder
import org.neo4j.cypher.internal.ir.ordering.ProvidedOrderFactory
import org.neo4j.cypher.internal.logical.plans.GetValueFromIndexBehavior
import org.neo4j.cypher.internal.logical.plans.IndexOrder
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.planner.spi.IndexDescriptor
import org.neo4j.cypher.internal.planner.spi.PlanContext
import org.neo4j.cypher.internal.util.RelTypeId
import org.neo4j.notifications.RelationshipIndexLookupUnfulfillableNotification

case class RelationshipIndexLeafPlanner(
  planProviders: Seq[RelationshipIndexPlanProvider],
  restrictions: LeafPlanRestrictions
) extends LeafPlanner {

  override def apply(
    qg: QueryGraph,
    interestingOrderConfig: InterestingOrderConfig,
    context: LogicalPlanningContext
  ): Set[LogicalPlan] = {
    val indexMatches = findIndexMatchesForQueryGraph(
      qg,
      context.semanticTable,
      context.staticComponents.planContext,
      context.plannerState.indexCompatiblePredicatesProviderContext,
      interestingOrderConfig,
      context.providedOrderFactory
    )

    val result: Set[LogicalPlan] =
      if (indexMatches.isEmpty) {
        Set.empty[LogicalPlan]
      } else {
        for {
          provider <- planProviders
          plan <- provider.createPlans(indexMatches, qg.hints, qg.argumentIds, restrictions, context)
        } yield plan
      }.toSet

    DynamicPropertyNotifier.issueNotifications(
      result,
      RelationshipIndexLookupUnfulfillableNotification,
      qg,
      RELATIONSHIP_TYPE,
      context
    )

    result
  }
}

object RelationshipIndexLeafPlanner extends IndexCompatiblePredicatesProvider {

  case class RelationshipIndexMatch(
    variableName: String,
    patternRelationship: PatternRelationship,
    relTypeName: RelTypeName,
    relTypeId: RelTypeId,
    propertyPredicates: Seq[IndexCompatiblePredicate],
    providedOrder: ProvidedOrder,
    indexOrder: IndexOrder,
    indexDescriptor: IndexDescriptor
  ) extends IndexMatch {

    def relationshipTypeToken: RelationshipTypeToken = RelationshipTypeToken(relTypeName, relTypeId)

    override def predicateSet(
      newPredicates: Seq[IndexCompatiblePredicate],
      exactPredicatesCanGetValue: Boolean
    ): PredicateSet =
      RelationshipPredicateSet(
        variableName,
        relTypeName,
        newPredicates,
        getValueBehaviors(indexDescriptor, newPredicates, exactPredicatesCanGetValue)
      )

  }

  case class RelationshipPredicateSet(
    variableName: String,
    symbolicName: RelTypeName,
    propertyPredicates: Seq[IndexCompatiblePredicate],
    getValueBehaviors: Seq[GetValueFromIndexBehavior]
  ) extends PredicateSet {

    override def getEntityType: EntityType = RELATIONSHIP_TYPE
  }

  def findIndexMatchesForQueryGraph(
    qg: QueryGraph,
    semanticTable: SemanticTable,
    planContext: PlanContext,
    indexPredicateProviderContext: IndexCompatiblePredicatesProviderContext,
    interestingOrderConfig: InterestingOrderConfig = InterestingOrderConfig.empty,
    providedOrderFactory: ProvidedOrderFactory = NoProvidedOrderFactory,
    findTextIndexes: Boolean = true,
    findRangeIndexes: Boolean = true,
    findPointIndexes: Boolean = true
  ): Set[RelationshipIndexMatch] = {
    def shouldIgnore(pattern: PatternRelationship) =
      qg.argumentIds.contains(pattern.variable.name)

    val predicates = qg.selections.flatPredicatesSet
    val patternRelationshipsMap: Map[String, PatternRelationship] = qg.patternRelationships.collect({
      case pattern @ PatternRelationship(rel, _, _, Seq(_), SimplePatternLength) if !shouldIgnore(pattern) =>
        rel.name -> pattern
    }).toMap

    // Find plans solving given property predicates together with any label predicates from QG
    val indexMatches =
      if (patternRelationshipsMap.isEmpty) {
        Seq.empty[RelationshipIndexMatch]
      } else {
        val compatiblePropertyPredicates = findIndexCompatiblePredicates(
          predicates,
          qg.argumentIds,
          semanticTable,
          planContext,
          indexPredicateProviderContext,
          patternRelationshipsMap.values
        )

        for {
          propertyPredicates <- compatiblePropertyPredicates.groupBy(_.name)
          variableName = propertyPredicates._1
          patternRelationship <- patternRelationshipsMap.get(variableName).toSet[PatternRelationship]
          indexMatch <- findIndexMatches(
            variableName,
            propertyPredicates._2,
            patternRelationship,
            interestingOrderConfig,
            semanticTable,
            planContext,
            providedOrderFactory,
            findTextIndexes,
            findRangeIndexes,
            findPointIndexes
          )
        } yield indexMatch
      }
    indexMatches.toSet
  }

  private def findIndexCompatiblePredicates(
    predicates: Set[Expression],
    argumentIds: Set[String],
    semanticTable: SemanticTable,
    planContext: PlanContext,
    indexPredicateProviderContext: IndexCompatiblePredicatesProviderContext,
    patterns: Iterable[PatternRelationship]
  ): Set[IndexCompatiblePredicate] = {
    val generalCompatiblePredicates = findIndexCompatiblePredicates(
      predicates,
      argumentIds,
      semanticTable,
      planContext,
      indexPredicateProviderContext
    )

    def valid(variableName: String): Boolean = !argumentIds.contains(variableName)

    generalCompatiblePredicates ++ patterns.flatMap {
      case PatternRelationship(name, _, _, Seq(RelTypeName(relTypeName)), _) if valid(relTypeName) =>
        val constrainedPropNames =
          if (
            indexPredicateProviderContext.outerPlanHasUpdates || planContext.txStateHasChanges()
          ) // non-committed changes may not conform to the existence constraint, so we cannot rely on it
            Set.empty[String]
          else
            planContext.getRelationshipPropertiesWithExistenceConstraint(relTypeName)

        implicitIsNotNullPredicates(
          name,
          indexPredicateProviderContext.aggregatingProperties,
          constrainedPropNames,
          generalCompatiblePredicates
        )

      case _ => Set.empty[IndexCompatiblePredicate]
    }
  }

  private def findIndexMatches(
    variableName: String,
    propertyPredicates: Set[IndexCompatiblePredicate],
    patternRelationship: PatternRelationship,
    interestingOrderConfig: InterestingOrderConfig,
    semanticTable: SemanticTable,
    planContext: PlanContext,
    providedOrderFactory: ProvidedOrderFactory,
    findTextIndexes: Boolean,
    findRangeIndexes: Boolean,
    findPointIndexes: Boolean
  ): Set[RelationshipIndexMatch] = {
    val relTypeName = patternRelationship.types.head
    val indexMatches =
      for {
        relTypeId <- semanticTable.id(relTypeName).toSet[RelTypeId]
        indexDescriptor <- indexDescriptorsForRelType(
          relTypeId,
          planContext,
          findTextIndexes,
          findRangeIndexes,
          findPointIndexes
        )
        predicatesForIndex <- predicatesForIndex(
          indexDescriptor,
          propertyPredicates,
          interestingOrderConfig,
          semanticTable,
          planContext.getRelationshipPropertiesWithTypeConstraint(relTypeName.name),
          providedOrderFactory
        )
      } yield RelationshipIndexMatch(
        variableName,
        patternRelationship,
        relTypeName,
        relTypeId,
        predicatesForIndex.predicatesInOrder,
        predicatesForIndex.providedOrder,
        predicatesForIndex.indexOrder,
        indexDescriptor
      )
    indexMatches
  }

  private def indexDescriptorsForRelType(
    relTypeId: RelTypeId,
    planContext: PlanContext,
    findTextIndexes: Boolean,
    findRangeIndexes: Boolean,
    findPointIndexes: Boolean
  ): Iterator[IndexDescriptor] = {
    {
      if (findRangeIndexes) planContext.rangeIndexesGetForRelType(relTypeId)
      else Iterator.empty
    } ++ {
      if (findTextIndexes) planContext.textIndexesGetForRelType(relTypeId)
      else Iterator.empty
    } ++ {
      if (findPointIndexes) planContext.pointIndexesGetForRelType(relTypeId)
      else Iterator.empty
    }
  }

  /**
   * Find any implicit index compatible predicates.
   *
   * @param planContext                  planContext to ask for indexes
   * @param predicates                   the predicates in the query
   * @param explicitCompatiblePredicates the explicit index compatible predicates that were extracted from predicates
   * @param valid                        a test that can be applied to check if an implicit predicate is valid
   *                                     based on its variable and dependencies as arguments to the lambda function.
   */
  override protected def implicitIndexCompatiblePredicates(
    planContext: PlanContext,
    indexPredicateProviderContext: IndexCompatiblePredicatesProviderContext,
    predicates: Set[Expression],
    explicitCompatiblePredicates: Set[IndexCompatiblePredicate],
    valid: (LogicalVariable, Set[LogicalVariable]) => Boolean
  ): Set[IndexCompatiblePredicate] = {
    // The implicit index compatible predicates for relationship indexes come from the pattern relationships.
    // Instead of returning them here (where we don't have access to the pattern relationships), we add them in an extra step
    // in findIndexCompatiblePredicates
    Set.empty
  }
}
