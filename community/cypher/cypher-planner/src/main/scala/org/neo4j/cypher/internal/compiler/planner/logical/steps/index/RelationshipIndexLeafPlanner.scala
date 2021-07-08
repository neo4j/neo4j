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
import org.neo4j.cypher.internal.ast.UsingIndexHint
import org.neo4j.cypher.internal.compiler.planner.logical.LeafPlanRestrictions
import org.neo4j.cypher.internal.compiler.planner.logical.LeafPlansForVariable
import org.neo4j.cypher.internal.compiler.planner.logical.LogicalPlanningContext
import org.neo4j.cypher.internal.compiler.planner.logical.ordering.InterestingOrderConfig
import org.neo4j.cypher.internal.compiler.planner.logical.steps.index.EntityIndexLeafPlanner.IndexCompatiblePredicate
import org.neo4j.cypher.internal.compiler.planner.logical.steps.index.EntityIndexLeafPlanner.getValueBehaviors
import org.neo4j.cypher.internal.compiler.planner.logical.steps.index.EntityIndexLeafPlanner.implicitExistsPredicates
import org.neo4j.cypher.internal.compiler.planner.logical.steps.index.EntityIndexLeafPlanner.predicatesForIndex
import org.neo4j.cypher.internal.compiler.planner.logical.steps.index.EntityIndexLeafPlanner.variable
import org.neo4j.cypher.internal.compiler.planner.logical.steps.index.RelationshipIndexLeafPlanner.IndexMatch
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.LabelOrRelTypeName
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.PropertyKeyToken
import org.neo4j.cypher.internal.expressions.RELATIONSHIP_TYPE
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.expressions.RelationshipTypeToken
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.ir.PatternRelationship
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.ir.SimplePatternLength
import org.neo4j.cypher.internal.ir.ordering.ProvidedOrder
import org.neo4j.cypher.internal.logical.plans.GetValueFromIndexBehavior
import org.neo4j.cypher.internal.logical.plans.IndexOrder
import org.neo4j.cypher.internal.logical.plans.IndexedProperty
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.planner.spi.IndexDescriptor
import org.neo4j.cypher.internal.util.RelTypeId

case class RelationshipIndexLeafPlanner(planProviders: Seq[RelationshipIndexPlanProvider], restrictions: LeafPlanRestrictions) extends EntityIndexLeafPlanner {

  override def producePlanFor(predicates: Set[Expression],
                              qg: QueryGraph,
                              interestingOrderConfig: InterestingOrderConfig,
                              context: LogicalPlanningContext): Set[LeafPlansForVariable] = {
    def shouldIgnore(pattern: PatternRelationship) =
      pattern.left == pattern.right ||
        pattern.coveredIds.intersect(qg.argumentIds).nonEmpty

    if (!context.enablePlanningRelationshipIndexes)
      return Set.empty

    val patternRelationshipsMap: Map[String, PatternRelationship] = qg.patternRelationships.collect({
      case pattern@PatternRelationship(name, _, _, Seq(_), SimplePatternLength) if !shouldIgnore(pattern) => name -> pattern
    }).toMap

    // Find plans solving given property predicates together with any label predicates from QG
    val resultFromPropertyPredicates = if (patternRelationshipsMap.isEmpty) Set.empty[LeafPlansForVariable] else {
      val compatiblePropertyPredicates = findIndexCompatiblePredicates(predicates, qg.argumentIds, context, patternRelationshipsMap.values)

      for {
        propertyPredicates <- compatiblePropertyPredicates.groupBy(_.name)
        variableName = propertyPredicates._1
        patternRelationship <- patternRelationshipsMap.get(variableName)
        plan <- producePlanForSpecificVariable(variableName, propertyPredicates._2, patternRelationship, qg.hints, qg.argumentIds, context,
        interestingOrderConfig)
      } yield plan
    }

    resultFromPropertyPredicates.toSet
  }

  override protected def implicitIndexCompatiblePredicates(context: LogicalPlanningContext,
                                                           predicates: Set[Expression],
                                                           explicitCompatiblePredicates: Set[IndexCompatiblePredicate],
                                                           valid: (LogicalVariable, Set[LogicalVariable]) => Boolean): Set[IndexCompatiblePredicate] = {
    // The implicit index compatible predicates for relationship indexes come from the pattern relationships.
    // Instead of returning them here (where we don't have access to the pattern relationships), we add them in an extra step
    // in findIndexCompatiblePredicates
    Set.empty
  }

  private def findIndexCompatiblePredicates(predicates: Set[Expression],
                                            argumentIds: Set[String],
                                            context: LogicalPlanningContext,
                                            patterns: Iterable[PatternRelationship]): Set[IndexCompatiblePredicate] = {
    val generalCompatiblePredicates = findIndexCompatiblePredicates(predicates, argumentIds, context)

    def valid(variableName: String): Boolean = !argumentIds.contains(variableName)

    generalCompatiblePredicates ++ patterns.flatMap {
      case PatternRelationship(name, _, _, Seq(RelTypeName(relTypeName)), _) if valid(relTypeName) =>
        val constrainedPropNames =
          if (context.indexCompatiblePredicatesProviderContext.outerPlanHasUpdates || context.planContext.txStateHasChanges()) // non-committed changes may not conform to the existence constraint, so we cannot rely on it
            Set.empty[String]
          else
            context.planContext.getRelationshipPropertiesWithExistenceConstraint(relTypeName)

        implicitExistsPredicates(variable(name), context, constrainedPropNames, generalCompatiblePredicates)

      case _ => Set.empty[IndexCompatiblePredicate]
    }
  }

  def producePlanForSpecificVariable(variableName: String,
                                     propertyPredicates: Set[IndexCompatiblePredicate],
                                     patternRelationship: PatternRelationship,
                                     hints: Set[Hint],
                                     argumentIds: Set[String],
                                     context: LogicalPlanningContext,
                                     interestingOrderConfig: InterestingOrderConfig): Option[LeafPlansForVariable] = {
    val relTypeName = patternRelationship.types.head
    val indexMatches = for {
      relTypeId <- context.semanticTable.id(relTypeName).toSet[RelTypeId]
      indexDescriptor <- context.planContext.indexesGetForRelType(relTypeId)
      predicatesForIndex <- predicatesForIndex(indexDescriptor, propertyPredicates, interestingOrderConfig, context.semanticTable)
    } yield IndexMatch(variableName, patternRelationship, relTypeName, relTypeId,
      predicatesForIndex.predicatesInOrder,
      predicatesForIndex.providedOrder,
      predicatesForIndex.indexOrder,
      indexDescriptor)
    val plans: Seq[LogicalPlan] = for {
      provider <- planProviders
      plan <- provider.createPlans(indexMatches, hints, argumentIds, restrictions, context)
    } yield plan
    if (plans.nonEmpty) {
      Some(LeafPlansForVariable(variableName, plans.toSet))
    } else {
      None
    }
  }
}

object RelationshipIndexLeafPlanner {
  case class IndexMatch(
                         variableName: String,
                         patternRelationship: PatternRelationship,
                         relTypeName: RelTypeName,
                         relTypeId: RelTypeId,
                         propertyPredicates: Seq[IndexCompatiblePredicate],
                         providedOrder: ProvidedOrder,
                         indexOrder: IndexOrder,
                         indexDescriptor: IndexDescriptor,
                       ) {
    def relationshipTypeToken: RelationshipTypeToken = RelationshipTypeToken(relTypeName, relTypeId)

    def predicateSet(newPredicates: Seq[IndexCompatiblePredicate],
                     exactPredicatesCanGetValue: Boolean
                    ): PredicateSet = PredicateSet(
      variableName,
      relTypeName,
      newPredicates,
      getValueBehaviors(indexDescriptor, newPredicates, exactPredicatesCanGetValue)
    )

  }

  /**
   * A set of predicates to create an index leaf plan from
   */
  case class PredicateSet(
                           variableName: String,
                           relTypeName: RelTypeName,
                           propertyPredicates: Seq[IndexCompatiblePredicate],
                           getValueBehaviors: Seq[GetValueFromIndexBehavior],
                         ) {

    def allSolvedPredicates: Seq[Expression] =
      propertyPredicates.flatMap(_.solvedPredicate)

    def indexedProperties(context: LogicalPlanningContext): Seq[IndexedProperty] = propertyPredicates.zip(getValueBehaviors).map {
      case (predicate, behavior) =>
        val propertyName = predicate.propertyKeyName
        val getValue = behavior
        IndexedProperty(PropertyKeyToken(propertyName, context.semanticTable.id(propertyName).head), getValue, RELATIONSHIP_TYPE)
    }

    def matchingHints(hints: Set[Hint]): Set[UsingIndexHint] = {
      val propertyNames = propertyPredicates.map(_.propertyKeyName.name)
      hints.collect {
        case hint@UsingIndexHint(Variable(`variableName`), LabelOrRelTypeName(relTypeName.name), propertyKeyNames, _)
          if propertyKeyNames.map(_.name) == propertyNames => hint
      }
    }
  }
}
