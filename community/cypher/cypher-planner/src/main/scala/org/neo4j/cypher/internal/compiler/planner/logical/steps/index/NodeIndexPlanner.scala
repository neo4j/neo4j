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
import org.neo4j.cypher.internal.compiler.IndexLookupUnfulfillableNotification
import org.neo4j.cypher.internal.compiler.planner.logical.LeafPlanRestrictions
import org.neo4j.cypher.internal.compiler.planner.logical.LeafPlansForVariable
import org.neo4j.cypher.internal.compiler.planner.logical.LeafPlansForVariable.maybeLeafPlans
import org.neo4j.cypher.internal.compiler.planner.logical.LogicalPlanningContext
import org.neo4j.cypher.internal.compiler.planner.logical.ordering.InterestingOrderConfig
import org.neo4j.cypher.internal.compiler.planner.logical.steps.DynamicPropertyNotifier
import org.neo4j.cypher.internal.compiler.planner.logical.steps.index.EntityIndexLeafPlanner.IndexCompatiblePredicate
import org.neo4j.cypher.internal.compiler.planner.logical.steps.index.EntityIndexLeafPlanner.getValueBehaviors
import org.neo4j.cypher.internal.compiler.planner.logical.steps.index.EntityIndexLeafPlanner.implicitExistsPredicates
import org.neo4j.cypher.internal.compiler.planner.logical.steps.index.EntityIndexLeafPlanner.predicatesForIndex
import org.neo4j.cypher.internal.compiler.planner.logical.steps.index.NodeIndexPlanner.IndexCandidateHasLabelsPredicate
import org.neo4j.cypher.internal.compiler.planner.logical.steps.index.NodeIndexPlanner.IndexMatch
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.HasLabels
import org.neo4j.cypher.internal.expressions.LabelName
import org.neo4j.cypher.internal.expressions.LabelOrRelTypeName
import org.neo4j.cypher.internal.expressions.LabelToken
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.NODE_TYPE
import org.neo4j.cypher.internal.expressions.PropertyKeyToken
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.ir.PatternRelationship
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.ir.ordering.ProvidedOrder
import org.neo4j.cypher.internal.logical.plans.AsDynamicPropertyNonScannable
import org.neo4j.cypher.internal.logical.plans.AsDynamicPropertyNonSeekable
import org.neo4j.cypher.internal.logical.plans.AsStringRangeNonSeekable
import org.neo4j.cypher.internal.logical.plans.AsValueRangeNonSeekable
import org.neo4j.cypher.internal.logical.plans.ExistenceQueryExpression
import org.neo4j.cypher.internal.logical.plans.GetValueFromIndexBehavior
import org.neo4j.cypher.internal.logical.plans.IndexOrder
import org.neo4j.cypher.internal.logical.plans.IndexedProperty
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.planner.spi.IndexDescriptor
import org.neo4j.cypher.internal.util.LabelId

case class NodeIndexPlanner(planProviders: Seq[NodeIndexPlanProvider], restrictions: LeafPlanRestrictions) extends EntityIndexLeafPlanner {

  override def producePlanFor(predicates: Set[Expression],
                              qg: QueryGraph,
                              interestingOrderConfig: InterestingOrderConfig,
                              context: LogicalPlanningContext): Set[LeafPlansForVariable] = {
    val allLabelPredicatesMap: Map[String, Set[HasLabels]] = qg.selections.labelPredicates

    // Find plans solving given property predicates together with any label predicates from QG
    val resultFromPropertyPredicates = if (allLabelPredicatesMap.isEmpty) Set.empty[LeafPlansForVariable] else {
      val compatiblePropertyPredicates: Set[IndexCompatiblePredicate] = findIndexCompatiblePredicates(predicates, qg.argumentIds, context)
      compatiblePropertyPredicates.map(_.name).flatMap { variableName =>
        val labelPredicates = allLabelPredicatesMap.getOrElse(variableName, Set.empty)
        val propertyPredicates = compatiblePropertyPredicates.filter(p => p.name == variableName)
        val plansForVariable = producePlansForSpecificVariable(variableName, propertyPredicates, labelPredicates, qg.hints, qg.argumentIds, context, interestingOrderConfig)
        maybeLeafPlans(variableName, plansForVariable)
      }
    }

    // Find plans solving given label predicates together with any property predicates from QG
    val resultFromLabelPredicates = {
      lazy val allIndexCompatibles: Set[IndexCompatiblePredicate] = findIndexCompatiblePredicates(qg.selections.flatPredicates.toSet, qg.argumentIds, context)
      val candidateLabelPredicates = predicates.collect { case p@HasLabels(v: LogicalVariable, _) => IndexCandidateHasLabelsPredicate(v.name, p) }
      candidateLabelPredicates.flatMap { candidate =>
        val name = candidate.name
        val labelPredicates = Set(candidate.hasLabels)
        val propertyPredicates = allIndexCompatibles.filter(p => p.name == name)
        val plansForVariable = producePlansForSpecificVariable(name, propertyPredicates, labelPredicates, qg.hints, qg.argumentIds, context, interestingOrderConfig)
        maybeLeafPlans(name, plansForVariable)
      }
    }

    val result = resultFromPropertyPredicates ++ resultFromLabelPredicates

    issueNotifications(result, qg, context)

    result
  }

  override protected def implicitIndexCompatiblePredicates(context: LogicalPlanningContext,
                                                           predicates: Set[Expression],
                                                           explicitCompatiblePredicates: Set[IndexCompatiblePredicate],
                                                           valid: (LogicalVariable, Set[LogicalVariable]) => Boolean): Set[IndexCompatiblePredicate] = {
    predicates.flatMap {
      // n:User ... aggregation(n.prop)
      // or
      // n:User with CREATE CONSTRAINT ON (n:User) ASSERT EXISTS (n.prop)
      case HasLabels(variable: Variable, labels) if valid(variable, Set.empty) =>
        val constrainedPropNames = context.planContext.getNodePropertiesWithExistenceConstraint(labels.head.name)
        implicitExistsPredicates(variable, context, constrainedPropNames, explicitCompatiblePredicates)

      case _ =>
        Set.empty[IndexCompatiblePredicate]
    }
  }

  private def producePlansForSpecificVariable(idName: String,
                                              indexCompatiblePredicates: Set[IndexCompatiblePredicate],
                                              labelPredicates: Set[HasLabels],
                                              hints: Set[Hint],
                                              argumentIds: Set[String],
                                              context: LogicalPlanningContext,
                                              interestingOrderConfig: InterestingOrderConfig): Set[LogicalPlan] = {
    val indexMatches = findIndexMatches(idName, indexCompatiblePredicates, labelPredicates, interestingOrderConfig, context)
    val plans = for {
      provider <- planProviders
      plan <- provider.createPlans(indexMatches, hints, argumentIds, restrictions, context)
    } yield plan
    plans.toSet
  }

  private def findIndexMatches(
    variableName: String,
    indexCompatiblePredicates: Set[IndexCompatiblePredicate],
    labelPredicates: Set[HasLabels],
    interestingOrderConfig: InterestingOrderConfig,
    context: LogicalPlanningContext,
  ): Set[IndexMatch] = for {
    labelPredicate <- labelPredicates
    labelName <- labelPredicate.labels
    labelId: LabelId <- context.semanticTable.id(labelName).toSet
    indexDescriptor: IndexDescriptor <- findIndexesForLabel(labelId, context).toSet
    predicatesForIndex <- predicatesForIndex(indexDescriptor, indexCompatiblePredicates, interestingOrderConfig, context.semanticTable)
    indexMatch = IndexMatch(
      variableName,
      labelPredicate,
      labelName,
      labelId,
      predicatesForIndex.predicatesInOrder,
      predicatesForIndex.providedOrder,
      predicatesForIndex.indexOrder,
      indexDescriptor,
    )
  } yield indexMatch

  private def findIndexesForLabel(labelId: Int, context: LogicalPlanningContext): Iterator[IndexDescriptor] =
    context.planContext.indexesGetForLabel(labelId)

  private def issueNotifications(result: Set[LeafPlansForVariable], qg: QueryGraph, context: LogicalPlanningContext): Unit = {
    val nonSolvable = findNonSolvableIdentifiers(qg.selections.flatPredicates, context)
    DynamicPropertyNotifier.process(nonSolvable, IndexLookupUnfulfillableNotification, qg, context)
  }

  private def findNonSolvableIdentifiers(predicates: Seq[Expression], context: LogicalPlanningContext): Set[Variable] = {
    def isNode(variable: Variable) = context.semanticTable.isNode(variable)

    predicates.flatMap {
      // n['some' + n.prop] IN [ ... ]
      case AsDynamicPropertyNonSeekable(nonSeekableId) if isNode(nonSeekableId) =>
        Some(nonSeekableId)
      // n['some' + n.prop] STARTS WITH "prefix%..."
      case AsStringRangeNonSeekable(nonSeekableId) if isNode(nonSeekableId) =>
        Some(nonSeekableId)
      // n['some' + n.prop] <|<=|>|>= value
      case AsValueRangeNonSeekable(nonSeekableId) if isNode(nonSeekableId) =>
        Some(nonSeekableId)

      case AsDynamicPropertyNonScannable(nonScannableId) if isNode(nonScannableId) =>
        Some(nonScannableId)

      case _ =>
        None
    }.toSet
  }
}

object NodeIndexPlanner {

  /**
   * A label predicate that is a candidate for being solved by an index
   *
   * @param name      Variable name
   * @param hasLabels Original expression
   */
  case class IndexCandidateHasLabelsPredicate(name: String, hasLabels: HasLabels)

  /**
   * Represents a match between a set of predicates and an existing index
   * that covers the label and all properties in the predicates
   */
  case class IndexMatch(
    variableName: String,
    labelPredicate: HasLabels,
    labelName: LabelName,
    labelId: LabelId,
    propertyPredicates: Seq[IndexCompatiblePredicate],
    providedOrder: ProvidedOrder,
    indexOrder: IndexOrder,
    indexDescriptor: IndexDescriptor,
  ) {

    def labelToken: LabelToken = LabelToken(labelName, labelId)

    def predicateSet(newPredicates: Seq[IndexCompatiblePredicate], exactPredicatesCanGetValue: Boolean): PredicateSet =
      PredicateSet(
        variableName,
        labelPredicate,
        labelName,
        newPredicates,
        getValueBehaviors(indexDescriptor, newPredicates, exactPredicatesCanGetValue),
      )

    def hasImplicitPredicates: Boolean =
      propertyPredicates.exists(_.isImplicit)
  }

  /**
   * A set of predicates to create an index leaf plan from
   */
  case class PredicateSet(
    variableName: String,
    labelPredicate: HasLabels,
    labelName: LabelName,
    propertyPredicates: Seq[IndexCompatiblePredicate],
    getValueBehaviors: Seq[GetValueFromIndexBehavior],
  ) {
    def allSolvedPredicates: Seq[Expression] =
      propertyPredicates.flatMap(_.solvedPredicate) :+ labelPredicate

    def indexedProperties(context: LogicalPlanningContext): Seq[IndexedProperty] = propertyPredicates.zip(getValueBehaviors).map {
      case (predicate, behavior) =>
        val propertyName = predicate.propertyKeyName
        val getValue = behavior
        IndexedProperty(PropertyKeyToken(propertyName, context.semanticTable.id(propertyName).head), getValue, NODE_TYPE)
    }

    def matchingHints(hints: Set[Hint]): Set[UsingIndexHint] = {
      val propertyNames = propertyPredicates.map(_.propertyKeyName.name)
      hints.collect {
        case hint@UsingIndexHint(Variable(`variableName`), LabelOrRelTypeName(labelName.name), propertyKeyNames, _)
          if propertyKeyNames.map(_.name) == propertyNames => hint
      }
    }
  }
}
