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

import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.compiler.IndexLookupUnfulfillableNotification
import org.neo4j.cypher.internal.compiler.helpers.PropertyAccessHelper.PropertyAccess
import org.neo4j.cypher.internal.compiler.planner.logical.LeafPlanRestrictions
import org.neo4j.cypher.internal.compiler.planner.logical.LeafPlanner
import org.neo4j.cypher.internal.compiler.planner.logical.LogicalPlanningContext
import org.neo4j.cypher.internal.compiler.planner.logical.ordering.InterestingOrderConfig
import org.neo4j.cypher.internal.compiler.planner.logical.steps.DynamicPropertyNotifier
import org.neo4j.cypher.internal.compiler.planner.logical.steps.index.EntityIndexLeafPlanner.IndexCompatiblePredicate
import org.neo4j.cypher.internal.compiler.planner.logical.steps.index.EntityIndexLeafPlanner.getValueBehaviors
import org.neo4j.cypher.internal.compiler.planner.logical.steps.index.EntityIndexLeafPlanner.implicitIsNotNullPredicates
import org.neo4j.cypher.internal.compiler.planner.logical.steps.index.EntityIndexLeafPlanner.predicatesForIndex
import org.neo4j.cypher.internal.compiler.planner.logical.steps.index.NodeIndexLeafPlanner.findIndexMatchesForQueryGraph
import org.neo4j.cypher.internal.expressions.EntityType
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.HasLabels
import org.neo4j.cypher.internal.expressions.LabelName
import org.neo4j.cypher.internal.expressions.LabelToken
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.NODE_TYPE
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.ir.ordering.NoProvidedOrderFactory
import org.neo4j.cypher.internal.ir.ordering.ProvidedOrder
import org.neo4j.cypher.internal.ir.ordering.ProvidedOrderFactory
import org.neo4j.cypher.internal.logical.plans.AsDynamicPropertyNonScannable
import org.neo4j.cypher.internal.logical.plans.AsDynamicPropertyNonSeekable
import org.neo4j.cypher.internal.logical.plans.AsStringRangeNonSeekable
import org.neo4j.cypher.internal.logical.plans.AsValueRangeNonSeekable
import org.neo4j.cypher.internal.logical.plans.GetValueFromIndexBehavior
import org.neo4j.cypher.internal.logical.plans.IndexOrder
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.planner.spi.IndexDescriptor
import org.neo4j.cypher.internal.planner.spi.PlanContext
import org.neo4j.cypher.internal.util.LabelId

case class NodeIndexLeafPlanner(planProviders: Seq[NodeIndexPlanProvider], restrictions: LeafPlanRestrictions) extends LeafPlanner {

  override def apply(qg: QueryGraph,
                     interestingOrderConfig: InterestingOrderConfig,
                     context: LogicalPlanningContext): Set[LogicalPlan] = {
    val predicates = qg.selections.flatPredicatesSet
    val allLabelPredicatesMap: Map[String, Set[HasLabels]] = qg.selections.labelPredicates

    val indexMatches = findIndexMatchesForQueryGraph(qg, context.semanticTable, context.planContext, context.aggregatingProperties, interestingOrderConfig, context.providedOrderFactory)
    // Find plans solving given property predicates together with any label predicates from QG
    val result: Set[LogicalPlan] = if (indexMatches.isEmpty) {
      Set.empty[LogicalPlan]
    } else {
      for {
        provider <- planProviders
        plan <- provider.createPlans(indexMatches, qg.hints, qg.argumentIds, restrictions, context)
      } yield plan
    }.toSet

    issueNotifications(result, qg, context)

    result
  }

  private def issueNotifications(result: Set[LogicalPlan], qg: QueryGraph, context: LogicalPlanningContext): Unit = {
    if (result.isEmpty) {
      val nonSolvable = findNonSolvableIdentifiers(qg.selections.flatPredicates, context)
      DynamicPropertyNotifier.process(nonSolvable, IndexLookupUnfulfillableNotification, qg, context)
    }
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

object NodeIndexLeafPlanner extends IndexCompatiblePredicatesProvider {

  case class NodeIndexMatch(
                             variableName: String,
                             labelPredicate: HasLabels,
                             labelName: LabelName,
                             labelId: LabelId,
                             propertyPredicates: Seq[IndexCompatiblePredicate],
                             providedOrder: ProvidedOrder,
                             indexOrder: IndexOrder,
                             indexDescriptor: IndexDescriptor,
                           ) extends IndexMatch {

    def labelToken: LabelToken = LabelToken(labelName, labelId)

    override def predicateSet(newPredicates: Seq[IndexCompatiblePredicate], exactPredicatesCanGetValue: Boolean): PredicateSet =
      NodePredicateSet(
        variableName,
        labelPredicate,
        labelName,
        newPredicates,
        getValueBehaviors(indexDescriptor, newPredicates, exactPredicatesCanGetValue),
      )
  }

  case class NodePredicateSet(
                               variableName: String,
                               labelPredicate: HasLabels,
                               symbolicName: LabelName,
                               propertyPredicates: Seq[IndexCompatiblePredicate],
                               getValueBehaviors: Seq[GetValueFromIndexBehavior],
                             ) extends PredicateSet {

    override def allSolvedPredicates: Seq[Expression] =
      super.allSolvedPredicates :+ labelPredicate

    override def getEntityType: EntityType = NODE_TYPE
  }

  def findIndexMatchesForQueryGraph(
                                     qg: QueryGraph,
                                     semanticTable: SemanticTable,
                                     planContext: PlanContext,
                                     aggregatingProperties: Set[PropertyAccess],
                                     interestingOrderConfig: InterestingOrderConfig = InterestingOrderConfig.empty,
                                     providedOrderFactory: ProvidedOrderFactory = NoProvidedOrderFactory,
                                   ): Set[NodeIndexMatch] = {
    val predicates = qg.selections.flatPredicates.toSet
    val allLabelPredicatesMap: Map[String, Set[HasLabels]] = qg.selections.labelPredicates

    if (allLabelPredicatesMap.isEmpty) {
      Set.empty[NodeIndexMatch]
    } else {
      val compatiblePropertyPredicates = findIndexCompatiblePredicates(
        predicates,
        qg.argumentIds,
        semanticTable,
        planContext,
        aggregatingProperties
      )

      val matches = for {
        propertyPredicates <- compatiblePropertyPredicates.groupBy(_.name)
        variableName = propertyPredicates._1
        labelPredicates = allLabelPredicatesMap.getOrElse(variableName, Set.empty)
        indexMatch <- findIndexMatches(variableName, propertyPredicates._2, labelPredicates, interestingOrderConfig, semanticTable, planContext, providedOrderFactory)
      } yield indexMatch
      matches.toSet
    }
  }

  private def findIndexMatches(variableName: String,
                               indexCompatiblePredicates: Set[IndexCompatiblePredicate],
                               labelPredicates: Set[HasLabels],
                               interestingOrderConfig: InterestingOrderConfig,
                               semanticTable: SemanticTable,
                               planContext: PlanContext,
                               providedOrderFactory: ProvidedOrderFactory,
                              ): Set[NodeIndexMatch] = for {
    labelPredicate <- labelPredicates
    labelName <- labelPredicate.labels
    labelId: LabelId <- semanticTable.id(labelName).toSet
    indexDescriptor: IndexDescriptor <- planContext.indexesGetForLabel(labelId)
    predicatesForIndex <- predicatesForIndex(indexDescriptor, indexCompatiblePredicates, interestingOrderConfig, semanticTable, providedOrderFactory)
    indexMatch = NodeIndexMatch(
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

  override protected def implicitIndexCompatiblePredicates(planContext: PlanContext,
                                                           aggregatingProperties: Set[PropertyAccess],
                                                           predicates: Set[Expression],
                                                           explicitCompatiblePredicates: Set[IndexCompatiblePredicate],
                                                           valid: (LogicalVariable, Set[LogicalVariable]) => Boolean): Set[IndexCompatiblePredicate] = {
    predicates.flatMap {
      // n:User ... aggregation(n.prop)
      // or
      // n:User with CREATE CONSTRAINT ON (n:User) ASSERT n.prop IS NOT NULL
      case HasLabels(variable: Variable, labels) if valid(variable, Set.empty) =>
        val constrainedPropNames = planContext.getNodePropertiesWithExistenceConstraint(labels.head.name)
        implicitIsNotNullPredicates(variable, aggregatingProperties, constrainedPropNames, explicitCompatiblePredicates)

      case _ =>
        Set.empty[IndexCompatiblePredicate]
    }
  }
}
