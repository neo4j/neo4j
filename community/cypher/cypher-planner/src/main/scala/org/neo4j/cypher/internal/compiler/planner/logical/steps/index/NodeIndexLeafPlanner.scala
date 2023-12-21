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
import org.neo4j.cypher.internal.compiler.planner.logical.steps.index.NodeIndexLeafPlanner.findIndexMatchesForQueryGraph
import org.neo4j.cypher.internal.expressions.EntityType
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.HasLabels
import org.neo4j.cypher.internal.expressions.LabelName
import org.neo4j.cypher.internal.expressions.LabelToken
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.NODE_TYPE
import org.neo4j.cypher.internal.expressions.UnPositionedVariable.varFor
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.ir.ordering.NoProvidedOrderFactory
import org.neo4j.cypher.internal.ir.ordering.ProvidedOrder
import org.neo4j.cypher.internal.ir.ordering.ProvidedOrderFactory
import org.neo4j.cypher.internal.logical.plans.GetValueFromIndexBehavior
import org.neo4j.cypher.internal.logical.plans.IndexOrder
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.planner.spi.IndexDescriptor
import org.neo4j.cypher.internal.planner.spi.PlanContext
import org.neo4j.cypher.internal.util.LabelId
import org.neo4j.notifications.NodeIndexLookupUnfulfillableNotification

case class NodeIndexLeafPlanner(planProviders: Seq[NodeIndexPlanProvider], restrictions: LeafPlanRestrictions)
    extends LeafPlanner {

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

    // Find plans solving given property predicates together with any label predicates from QG
    val result: Set[LogicalPlan] =
      if (indexMatches.isEmpty) {
        Set.empty[LogicalPlan]
      } else {
        for {
          provider <- planProviders
          plan <- provider.createPlans(indexMatches, qg.hints, qg.argumentIds, restrictions, context)
        } yield plan
      }.toSet

    DynamicPropertyNotifier.issueNotifications(result, NodeIndexLookupUnfulfillableNotification, qg, NODE_TYPE, context)

    result
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
    indexDescriptor: IndexDescriptor
  ) extends IndexMatch {

    def labelToken: LabelToken = LabelToken(labelName, labelId)

    override def predicateSet(
      newPredicates: Seq[IndexCompatiblePredicate],
      exactPredicatesCanGetValue: Boolean
    ): PredicateSet =
      NodePredicateSet(
        variableName,
        labelPredicate,
        labelName,
        newPredicates,
        getValueBehaviors(indexDescriptor, newPredicates, exactPredicatesCanGetValue)
      )
  }

  case class NodePredicateSet(
    variableName: String,
    labelPredicate: HasLabels,
    symbolicName: LabelName,
    propertyPredicates: Seq[IndexCompatiblePredicate],
    getValueBehaviors: Seq[GetValueFromIndexBehavior]
  ) extends PredicateSet {

    override def allSolvedPredicates: Seq[Expression] =
      super.allSolvedPredicates :+ labelPredicate

    override def getEntityType: EntityType = NODE_TYPE
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
  ): Set[NodeIndexMatch] = {
    val predicates = qg.selections.flatPredicates.toSet
    val allLabelPredicatesMap: Map[LogicalVariable, Set[HasLabels]] = qg.selections.labelPredicates

    if (allLabelPredicatesMap.isEmpty) {
      Set.empty[NodeIndexMatch]
    } else {
      val compatiblePropertyPredicates = findIndexCompatiblePredicates(
        predicates,
        qg.argumentIds,
        semanticTable,
        planContext,
        indexPredicateProviderContext
      )

      val matches = for {
        propertyPredicates <- compatiblePropertyPredicates.groupBy(_.name)
        variableName = propertyPredicates._1
        labelPredicates = allLabelPredicatesMap.getOrElse(varFor(variableName), Set.empty)
        indexMatch <- findIndexMatches(
          variableName,
          propertyPredicates._2,
          labelPredicates,
          interestingOrderConfig,
          semanticTable,
          planContext,
          providedOrderFactory,
          findTextIndexes,
          findRangeIndexes,
          findPointIndexes
        )
      } yield indexMatch
      matches.toSet
    }
  }

  private def findIndexMatches(
    variableName: String,
    indexCompatiblePredicates: Set[IndexCompatiblePredicate],
    labelPredicates: Set[HasLabels],
    interestingOrderConfig: InterestingOrderConfig,
    semanticTable: SemanticTable,
    planContext: PlanContext,
    providedOrderFactory: ProvidedOrderFactory,
    findTextIndexes: Boolean,
    findRangeIndexes: Boolean,
    findPointIndexes: Boolean
  ): Set[NodeIndexMatch] = for {
    labelPredicate <- labelPredicates
    labelName <- labelPredicate.labels
    labelId: LabelId <- semanticTable.id(labelName).toSet
    indexDescriptor <- indexDescriptorsForLabel(
      labelId,
      planContext,
      findTextIndexes,
      findRangeIndexes,
      findPointIndexes
    )
    predicatesForIndex <- predicatesForIndex(
      indexDescriptor,
      indexCompatiblePredicates,
      interestingOrderConfig,
      semanticTable,
      planContext.getNodePropertiesWithTypeConstraint(labelName.name),
      providedOrderFactory
    )
    indexMatch = NodeIndexMatch(
      variableName,
      labelPredicate,
      labelName,
      labelId,
      predicatesForIndex.predicatesInOrder,
      predicatesForIndex.providedOrder,
      predicatesForIndex.indexOrder,
      indexDescriptor
    )
  } yield indexMatch

  private def indexDescriptorsForLabel(
    labelId: LabelId,
    planContext: PlanContext,
    findTextIndexes: Boolean,
    findRangeIndexes: Boolean,
    findPointIndexes: Boolean
  ): Iterator[IndexDescriptor] = {
    {
      if (findRangeIndexes) planContext.rangeIndexesGetForLabel(labelId)
      else Iterator.empty
    } ++ {
      if (findTextIndexes) planContext.textIndexesGetForLabel(labelId)
      else Iterator.empty
    } ++ {
      if (findPointIndexes) planContext.pointIndexesGetForLabel(labelId)
      else Iterator.empty
    }
  }

  override protected def implicitIndexCompatiblePredicates(
    planContext: PlanContext,
    indexPredicateProviderContext: IndexCompatiblePredicatesProviderContext,
    predicates: Set[Expression],
    explicitCompatiblePredicates: Set[IndexCompatiblePredicate],
    valid: (LogicalVariable, Set[LogicalVariable]) => Boolean
  ): Set[IndexCompatiblePredicate] = {
    predicates.flatMap {
      // n:User ... aggregation(n.prop)
      // or
      // n:User with CREATE CONSTRAINT FOR (n:User) REQUIRE n.prop IS NOT NULL
      case HasLabels(variable: Variable, labels) if valid(variable, Set.empty) =>
        val constrainedPropNames =
          if (
            indexPredicateProviderContext.outerPlanHasUpdates || planContext.txStateHasChanges()
          ) // non-committed changes may not conform to the existence constraint, so we cannot rely on it
            Set.empty[String]
          else
            planContext.getNodePropertiesWithExistenceConstraint(labels.head.name) // HasLabels has been normalized in normalizeComparisons to only have one label each, which is why we can look only at the head here.

        implicitIsNotNullPredicates(
          variable,
          indexPredicateProviderContext.aggregatingProperties,
          constrainedPropNames,
          explicitCompatiblePredicates
        )

      case _ =>
        Set.empty[IndexCompatiblePredicate]
    }
  }
}
