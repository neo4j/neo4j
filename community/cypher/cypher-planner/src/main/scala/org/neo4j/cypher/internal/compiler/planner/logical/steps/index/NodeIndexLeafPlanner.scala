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
import org.neo4j.cypher.internal.ast.semantics.TokenTable
import org.neo4j.cypher.internal.compiler.planner.logical.LeafPlanner
import org.neo4j.cypher.internal.compiler.planner.logical.LogicalPlanningContext
import org.neo4j.cypher.internal.compiler.planner.logical.Metrics.LabelInfo
import org.neo4j.cypher.internal.compiler.planner.logical.impliedLabelPredicate
import org.neo4j.cypher.internal.compiler.planner.logical.ordering.InterestingOrderConfig
import org.neo4j.cypher.internal.compiler.planner.logical.schema.GraphSchemaOptimizations
import org.neo4j.cypher.internal.compiler.planner.logical.steps.DynamicPropertyNotifier
import org.neo4j.cypher.internal.compiler.planner.logical.steps.index.EntityIndexLeafPlanner.IndexCompatiblePredicate
import org.neo4j.cypher.internal.compiler.planner.logical.steps.index.EntityIndexLeafPlanner.implicitIsNotNullPredicates
import org.neo4j.cypher.internal.compiler.planner.logical.steps.index.EntityIndexLeafPlanner.predicatesForIndex
import org.neo4j.cypher.internal.compiler.planner.logical.steps.index.NodeIndexLeafPlanner.findIndexMatchesForQueryGraph
import org.neo4j.cypher.internal.expressions.EntityType
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.HasLabels
import org.neo4j.cypher.internal.expressions.HasLabelsExpression
import org.neo4j.cypher.internal.expressions.ImpliedLabel
import org.neo4j.cypher.internal.expressions.LabelName
import org.neo4j.cypher.internal.expressions.LabelToken
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.NODE_TYPE
import org.neo4j.cypher.internal.expressions.PartialPredicate.PartialPredicateWrapper
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.logical.plans.IndexOrder
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.ordering.ProvidedOrder
import org.neo4j.cypher.internal.logical.plans.ordering.ProvidedOrderFactory
import org.neo4j.cypher.internal.notification.NodeIndexLookupUnfulfillableNotification
import org.neo4j.cypher.internal.planner.spi.IndexDescriptor
import org.neo4j.cypher.internal.planner.spi.PlanContext
import org.neo4j.cypher.internal.util.LabelId

case class NodeIndexLeafPlanner(planProviders: Seq[NodeIndexPlanProvider])
    extends LeafPlanner {

  override def apply(
    qg: QueryGraph,
    interestingOrderConfig: InterestingOrderConfig,
    context: LogicalPlanningContext
  ): Set[LogicalPlan] = {
    val indexMatches = findIndexMatchesForQueryGraph(
      qg.addPredicates(context.plannerState.overlappingMulticomponentPredicates),
      context.semanticTable,
      context.staticComponents.planContext,
      context.plannerState.indexCompatiblePredicatesProviderContext,
      interestingOrderConfig,
      context.providedOrderFactory,
      context.staticComponents.graphSchemaOptimizations
    )

    // Find plans solving given property predicates together with any label predicates from QG
    val result: Set[LogicalPlan] =
      if (indexMatches.isEmpty) {
        Set.empty[LogicalPlan]
      } else {
        for {
          provider <- planProviders
          plan <- provider.createPlans(indexMatches, qg, context)
        } yield plan
      }.toSet

    DynamicPropertyNotifier.issueNotifications(
      result,
      NodeIndexLookupUnfulfillableNotification.apply,
      qg,
      NODE_TYPE,
      context
    )

    result
  }
}

object NodeIndexLeafPlanner extends IndexCompatiblePredicatesProvider {

  case class NodeIndexMatch(
    variable: LogicalVariable,
    labelPredicate: HasLabelsExpression,
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
      exactPredicatesCanGetValue: Boolean,
      context: LogicalPlanningContext,
      queryGraph: QueryGraph
    ): PredicateSet =
      NodePredicateSet(
        variable,
        labelPredicate,
        labelName,
        context.settings.remoteBatchPropertiesStrategy.getValueFromIndexBehaviors(
          indexDescriptor,
          newPredicates,
          exactPredicatesCanGetValue,
          context,
          queryGraph
        )
      )
  }

  case class NodePredicateSet(
    variable: LogicalVariable,
    labelPredicate: HasLabelsExpression,
    symbolicName: LabelName,
    propertyPredicates: Seq[IndexCompatiblePredicateWithValueBehavior]
  ) extends PredicateSet {

    override def allSolvedPredicates: Seq[Expression] =
      super.allSolvedPredicates :+ solvedLabelPredicate

    override def getEntityType: EntityType = NODE_TYPE

    private def solvedLabelPredicate: Expression = {
      labelPredicate match {
        case labelPredicate @ HasLabels(_, labels) => if (labels == Seq(symbolicName)) {
            labelPredicate
          } else {
            // using index with a parent (implied) label
            PartialPredicateWrapper(
              coveredPredicate =
                labelPredicate.copy(labels = Seq(symbolicName))(labelPredicate.position, labelPredicate.isPostfix),
              coveringPredicate = labelPredicate
            )
          }
        case il: ImpliedLabel => il
        case _ =>
          throw new IllegalStateException(
            s"Expected label predicate to be HasLabels or ImpliedLabel, but got: $labelPredicate"
          )
      }

    }
  }

  def findIndexMatchesForQueryGraph(
    qg: QueryGraph,
    semanticTable: SemanticTable,
    planContext: PlanContext,
    indexPredicateProviderContext: IndexCompatiblePredicatesProviderContext,
    interestingOrderConfig: InterestingOrderConfig,
    providedOrderFactory: ProvidedOrderFactory,
    graphSchemaOptimizations: GraphSchemaOptimizations,
    findTextIndexes: Boolean = true,
    findRangeIndexes: Boolean = true,
    findPointIndexes: Boolean = true
  ): Set[NodeIndexMatch] = {
    val predicates = qg.selections.flatPredicates.toSet
    val allLabelPredicatesMap: Map[LogicalVariable, Set[HasLabels]] = qg.selections.labelPredicates

    val impliedEndpointLabelsMap = graphSchemaOptimizations.impliedEndpointLabelsMap(qg.patternRelationships)

    if (allLabelPredicatesMap.isEmpty && impliedEndpointLabelsMap.isEmpty) {
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
        propertyPredicates <- compatiblePropertyPredicates.groupBy(_.variable)
        variable = propertyPredicates._1

        indexMatch <- findIndexMatches(
          variable,
          propertyPredicates._2,
          labelPredicates(variable, allLabelPredicatesMap, impliedEndpointLabelsMap),
          interestingOrderConfig,
          semanticTable,
          planContext,
          providedOrderFactory,
          findTextIndexes,
          findRangeIndexes,
          findPointIndexes,
          graphSchemaOptimizations
        )
      } yield indexMatch
      matches.toSet
    }
  }

  private def labelPredicates(
    variable: LogicalVariable,
    allLabelPredicatesMap: Map[LogicalVariable, Set[HasLabels]],
    impliedEndpointLabelsMap: LabelInfo
  ): Set[HasLabelsExpression] = {
    val explicitLabelPredicates = allLabelPredicatesMap.getOrElse(variable, Set.empty)
    val explicitLabelNames = explicitLabelPredicates.flatMap(_.hasLabels.labels)
    val impliedLabelPredicates =
      impliedEndpointLabelsMap.getOrElse(variable, Set.empty)
        .diff(explicitLabelNames)
        .map(label => impliedLabelPredicate(variable, label))
    impliedLabelPredicates ++ explicitLabelPredicates
  }

  private def findIndexMatches(
    variable: LogicalVariable,
    indexCompatiblePredicates: Set[IndexCompatiblePredicate],
    labelPredicates: Set[HasLabelsExpression],
    interestingOrderConfig: InterestingOrderConfig,
    tokenTable: TokenTable,
    planContext: PlanContext,
    providedOrderFactory: ProvidedOrderFactory,
    findTextIndexes: Boolean,
    findRangeIndexes: Boolean,
    findPointIndexes: Boolean,
    graphSchemaOptimizations: GraphSchemaOptimizations
  ): Set[NodeIndexMatch] = for {
    labelPredicate <- labelPredicates
    labelName <- graphSchemaOptimizations.addImpliedLabels(labelPredicate.hasLabels.labels.toSet)
    labelId: LabelId <- tokenTable.id(labelName).toSet
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
      tokenTable,
      planContext.getNodePropertiesWithTypeConstraint(labelName.name),
      providedOrderFactory
    )
    indexMatch = NodeIndexMatch(
      variable,
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
