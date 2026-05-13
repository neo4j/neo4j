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
package org.neo4j.cypher.internal.compiler.planner.logical.cardinality.assumeIndependence

import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.compiler.planner.logical.Metrics.CardinalityModel
import org.neo4j.cypher.internal.compiler.planner.logical.Metrics.LabelInfo
import org.neo4j.cypher.internal.compiler.planner.logical.Metrics.QueryGraphCardinalityModel
import org.neo4j.cypher.internal.compiler.planner.logical.Metrics.RelTypeInfo
import org.neo4j.cypher.internal.compiler.planner.logical.Metrics.SelectivityCalculator
import org.neo4j.cypher.internal.compiler.planner.logical.cardinality.SelectivityCombiner
import org.neo4j.cypher.internal.compiler.planner.logical.cardinality.assumeIndependence.QueryGraphPredicates.PredicatesWithDisjunctiveLabelInfos
import org.neo4j.cypher.internal.compiler.planner.logical.schema.GraphSchemaOptimizations
import org.neo4j.cypher.internal.compiler.planner.logical.steps.index.IndexCompatiblePredicatesProviderContext
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.logical.plans.TraversalPathMode
import org.neo4j.cypher.internal.planner.spi.PlanContext
import org.neo4j.cypher.internal.util.Cardinality
import org.neo4j.cypher.internal.util.Cardinality.NumericCardinality
import org.neo4j.cypher.internal.util.Multiplier.NumericMultiplier
import org.neo4j.cypher.internal.util.Selectivity
import org.neo4j.cypher.internal.util.SeqSupport.RichSeq
import org.neo4j.cypher.internal.util.helpers.MapSupport.PowerMap

import scala.math.Ordering.Implicits.infixOrderingOps

final class AssumeIndependenceQueryGraphCardinalityModel(
  planContext: PlanContext,
  selectivityCalculator: SelectivityCalculator,
  combiner: SelectivityCombiner,
  labelInferenceStrategy: LabelInferenceStrategy
) extends QueryGraphCardinalityModel with NodeConnectionCardinalityModel {

  override def apply(
    queryGraph: QueryGraph,
    previousLabelInfo: LabelInfo,
    relTypeInfo: RelTypeInfo,
    semanticTable: SemanticTable,
    indexPredicateProviderContext: IndexCompatiblePredicatesProviderContext,
    cardinalityModel: CardinalityModel,
    graphSchemaOptimizations: GraphSchemaOptimizations
  ): Cardinality = {
    // Plan context statistics must be consulted at least one per query, otherwise approximately 60k tests fail, so we cache the total number of nodes here
    val allNodesCardinality = planContext.statistics.nodesAllCardinality()
    val context = QueryGraphCardinalityContext(
      planContext.statistics,
      selectivityCalculator,
      combiner,
      relTypeInfo,
      semanticTable,
      indexPredicateProviderContext,
      cardinalityModel,
      allNodesCardinality,
      labelInferenceStrategy,
      queryGraph.argumentIds,
      graphSchemaOptimizations
    )

    val (searchSelectivity, queryGraphWithIndexLabelPredicate, updatedContext) =
      SearchClauseCardinalityModel.searchClauseSelectivity(queryGraph, context, planContext)

    // First calculate the cardinality of the "top-level" match query graph while keeping track of newly encountered node labels
    val (moreLabelInfo, matchCardinality) =
      getBaseQueryGraphCardinality(updatedContext, previousLabelInfo, queryGraphWithIndexLabelPredicate)
    val optionalMatchesCardinality =
      queryGraphWithIndexLabelPredicate
        .optionalMatches
        .toVector
        // calculate the cardinality of each optional match, accumulating labels and threading them through
        .foldMap(moreLabelInfo)(getBaseQueryGraphCardinality(updatedContext, _, _))
        ._2 // we only care about cardinality, we can ditch the accumulated labels at this point
        .filter(_ >= Cardinality.SINGLE) // we only want to modify the total cardinality if the optional match at hands increases it, we ignore it otherwise
        .product(NumericCardinality)

    matchCardinality * optionalMatchesCardinality * searchSelectivity
  }

  /**
   * Calculates the cardinality of a single "top-level" query graph, ignoring optional matches and mutating patterns, adding newly encountered node labels to the provided LabelInfo
   */
  private def getBaseQueryGraphCardinality(
    context: QueryGraphCardinalityContext,
    previousLabelInfo: LabelInfo,
    queryGraph: QueryGraph
  ): (LabelInfo, Cardinality) = {

    val impliedEndpointLabelsMap =
      context.graphSchemaOptimizations.impliedEndpointLabelsMap(queryGraph.patternRelationships)

    val predicates =
      QueryGraphPredicates.partitionSelections(
        previousLabelInfo,
        context.graphSchemaOptimizations.addImpliedLabels(
          queryGraph.patternNodeLabels.fuse(impliedEndpointLabelsMap)(_ ++ _)
        ),
        queryGraph.selectionsWithInlinedSearchClausePredicates
      )

    val (inferredLabelInfo, newContext) =
      context.labelInferenceStrategy.inferLabels(context, predicates.allLabelInfo, queryGraph.nodeConnections.toSeq)
    val newPredicates = predicates.copy(allLabelInfo = inferredLabelInfo)
    (inferredLabelInfo, getBaseQueryGraphCardinalityWithInferredLabelContext(queryGraph, newPredicates, newContext))
  }

  /**
   * @param context a context in which new the semantic table contains resolved tokens for all
   *                inferred labels.
   */
  private def getBaseQueryGraphCardinalityWithInferredLabelContext(
    queryGraph: QueryGraph,
    predicates: QueryGraphPredicates,
    context: QueryGraphCardinalityContext
  ): Cardinality =
    predicates.distributeLabelDisjunctionAsLabelInfo match {
      case PredicatesWithDisjunctiveLabelInfos(_, Seq(nonDistributedPredicates)) =>
        // If no disjunctions were distributed, we can just calculate the cardinality of predicates == nonDistributedPredicates
        getBaseQueryGraphCardinalityWithInferredLabelContextPerDisjunction(
          queryGraph,
          nonDistributedPredicates,
          context
        )

      case PredicatesWithDisjunctiveLabelInfos(basePredicates, distributedLabelInfos) =>
        // If some disjunctions were distributed into label infos, ...
        getBaseQueryGraphCardinalityWithInferredLabelContextPerDisjunction(queryGraph, basePredicates, context) match {
          // If the base predicates have no cardinality, we can just return it
          case Cardinality.EMPTY =>
            Cardinality.EMPTY
          case baseCardinality =>
            val labelInfoCardinalities = distributedLabelInfos
              // ... we calculate the cardinality for each label info separately, ...
              .map(getBaseQueryGraphCardinalityWithInferredLabelContextPerDisjunction(queryGraph, _, context))

            if (labelInfoCardinalities.contains(Cardinality.INFINITY)) {
              // If any of the disjunctive label already causes an overflow in cardinality, the disjunction will only be greater
              Cardinality.INFINITY
            } else {
              // It might happen that through a label info, we estimate a cardinality higher than the base cardinality.
              // In that case, we should use the sum of the label info cardinalities as the base cardinality as a better approximation
              val baseCardinalityToUse = baseCardinality max labelInfoCardinalities.reduce(_ + _)
              val labelInfosSelectivities =
                labelInfoCardinalities
                  // ... divide it by the cardinality of the base predicates to get the selectivity of each label info ...
                  .map(card => (card / baseCardinalityToUse).getOrElse(Selectivity.ONE))
              // ... and then combine the selectivities of all label infos using the formula for selectivity of disjunctions ...
              val labelInfosSelectivity =
                combiner.orTogetherSelectivities(labelInfosSelectivities).getOrElse(Selectivity.ONE)
              // ... to get the effective selectivity of the predicates that were removed from predicates to basePredicates
              baseCardinalityToUse * labelInfosSelectivity
            }
        }
    }

  private def getBaseQueryGraphCardinalityWithInferredLabelContextPerDisjunction(
    queryGraph: QueryGraph,
    predicates: QueryGraphPredicates,
    context: QueryGraphCardinalityContext
  ) = {
    // Calculate the multiplier for each node connection, accumulating bound nodes and arguments and threading them through
    val (boundNodesAndArguments, nodeConnectionMultipliers) =
      queryGraph
        .nodeConnections
        .toSeq
        .foldMap(BoundNodesAndArguments.withArguments(queryGraph.argumentIds)) {
          (boundNodesAndArguments, nodeConnection) =>
            getNodeConnectionMultiplier(
              context,
              predicates,
              boundNodesAndArguments,
              nodeConnection,
              queryGraph.coveredIdsForPatterns,
              TraversalPathMode.getFromPredicates(queryGraph.selections.predicates.map(_.expr))
            )
        }

    // Number of nodes with no labels at all, different from the number of nodes with any labels (i.e. the total number of nodes)
    lazy val nodeWithNoLabelsCardinality = context.graphStatistics.nodesWithLabelCardinality(None)

    // Calculate the cardinality of the node patterns that are still not bound
    val nodesCardinality =
      queryGraph.patternNodes
        .diff(boundNodesAndArguments.boundNodes)
        .toList
        .map { node =>
          if (boundNodesAndArguments.argumentIds.contains(node)) {
            // In case the node is passed as an argument in the query graph (or indeed the endpoint of a relationship passed as an argument),
            // then we apply the selectivity of the additional labels defined in this query graph but not in the previous ones.
            // For example: MATCH (n:A) OPTIONAL MATCH (n:B) <- we would apply the selectivity of label B here
            Cardinality(getArgumentSelectivity(context, predicates.localOnlyLabelInfo, node).factor)
          } else
            getNodeCardinality(context, predicates.localOnlyLabelInfo, node).getOrElse(nodeWithNoLabelsCardinality)
        }.product(NumericCardinality)

    val otherPredicatesSelectivity =
      context.predicatesSelectivity(predicates.allLabelInfo, predicates.otherPredicates)

    nodesCardinality *
      nodeConnectionMultipliers.product(NumericMultiplier) *
      otherPredicatesSelectivity
  }
}
