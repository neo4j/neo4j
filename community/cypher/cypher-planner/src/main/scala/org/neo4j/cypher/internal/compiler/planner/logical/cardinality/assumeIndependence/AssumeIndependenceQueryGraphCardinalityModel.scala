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
import org.neo4j.cypher.internal.compiler.helpers.SeqSupport.RichSeq
import org.neo4j.cypher.internal.compiler.planner.logical.Metrics.CardinalityModel
import org.neo4j.cypher.internal.compiler.planner.logical.Metrics.LabelInfo
import org.neo4j.cypher.internal.compiler.planner.logical.Metrics.QueryGraphCardinalityModel
import org.neo4j.cypher.internal.compiler.planner.logical.Metrics.RelTypeInfo
import org.neo4j.cypher.internal.compiler.planner.logical.Metrics.SelectivityCalculator
import org.neo4j.cypher.internal.compiler.planner.logical.cardinality.SelectivityCombiner
import org.neo4j.cypher.internal.compiler.planner.logical.steps.index.IndexCompatiblePredicatesProviderContext
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.planner.spi.PlanContext
import org.neo4j.cypher.internal.util.Cardinality
import org.neo4j.cypher.internal.util.Cardinality.NumericCardinality
import org.neo4j.cypher.internal.util.Multiplier.NumericMultiplier

import scala.util.chaining.scalaUtilChainingOps

final class AssumeIndependenceQueryGraphCardinalityModel(
  planContext: PlanContext,
  selectivityCalculator: SelectivityCalculator,
  combiner: SelectivityCombiner,
  labelInferenceStrategy: LabelInferenceStrategy
) extends QueryGraphCardinalityModel with NodeConnectionCardinalityModel {

  override def apply(
    queryGraph: QueryGraph,
    labelInfo: LabelInfo,
    relTypeInfo: RelTypeInfo,
    semanticTable: SemanticTable,
    indexPredicateProviderContext: IndexCompatiblePredicatesProviderContext,
    cardinalityModel: CardinalityModel
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
      labelInferenceStrategy
    )
    // First calculate the cardinality of the "top-level" match query graph while keeping track of newly encountered node labels
    val (moreLabelInfo, matchCardinality) = getBaseQueryGraphCardinality(context, labelInfo, queryGraph)
    val optionalMatchesCardinality =
      queryGraph
        .optionalMatches
        // calculate the cardinality of each optional match, accumulating labels and threading them through
        .foldMap(moreLabelInfo)(getBaseQueryGraphCardinality(context, _, _))
        ._2 // we only care about cardinality, we can ditch the accumulated labels at this point
        .filter(_ >= Cardinality.SINGLE) // we only want to modify the total cardinality if the optional match at hands increases it, we ignore it otherwise
        .product(NumericCardinality)

    matchCardinality * optionalMatchesCardinality
  }

  /**
   * Calculates the cardinality of a single "top-level" query graph, ignoring optional matches and mutating patterns, adding newly encountered node labels to the provided LabelInfo
   */
  private def getBaseQueryGraphCardinality(
    context: QueryGraphCardinalityContext,
    labelInfo: LabelInfo,
    queryGraph: QueryGraph
  ): (LabelInfo, Cardinality) = {
    val predicates = QueryGraphPredicates.partitionSelections(labelInfo, queryGraph.selections)

    val inferLabels: (QueryGraphCardinalityContext, LabelInfo) => (LabelInfo, QueryGraphCardinalityContext) =
      context.labelInferenceStrategy.inferLabels(_, _, queryGraph.nodeConnections.toSeq)

    inferLabels(context, predicates.allLabelInfo) pipe {
      case (allLabelInfo, context) =>
        (allLabelInfo, inferLabels(context, predicates.localLabelInfo))
    } pipe {
      case (allLabelInfo, (localLabelInfo, context)) =>
        (predicates.copy(allLabelInfo = allLabelInfo, localLabelInfo = localLabelInfo), context)
    } pipe {
      case (predicates, context) =>
        // Note that the new context is not propagated further than this method.
        // This means that any newly resolved label names will not be known
        // to any later query graphs.
        getBaseQueryGraphCardinalityWithInferredLabelContext(queryGraph, predicates, context)
    }
  }

  /**
   * @param context a context in which new the semantic table contains resolved tokens for all
   *                inferred labels.
   */
  private def getBaseQueryGraphCardinalityWithInferredLabelContext(
    queryGraph: QueryGraph,
    predicates: QueryGraphPredicates,
    context: QueryGraphCardinalityContext
  ): (LabelInfo, Cardinality) = {
    // Calculate the multiplier for each node connection, accumulating bound nodes and arguments and threading them through
    val (boundNodesAndArguments, nodeConnectionMultipliers) =
      queryGraph
        .nodeConnections
        .toSeq
        .foldMap(BoundNodesAndArguments.withArguments(queryGraph.argumentIds)) {
          (boundNodesAndArguments, nodeConnection) =>
            getNodeConnectionMultiplier(context, predicates, boundNodesAndArguments, nodeConnection)
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
            Cardinality(getArgumentSelectivity(context, predicates.localLabelInfo, node).factor)
          } else
            getNodeCardinality(context, predicates.allLabelInfo, node).getOrElse(nodeWithNoLabelsCardinality)
        }.product(NumericCardinality)

    val otherPredicatesSelectivity =
      context.predicatesSelectivity(predicates.allLabelInfo, predicates.otherPredicates)

    val cardinality =
      nodesCardinality *
        nodeConnectionMultipliers.product(NumericMultiplier) *
        otherPredicatesSelectivity

    (predicates.allLabelInfo, cardinality)
  }
}
