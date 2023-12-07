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
import org.neo4j.cypher.internal.expressions.LabelName
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.ir.NodeConnection
import org.neo4j.cypher.internal.ir.PatternRelationship
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.ir.SimplePatternLength
import org.neo4j.cypher.internal.planner.spi.PlanContext
import org.neo4j.cypher.internal.util.Cardinality
import org.neo4j.cypher.internal.util.Cardinality.NumericCardinality
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.LabelId
import org.neo4j.cypher.internal.util.Multiplier.NumericMultiplier
import org.neo4j.cypher.internal.util.RelTypeId

final class AssumeIndependenceQueryGraphCardinalityModel(
  planContext: PlanContext,
  selectivityCalculator: SelectivityCalculator,
  combiner: SelectivityCombiner,
  labelInference: Boolean
) extends QueryGraphCardinalityModel with NodeConnectionCardinalityModel {

  private case class SimpleRelationship(startNode: String, endNode: String, relationshipType: RelTypeId) {

    def nodesWithSameCardinalityWhenAddingLabel(labelId: LabelId): List[String] = {
      val relationshipCardinality = planContext.statistics.patternStepCardinality(None, Some(relationshipType), None)

      def hasSameCardinalityWhenAddingLabel(fromLabel: Option[LabelId], toLabel: Option[LabelId]): Boolean =
        planContext.statistics.patternStepCardinality(fromLabel, Some(relationshipType), toLabel).amount ==
          relationshipCardinality.amount

      List(
        Option.when(hasSameCardinalityWhenAddingLabel(Some(labelId), None))(startNode),
        Option.when(hasSameCardinalityWhenAddingLabel(None, Some(labelId)))(endNode)
      ).flatten
    }

    def inferLabels: Seq[SimpleRelationship.InferredLabel] = {
      for {
        mostCommonLabelId <- planContext.statistics.mostCommonLabelGivenRelationshipType(this.relationshipType.id)
        nodeName <- this.nodesWithSameCardinalityWhenAddingLabel(LabelId(mostCommonLabelId))
        labelName = planContext.getLabelName(mostCommonLabelId)
      } yield SimpleRelationship.InferredLabel(nodeName, labelName, LabelId(mostCommonLabelId))
    }
  }

  private object SimpleRelationship {
    case class InferredLabel(nodeName: String, labelName: String, labelId: LabelId)

    def fromNodeConnection(nodeConnection: NodeConnection, semanticTable: SemanticTable): Option[SimpleRelationship] =
      nodeConnection match {
        case relationship @ PatternRelationship(_, _, dir, Seq(relationshipTypeName), SimplePatternLength)
          if dir == SemanticDirection.OUTGOING || dir == SemanticDirection.INCOMING =>
          val (startNode, endNode) = relationship.inOrder
          semanticTable.id(relationshipTypeName).map(SimpleRelationship(startNode.name, endNode.name, _))
        case _ => None
      }
  }

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
      allNodesCardinality
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
    val initialPredicates = QueryGraphPredicates.partitionSelections(labelInfo, queryGraph.selections)

    val (predicates, newContext) = if (labelInference) {
      val allInferredLabels = for {
        nodeConnection <- queryGraph.nodeConnections.toSeq
        simpleRelationship <- SimpleRelationship.fromNodeConnection(nodeConnection, context.semanticTable).iterator
        inferredLabel <- simpleRelationship.inferLabels
      } yield inferredLabel

      val inferredLabels = allInferredLabels
        .sequentiallyGroupBy(_.nodeName)
        .map { case (key, value) =>
          key -> value.minBy(x => planContext.statistics.nodesWithLabelCardinality(Some(x.labelId)))
        }.toMap

      // Update the semantic table with newly resolved label names.
      // Note that the new context is not propagated further than this method.
      // This means that any newly resolved label names will not be known
      // to any later query graphs.
      val newContext = inferredLabels.values.foldLeft(context) {
        case (context, il) =>
          context.copy(semanticTable = context.semanticTable.addResolvedLabelName(il.labelName, il.labelId))
      }

      def addInferredLabelOnlyIfNoOtherLabel(labelInfo: LabelInfo): LabelInfo = {
        labelInfo.map {
          case (node, labelNames) if labelNames.isEmpty => // only infer if node has no labels
            node -> Set(inferredLabels.get(node.name).map(x => LabelName(x.labelName)(InputPosition.NONE))).flatten
          case (nodeName, labelNames) =>
            nodeName -> labelNames
        }
      }

      val allLabelInfo = addInferredLabelOnlyIfNoOtherLabel(initialPredicates.allLabelInfo)
      val localLabelInfo = addInferredLabelOnlyIfNoOtherLabel(initialPredicates.localLabelInfo)

      (initialPredicates.copy(allLabelInfo = allLabelInfo, localLabelInfo = localLabelInfo), newContext)
    } else {
      (initialPredicates, context)
    }

    getBaseQueryGraphCardinalityWithInferredLabelContext(queryGraph, predicates, newContext)
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
