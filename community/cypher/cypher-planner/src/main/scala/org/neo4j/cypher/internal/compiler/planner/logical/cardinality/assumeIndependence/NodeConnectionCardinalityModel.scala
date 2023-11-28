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

import org.neo4j.cypher.internal.compiler.planner.logical.idp.extractQPPPredicates
import org.neo4j.cypher.internal.expressions.UnPositionedVariable
import org.neo4j.cypher.internal.ir.NodeConnection
import org.neo4j.cypher.internal.ir.PatternRelationship
import org.neo4j.cypher.internal.ir.QuantifiedPathPattern
import org.neo4j.cypher.internal.ir.SelectivePathPattern
import org.neo4j.cypher.internal.util.Cardinality
import org.neo4j.cypher.internal.util.Multiplier

trait NodeConnectionCardinalityModel
    extends NodeCardinalityModel
    with QuantifiedPathPatternCardinalityModel
    with SelectivePathPatternCardinalityModel {

  def getNodeConnectionMultiplier(
    context: QueryGraphCardinalityContext,
    predicates: QueryGraphPredicates,
    boundNodesAndArguments: BoundNodesAndArguments,
    nodeConnection: NodeConnection
  ): (BoundNodesAndArguments, Multiplier) =
    nodeConnection match {
      case relationship: PatternRelationship =>
        if (boundNodesAndArguments.argumentIds.contains(relationship.variable.name)) {
          val additionalLabelsSelectivity =
            // arguably we should check whether the two nodes are the same instead of applying the selectivity twice
            getArgumentSelectivity(context, predicates.localLabelInfo, relationship.left.name) *
              getArgumentSelectivity(context, predicates.localLabelInfo, relationship.right.name)
          val newBoundNodesAndArguments =
            boundNodesAndArguments.addArgumentIdsMarkedAsBound(Set(relationship.left.name, relationship.right.name))
          val multiplier = Multiplier(additionalLabelsSelectivity.factor)
          (newBoundNodesAndArguments, multiplier)
        } else {
          val cardinality =
            getRelationshipCardinality(
              context,
              predicates.allLabelInfo,
              relationship,
              predicates.uniqueRelationships.contains(relationship.variable.name)
            )
          boundNodesAndArguments.bindEndpoints(context, predicates, relationship, cardinality)
        }

      case quantifiedPathPattern: QuantifiedPathPattern =>
        val qppWithExtractedPredicates = {
          val extractedPredicates =
            extractQPPPredicates(
              predicates.otherPredicates.map(_.expr).toSeq,
              quantifiedPathPattern.variableGroupings,
              (boundNodesAndArguments.boundNodes ++ boundNodesAndArguments.argumentIds).map(UnPositionedVariable.varFor)
            )

          quantifiedPathPattern.copy(selections =
            quantifiedPathPattern.selections ++ extractedPredicates.predicates.map(_.extracted)
          )
        }

        val cardinality =
          getQuantifiedPathPatternCardinality(
            context,
            predicates.allLabelInfo,
            qppWithExtractedPredicates,
            predicates.uniqueRelationships
          )
        boundNodesAndArguments.bindEndpoints(context, predicates, quantifiedPathPattern, cardinality)

      case selectivePathPattern: SelectivePathPattern =>
        val cardinality = getSelectivePathPatternCardinality(context, predicates.allLabelInfo, selectivePathPattern)
        // At the time of writing, if a match clause contains a selective path pattern, it can't contain anything else,
        // so we don't need to worry about marking the inner nodes as bound.
        boundNodesAndArguments.bindEndpoints(context, predicates, selectivePathPattern, cardinality)
    }
}

case class BoundNodesAndArguments(boundNodes: Set[String], argumentIds: Set[String]) extends NodeCardinalityModel {

  /**
   * Adds new arguments ids both to the list of arguments ids and to the list of bound nodes
   */
  def addArgumentIdsMarkedAsBound(newArgumentIds: Set[String]): BoundNodesAndArguments =
    copy(boundNodes = boundNodes.union(newArgumentIds), argumentIds = boundNodes.union(newArgumentIds))

  /**
   * Adds the endpoints to the list of bound nodes and divides the node connection's cardinality by the cardinality of its endpoints that were are already bound.
   */
  def bindEndpoints(
    context: QueryGraphCardinalityContext,
    predicates: QueryGraphPredicates,
    nodeConnection: NodeConnection,
    nodeConnectionCardinality: Cardinality
  ): (BoundNodesAndArguments, Multiplier) = {
    // First calculate the cardinality of the left node
    val leftNodeCardinality = getBoundEndpointCardinality(context, predicates, nodeConnection.left.name)
    // Then calculate the cardinality of the right node, having marked the left node as bound, in cases both endpoints are the same node
    val rightNodeCardinality =
      copy(boundNodes = boundNodes.incl(nodeConnection.left.name))
        .getBoundEndpointCardinality(context, predicates, nodeConnection.right.name)
    val multiplier =
      Multiplier.ofDivision(
        dividend = nodeConnectionCardinality,
        divisor = leftNodeCardinality * rightNodeCardinality
      ).getOrElse(Multiplier.ZERO)
    (copy(boundNodes = boundNodes.union(Set(nodeConnection.left.name, nodeConnection.right.name))), multiplier)
  }

  private def getBoundEndpointCardinality(
    context: QueryGraphCardinalityContext,
    predicates: QueryGraphPredicates,
    node: String
  ): Cardinality =
    if (boundNodes.contains(node))
      getNodeCardinality(context, predicates.allLabelInfo, node).getOrElse(Cardinality.EMPTY)
    else if (argumentIds.contains(node))
      // if the endpoint is not bound but was passed as an argument, we calculate its cardinality solely based on external labels
      getNodeCardinality(context, predicates.externalLabelInfo, node).getOrElse(Cardinality.EMPTY)
    else
      Cardinality.SINGLE
}

object BoundNodesAndArguments {

  def withArguments(argumentIds: Set[String]): BoundNodesAndArguments =
    BoundNodesAndArguments(boundNodes = Set.empty, argumentIds = argumentIds)
}
