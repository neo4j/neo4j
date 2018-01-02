/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.cypher.internal.compiler.v2_3.planner.logical.cardinality.triplet

import org.neo4j.cypher.internal.frontend.v2_3.ast.Expression
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.Metrics._
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.cardinality.triplet.TripletQueryGraphCardinalityModel.NodeCardinalities
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.cardinality.{ExpressionSelectivityEstimator, NodeCardinalityEstimator, SelectivityCombiner}
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.plans.IdName
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.{Cardinality, Selectivity}
import org.neo4j.cypher.internal.compiler.v2_3.planner.QueryGraph
import org.neo4j.cypher.internal.compiler.v2_3.spi.GraphStatistics
import org.neo4j.cypher.internal.frontend.v2_3.SemanticTable

object TripletQueryGraphCardinalityModel {
  type SelectivityEstimator = Expression => Selectivity
  type NodeCardinalities = Map[IdName, Cardinality]
}

case class TripletQueryGraphCardinalityModel(stats: GraphStatistics, combiner: SelectivityCombiner)
  extends QueryGraphCardinalityModel {

  def apply(qg: QueryGraph, input: QueryGraphSolverInput, semanticTable: SemanticTable): Cardinality = {
    if (qg.optionalMatches.nonEmpty)
      throw new IllegalArgumentException("OPTIONAL MATCH is unsupported in this cardinality model")

    val allNodes = stats.nodesWithLabelCardinality(None)
    val inputCardinality = if (qg.argumentIds.isEmpty) Cardinality(1.0) else input.inboundCardinality

    val estimateSelectivity = ExpressionSelectivityEstimator(qg.selections, stats, semanticTable, combiner)
    val estimateNodeCardinalities = NodeCardinalityEstimator(estimateSelectivity, allNodes, inputCardinality)
    val (nodeCardinalities, usedPredicates) = estimateNodeCardinalities(qg)

    val convertToTriplet = TripletConverter(qg, input, semanticTable)
    val triplets = qg.patternRelationships.map(convertToTriplet)

    // UNTESTED BELOW

    val estimateTriplets = newTripletCardinalityEstimator(allNodes, nodeCardinalities, stats)
    val tripletCardinalities = triplets.map(estimateTriplets)
    val tripletCardinality = tripletCardinalities.foldLeft(Cardinality.SINGLE)(_ * _)

    val connectedNodes = qg.patternRelationships.flatMap(_.coveredIds)
    val disconnectedNodes = (qg.patternNodes -- qg.argumentIds) -- connectedNodes
    val disconnectedCardinality = disconnectedNodes.map(nodeCardinalities).foldLeft(Cardinality.SINGLE)(_ * _)
    val overlapCardinality = calculateOverlapCardinality(qg, nodeCardinalities, inputCardinality)

    val unselectedCardinality = (inputCardinality * disconnectedCardinality * tripletCardinality) * overlapCardinality.inverse

    val remainingPredicates = qg.selections.flatPredicates.toSet -- usedPredicates
    val remainingSelectivity = estimateSelectivity.and(remainingPredicates)

    val resultCardinality = unselectedCardinality * remainingSelectivity

    resultCardinality
  }

  private def newTripletCardinalityEstimator(allNodes: Cardinality, nodeCardinalities: NodeCardinalities, stats: GraphStatistics) =
    VariableTripletCardinalityEstimator(
      allNodes,
      SimpleTripletCardinalityEstimator(allNodes, nodeCardinalities, stats)
    )
}







