/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_2.planner.logical.cardinality.triplet

import org.neo4j.cypher.internal.compiler.v2_2.RelTypeId
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.cardinality.TokenSpec
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.cardinality.triplet.TripletQueryGraphCardinalityModel.NodeCardinalities
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.{Cardinality, Multiplier}
import org.neo4j.cypher.internal.compiler.v2_2.spi.GraphStatistics

case class TripletCardinalityEstimator(nodeCardinalities: NodeCardinalities, stats: GraphStatistics)
  extends (Triplet => Cardinality) {

  def apply(triplet: Triplet): Cardinality = {
    // Compute sum over estimates per type as they are disjoint
    val cardinalitiesPerType = triplet.relTypes.map(estimateTripletCardinalityPerType(triplet))
    val result = cardinalitiesPerType.reduce(_ + _) /* safe to reduce as relTypes must never be empty */
    result
  }

  private def estimateTripletCardinalityPerType(triplet: Triplet)(relType: TokenSpec[RelTypeId]): Cardinality = {
    val result =
      if (triplet.directed) {
        // outgoing only
        estimateDirectedTripletCardinalityPerType(triplet, relType)
      } else {
        // Compute sum of outgoing and incoming as they are disjoint
        val outgoing = estimateDirectedTripletCardinalityPerType(triplet, relType)
        val incoming = estimateDirectedTripletCardinalityPerType(triplet.reverse, relType)
        incoming + outgoing
      }
    result
  }

  private def estimateDirectedTripletCardinalityPerType(triplet: Triplet, relType: TokenSpec[RelTypeId]): Cardinality = {
    // lowest estimate coming from the left
    val lhsCardinality = estimateDirectedTripletCardinalityPerSideAndType(triplet, LeftSide, relType)

    // lowest estimate coming from the right
    val rhsCardinality = estimateDirectedTripletCardinalityPerSideAndType(triplet, RightSide, relType)

    // best wins
    val cardinality = Cardinality.min(lhsCardinality, rhsCardinality)
    cardinality
  }

  private def estimateDirectedTripletCardinalityPerSideAndType(triplet: Triplet, side: TripletSide, relType: TokenSpec[RelTypeId]): Cardinality = {
    val optEstimate = relType.map {
      relTypeId =>
        // pick smallest degree estimate
        val degrees = estimateDirectedTripletDegreesPerSideAndTypeId(triplet, side, relTypeId)
        val degree = degrees.reduce(Multiplier.min) /* safe to reduce as degrees must never be empty */

        // node cardinality for chosen side
        val nodeCardinality = nodeCardinalities(side.sourceNode(triplet))

        // multiply
        val result = nodeCardinality * degree
        result

    }
    optEstimate.getOrElse(Cardinality.EMPTY)
  }

  private def estimateDirectedTripletDegreesPerSideAndTypeId(triplet: Triplet, side: TripletSide, relTypeId: Option[RelTypeId]): Set[Multiplier] = {
    val estimates =
      side.sourceLabels(triplet).map { label =>
        val optEstimate = label.map { labelId => side.estimateSourceDegreePerLabelIdAndTypeId(stats, labelId, relTypeId)}
        optEstimate.getOrElse(Multiplier.ZERO)
      }
    estimates
  }
}
