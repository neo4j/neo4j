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

import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.cardinality.{Unspecified, TokenSpec}
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.cardinality.triplet.TripletQueryGraphCardinalityModel.NodeCardinalities
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.plans.{IdName, VarPatternLength, SimplePatternLength}
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.{Cardinality, Multiplier}
import org.neo4j.cypher.internal.compiler.v2_3.spi.GraphStatistics
import org.neo4j.cypher.internal.frontend.v2_3.RelTypeId

import scala.annotation.tailrec

case class VariableTripletCardinalityEstimator(allNodes: Cardinality, inner: Triplet => Cardinality) extends (Triplet => Cardinality) {
  def apply(triplet: Triplet): Cardinality = triplet.length match {
    case SimplePatternLength =>
      inner(triplet)

    case length @ VarPatternLength(min, optMax) =>
      val max = length.implicitPatternNodeCount

      val tripletPaths: Seq[Seq[Triplet]] =
        for (steps <- min.to(max))
        yield {
          createTripletPath(triplet, steps)
        }.toSeq

      val pathEstimates = tripletPaths.map(estimateTripletPath)
      val finalEstimate = pathEstimates.reduce(_ + _)
      finalEstimate
  }

  private def estimateTripletPath(path: Seq[Triplet]): Cardinality = {
    val stepCardinalities = path.map(inner)
    val pathCardinality = stepCardinalities.reduce(_ * _)
    val pathLength = path.size
    val overlapCardinality =
      if (pathLength == 0) // empty
        Cardinality.SINGLE
      else
        // adjust for intermediary nodes
        allNodes ^ (pathLength - 1)

    val result = pathCardinality * overlapCardinality.inverse
    result
  }

  private def createTripletPath(triplet: Triplet, steps: Int): Seq[Triplet] =
    if (steps == 0)
      Seq.empty
    else if (steps == 1)
      Seq(triplet.copy(length = SimplePatternLength))
    else
      createTripletPath(triplet, 1, steps)

  @tailrec
  private def createTripletPath(triplet: Triplet, cur: Int, max: Int, result: Seq[Triplet] = Seq.empty): Seq[Triplet] =
    if (cur > max)
      result
    else {
      val nextTriplet =
        if (cur == 1) {
          triplet.copy(
            name = rel(cur),
            right = node(cur+1), rightLabels = Set(Unspecified()),
            length = SimplePatternLength
          )
        } else if (cur == max) {
          triplet.copy(
            name = rel(cur),
            left = node(cur), leftLabels = Set(Unspecified()),
            length = SimplePatternLength
          )
        } else {
          triplet.copy(
            name = rel(cur),
            left = node(cur), leftLabels = Set(Unspecified()),
            right = node(cur+1), rightLabels = Set(Unspecified()),
            length = SimplePatternLength
          )
        }

      createTripletPath(triplet, cur + 1, max, result :+ nextTriplet)
    }

  private def rel(cur: Int) = IdName(s"  VAR_REL$cur")
  private def node(cur: Int) = IdName(s"  VAR_NODE$cur")
}

case class SimpleTripletCardinalityEstimator(allNodes: Cardinality, nodeCardinalities: NodeCardinalities, stats: GraphStatistics)
  extends (Triplet => Cardinality) {

  def apply(triplet: Triplet): Cardinality = {
    if (!triplet.length.isSimple)
      throw new IllegalArgumentException("Non-simple patterns are not supported by this estimator")

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
        val nodeCardinality = nodeCardinalities.getOrElse(side.sourceNode(triplet), allNodes)

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
