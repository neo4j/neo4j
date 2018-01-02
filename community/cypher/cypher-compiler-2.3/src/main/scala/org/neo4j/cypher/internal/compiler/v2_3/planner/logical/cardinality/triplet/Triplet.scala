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

import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.Multiplier
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.cardinality.TokenSpec
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.plans.{IdName, PatternLength, SimplePatternLength}
import org.neo4j.cypher.internal.compiler.v2_3.spi.GraphStatistics
import org.neo4j.cypher.internal.frontend.v2_3.{LabelId, RelTypeId}

case class Triplet(name: IdName,
                   left: IdName, leftLabels: Set[TokenSpec[LabelId]],
                   right: IdName, rightLabels: Set[TokenSpec[LabelId]],
                   relTypes: Set[TokenSpec[RelTypeId]],
                   directed: Boolean,
                   length: PatternLength = SimplePatternLength) {

  def reverse =
    copy(
      left = right, leftLabels = rightLabels,
      right = left, rightLabels = leftLabels,
      directed = directed,
      length = length
    )
}

sealed trait TripletSide {
  def sourceNode(triplet: Triplet): IdName
  def sourceLabels(triplet: Triplet): Set[TokenSpec[LabelId]]
  def estimateSourceDegreePerLabelIdAndTypeId(stats: GraphStatistics, sourceId: Option[LabelId], typId: Option[RelTypeId]): Multiplier
}

case object LeftSide extends TripletSide {
  def sourceNode(triplet: Triplet) = triplet.left
  def sourceLabels(triplet: Triplet) = triplet.leftLabels

  def estimateSourceDegreePerLabelIdAndTypeId(stats: GraphStatistics, sourceId: Option[LabelId], typId: Option[RelTypeId]): Multiplier = {
    val rels = stats.cardinalityByLabelsAndRelationshipType(sourceId, typId, None)
    val nodes = stats.nodesWithLabelCardinality(sourceId)
    val result = Multiplier(rels.amount / nodes.amount)
    result
  }
}

case object RightSide extends TripletSide {
  def sourceNode(triplet: Triplet) = triplet.right
  def sourceLabels(triplet: Triplet)= triplet.rightLabels

  def estimateSourceDegreePerLabelIdAndTypeId(stats: GraphStatistics, sourceId: Option[LabelId], typId: Option[RelTypeId]): Multiplier = {
    val rels = stats.cardinalityByLabelsAndRelationshipType(None, typId, sourceId)
    val nodes = stats.nodesWithLabelCardinality(sourceId)
    val result = Multiplier(rels.amount / nodes.amount)
    result
  }
}

