/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_2.spi

import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.{Cardinality, Selectivity}
import org.neo4j.cypher.internal.compiler.v2_2.{LabelId, PropertyKeyId, RelTypeId}

import scala.collection.mutable

sealed trait StatisticsKey
case class NodesWithLabelCardinality(labelId: Option[LabelId]) extends StatisticsKey
case class CardinalityByLabelsAndRelationshipType(lhs: Option[LabelId], relType: Option[RelTypeId], rhs: Option[LabelId]) extends StatisticsKey
case class IndexSelectivity(labelId: LabelId, propertyKeyId: PropertyKeyId) extends StatisticsKey

case class MutableGraphStatisticsSnapshot(map: mutable.Map[StatisticsKey, Double] = mutable.Map.empty) {
  def freeze: GraphStatisticsSnapshot = GraphStatisticsSnapshot(map.toMap)
}

case class GraphStatisticsSnapshot(map: Map[StatisticsKey, Double] = Map.empty) {
  def recompute(statistics: GraphStatistics): GraphStatisticsSnapshot = {
    val snapshot = MutableGraphStatisticsSnapshot()
    val instrumented = InstrumentedGraphStatistics(statistics, snapshot)
    map.keys.foreach {
      case NodesWithLabelCardinality(labelId) =>
        instrumented.nodesWithLabelCardinality(labelId)
      case CardinalityByLabelsAndRelationshipType(lhs, relType, rhs) =>
        instrumented.cardinalityByLabelsAndRelationshipType(lhs, relType, rhs)
      case IndexSelectivity(labelId, propertyKeyId) =>
        instrumented.indexSelectivity(labelId, propertyKeyId)
    }
    snapshot.freeze
  }

  //A plan has diverged if there is a relative change in any of the
  //statistics that is bigger than the threshold
  def diverges(snapshot: GraphStatisticsSnapshot, minThreshold: Double): Boolean = {
    assert(map.keySet == snapshot.map.keySet)
    val v1 = map.values
    val v2 = snapshot.map.values
    //find the maximum relative difference (|e1 - e2| / max(e1, e2))
    val relativeDiff = v1.zip(v2).map(e => Math.abs(e._1 - e._2) / Math.max(e._1, e._2)).max
    relativeDiff >= minThreshold
  }
}

case class InstrumentedGraphStatistics(inner: GraphStatistics, snapshot: MutableGraphStatisticsSnapshot) extends GraphStatistics {
  def nodesWithLabelCardinality(labelId: Option[LabelId]): Cardinality =
    snapshot.map.getOrElseUpdate(NodesWithLabelCardinality(labelId), inner.nodesWithLabelCardinality(labelId).amount)

  def cardinalityByLabelsAndRelationshipType(fromLabel: Option[LabelId], relTypeId: Option[RelTypeId], toLabel: Option[LabelId]): Cardinality =
    snapshot.map.getOrElseUpdate(
      CardinalityByLabelsAndRelationshipType(fromLabel, relTypeId, toLabel),
      inner.cardinalityByLabelsAndRelationshipType(fromLabel, relTypeId, toLabel).amount
    )

  def indexSelectivity(label: LabelId, property: PropertyKeyId): Option[Selectivity] = {
    val selectivity = inner.indexSelectivity(label, property)
    snapshot.map.getOrElseUpdate(IndexSelectivity(label, property), selectivity.fold(0.0)(_.factor))
    selectivity
  }
}
