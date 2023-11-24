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
package org.neo4j.cypher.internal.planner.spi

import org.neo4j.cypher.internal.util.Cardinality
import org.neo4j.cypher.internal.util.LabelId
import org.neo4j.cypher.internal.util.RelTypeId
import org.neo4j.cypher.internal.util.Selectivity

import java.lang.Math.abs
import java.lang.Math.max

import scala.collection.mutable

sealed trait StatisticsKey
case class NodesWithLabelCardinality(labelId: Option[LabelId]) extends StatisticsKey
case object NodesAllCardinality extends StatisticsKey

case class CardinalityByLabelsAndRelationshipType(
  lhs: Option[LabelId],
  relType: Option[RelTypeId],
  rhs: Option[LabelId]
) extends StatisticsKey
case class IndexSelectivity(index: IndexDescriptor) extends StatisticsKey
case class IndexPropertyExistsSelectivity(index: IndexDescriptor) extends StatisticsKey

class MutableGraphStatisticsSnapshot(val map: mutable.Map[StatisticsKey, Double] = mutable.Map.empty) {
  def freeze: GraphStatisticsSnapshot = GraphStatisticsSnapshot(map.toMap)
}

case class DivergenceState(key: StatisticsKey, divergence: Double, before: Double, after: Double)

case class GraphStatisticsSnapshot(statsValues: Map[StatisticsKey, Double] = Map.empty) {

  def recompute(statistics: GraphStatistics): GraphStatisticsSnapshot = {
    val snapshot = new MutableGraphStatisticsSnapshot()
    val instrumented = InstrumentedGraphStatistics(statistics, snapshot)
    statsValues.keys.foreach {
      case NodesWithLabelCardinality(labelId) =>
        instrumented.nodesWithLabelCardinality(labelId)
      case NodesAllCardinality =>
        instrumented.nodesAllCardinality()
      case CardinalityByLabelsAndRelationshipType(lhs, relType, rhs) =>
        instrumented.patternStepCardinality(lhs, relType, rhs)
      case IndexSelectivity(index) =>
        instrumented.uniqueValueSelectivity(index)
      case IndexPropertyExistsSelectivity(index) =>
        instrumented.indexPropertyIsNotNullSelectivity(index)
    }
    snapshot.freeze
  }

  def diverges(snapshot: GraphStatisticsSnapshot): DivergenceState = {
    assert(statsValues.keySet == snapshot.statsValues.keySet)
    // find the maximum relative difference (|e1 - e2| / max(e1, e2))
    val (divergedStats, divergedStatKey, before, after) = (statsValues map {
      case (k, e1) =>
        val e2 = snapshot.statsValues(k)
        val divergence = abs(e1 - e2) / max(e1, e2)
        if (divergence.isNaN) (0.0, k, e1, e2) else (divergence, k, e1, e2)
    }).maxBy(_._1)
    DivergenceState(divergedStatKey, divergedStats, before, after)
  }
}

case class InstrumentedGraphStatistics(inner: GraphStatistics, snapshot: MutableGraphStatisticsSnapshot)
    extends GraphStatistics {

  def nodesWithLabelCardinality(labelId: Option[LabelId]): Cardinality = {
    val cardinality = inner.nodesWithLabelCardinality(labelId)
    snapshot.map.getOrElseUpdate(NodesWithLabelCardinality(labelId), cardinality.amount)
    cardinality
  }

  def patternStepCardinality(
    fromLabel: Option[LabelId],
    relTypeId: Option[RelTypeId],
    toLabel: Option[LabelId]
  ): Cardinality =
    snapshot.map.getOrElseUpdate(
      CardinalityByLabelsAndRelationshipType(fromLabel, relTypeId, toLabel),
      inner.patternStepCardinality(fromLabel, relTypeId, toLabel).amount
    )

  def uniqueValueSelectivity(index: IndexDescriptor): Option[Selectivity] = {
    val selectivity = inner.uniqueValueSelectivity(index)
    snapshot.map.getOrElseUpdate(IndexSelectivity(index), selectivity.fold(0.0)(_.factor))
    selectivity
  }

  def indexPropertyIsNotNullSelectivity(index: IndexDescriptor): Option[Selectivity] = {
    val selectivity = inner.indexPropertyIsNotNullSelectivity(index)
    snapshot.map.getOrElseUpdate(IndexPropertyExistsSelectivity(index), selectivity.fold(0.0)(_.factor))
    selectivity
  }

  override def nodesAllCardinality(): Cardinality =
    snapshot.map.getOrElseUpdate(NodesAllCardinality, inner.nodesAllCardinality().amount)

  /**
   * The return value of this method is not recorded in the snapshot. That's because the value returned by this method
   * is not by it's own used for planning decisions. Instead, the value is used as the set of candidates that is later
   * passed to `patternStepCardinality()` during label inference. As such, relevant statistics will be recorded in the
   * snapshot at a later stage.
   */
  override def mostCommonLabelGivenRelationshipType(typ: Int): Seq[Int] = {
    inner.mostCommonLabelGivenRelationshipType(typ)
  }
}
