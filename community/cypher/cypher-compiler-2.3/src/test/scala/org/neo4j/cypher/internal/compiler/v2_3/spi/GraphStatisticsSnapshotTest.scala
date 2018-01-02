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
package org.neo4j.cypher.internal.compiler.v2_3.spi

import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.{Cardinality, Selectivity}
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.frontend.v2_3.{LabelId, PropertyKeyId, RelTypeId}

import scala.language.reflectiveCalls

class GraphStatisticsSnapshotTest extends CypherFunSuite {
  test("records queries and its observed values") {
    val snapshot = new MutableGraphStatisticsSnapshot()

    val allNodes = 500
    val indexSelectivity = 0.25
    val nodesWithLabel = 40
    val relationships = 5000

    val statistics = graphStatistics(allNodes = allNodes, idxSelectivity = indexSelectivity,
      relCardinality = relationships, labeledNodes = nodesWithLabel)
    val instrumentedStatistics = InstrumentedGraphStatistics(statistics, snapshot)
    instrumentedStatistics.nodesWithLabelCardinality(None)
    instrumentedStatistics.indexSelectivity(LabelId(0), PropertyKeyId(3))
    instrumentedStatistics.nodesWithLabelCardinality(Some(LabelId(4)))
    instrumentedStatistics.cardinalityByLabelsAndRelationshipType(Some(LabelId(2)), None, None)

    snapshot.freeze.statsValues should equal(Map(
      NodesWithLabelCardinality(None) -> allNodes,
      IndexSelectivity(LabelId(0), PropertyKeyId(3)) -> indexSelectivity,
      NodesWithLabelCardinality(Some(LabelId(4))) -> nodesWithLabel,
      CardinalityByLabelsAndRelationshipType(Some(LabelId(2)), None, None) -> relationships
    ))
  }

  test("a snapshot shouldn't diverge from equal values") {
    val snapshot = new MutableGraphStatisticsSnapshot()
    val instrumentedStatistics = InstrumentedGraphStatistics(graphStatistics(), snapshot)
    instrumentedStatistics.nodesWithLabelCardinality(None)
    instrumentedStatistics.indexSelectivity(LabelId(0), PropertyKeyId(3))
    instrumentedStatistics.nodesWithLabelCardinality(Some(LabelId(4)))
    instrumentedStatistics.cardinalityByLabelsAndRelationshipType(Some(LabelId(2)), None, None)
    instrumentedStatistics.cardinalityByLabelsAndRelationshipType(None, Some(RelTypeId(1)), Some(LabelId(2)))

    val snapshot2 = new MutableGraphStatisticsSnapshot()
    val statistics = graphStatistics()

    val instrumentedStatistics2 = InstrumentedGraphStatistics(statistics, snapshot2)
    instrumentedStatistics2.cardinalityByLabelsAndRelationshipType(None, Some(RelTypeId(1)), Some(LabelId(2)))
    instrumentedStatistics2.cardinalityByLabelsAndRelationshipType(Some(LabelId(2)), None, None)
    instrumentedStatistics2.indexSelectivity(LabelId(0), PropertyKeyId(3))
    instrumentedStatistics2.nodesWithLabelCardinality(None)
    instrumentedStatistics2.nodesWithLabelCardinality(Some(LabelId(4)))

    val frozen1 = snapshot.freeze
    val reallySensitiveDivergence: Double = 0.9

    frozen1.diverges(snapshot2.freeze, reallySensitiveDivergence) should equal(false)
  }

  test("a snapshot shouldn't diverge from small differences") {
    val snapshot = new MutableGraphStatisticsSnapshot()
    val instrumentedStatistics = InstrumentedGraphStatistics(graphStatistics(allNodes = 1000), snapshot)
    instrumentedStatistics.nodesWithLabelCardinality(None)

    val snapshot2 = new MutableGraphStatisticsSnapshot()
    val instrumentedStatistics2 = InstrumentedGraphStatistics(graphStatistics(allNodes = 1010), snapshot2)
    instrumentedStatistics2.nodesWithLabelCardinality(None)

    val frozen1 = snapshot.freeze
    frozen1.diverges(snapshot2.freeze, minThreshold = 0.01) should equal(false)
    frozen1.diverges(snapshot2.freeze, minThreshold = 0.001) should equal(true)
  }

  test("a snapshot should pick up divergences") {
    val snapshot1 = new MutableGraphStatisticsSnapshot()
    val statistics = graphStatistics()
    val instrumentedStatistics1 = InstrumentedGraphStatistics(statistics, snapshot1)
    instrumentedStatistics1.nodesWithLabelCardinality(None)
    instrumentedStatistics1.indexSelectivity(LabelId(0), PropertyKeyId(3))
    instrumentedStatistics1.nodesWithLabelCardinality(Some(LabelId(4)))

    val snapshot2 = new MutableGraphStatisticsSnapshot()
    val instrumentedStatistics2 = InstrumentedGraphStatistics(statistics, snapshot2)
    instrumentedStatistics2.nodesWithLabelCardinality(None)
    instrumentedStatistics2.nodesWithLabelCardinality(Some(LabelId(4)))

    statistics.factor(2)
    instrumentedStatistics2.indexSelectivity(LabelId(0), PropertyKeyId(3))

    val frozen1 = snapshot1.freeze
    val frozen2 = snapshot2.freeze
    val smallNumber = 0.1
    val bigNumber = 0.6

    frozen1.diverges(frozen2, smallNumber) should equal(true)
    frozen1.diverges(frozen2, bigNumber) should equal(false)
  }

  test("if threshold is 1.0 nothing diverges") {
    val snapshot1 = new MutableGraphStatisticsSnapshot()
    val statistics = graphStatistics()
    val instrumentedStatistics1 = InstrumentedGraphStatistics(statistics, snapshot1)
    instrumentedStatistics1.nodesWithLabelCardinality(None)
    instrumentedStatistics1.indexSelectivity(LabelId(0), PropertyKeyId(3))
    instrumentedStatistics1.nodesWithLabelCardinality(Some(LabelId(4)))

    val snapshot2 = new MutableGraphStatisticsSnapshot()
    val instrumentedStatistics2 = InstrumentedGraphStatistics(statistics, snapshot2)
    instrumentedStatistics2.nodesWithLabelCardinality(None)
    instrumentedStatistics2.nodesWithLabelCardinality(Some(LabelId(4)))

    statistics.factor(Long.MaxValue)
    instrumentedStatistics2.indexSelectivity(LabelId(0), PropertyKeyId(3))

    val frozen1 = snapshot1.freeze
    val frozen2 = snapshot2.freeze

    frozen1.diverges(frozen2, 1.0) should equal(false)
  }

  private def graphStatistics(allNodes: Long = 500,
                              labeledNodes: Long = 500,
                              relCardinality: Long = 5000,
                              idxSelectivity: Double = 1,
                              idxPropertyExistsSelectivity: Double = 1) = new GraphStatistics {
    private var _factor: Double = 1L

    def nodesWithLabelCardinality(labelId: Option[LabelId]): Cardinality = labelId match {
      case None => Cardinality(allNodes * _factor)
      case Some(id) => Cardinality(labeledNodes * _factor)
    }

    def cardinalityByLabelsAndRelationshipType(fromLabel: Option[LabelId], relTypeId: Option[RelTypeId], toLabel: Option[LabelId]): Cardinality =
      Cardinality(relCardinality * _factor)

    def indexSelectivity(label: LabelId, property: PropertyKeyId): Option[Selectivity] = {
      Selectivity.of(idxSelectivity / _factor)
    }

    def indexPropertyExistsSelectivity(label: LabelId, property: PropertyKeyId): Option[Selectivity] =
      Selectivity.of(idxPropertyExistsSelectivity / _factor)

    def factor(factor: Double): Unit = {
      _factor = factor
    }
  }
}
