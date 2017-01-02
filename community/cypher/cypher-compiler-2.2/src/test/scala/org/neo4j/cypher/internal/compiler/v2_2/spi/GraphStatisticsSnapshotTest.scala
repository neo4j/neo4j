/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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

import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.{Cardinality, Selectivity}
import org.neo4j.cypher.internal.compiler.v2_2.{LabelId, PropertyKeyId, RelTypeId}

import scala.language.reflectiveCalls

class GraphStatisticsSnapshotTest extends CypherFunSuite {
  def graphStatistics() = new GraphStatistics {
    private var _factor = 1L

    def nodesWithLabelCardinality(labelId: Option[LabelId]): Cardinality =
      Cardinality(labelId.fold(500)(_.id * 10) * _factor)

    def cardinalityByLabelsAndRelationshipType(fromLabel: Option[LabelId], relTypeId: Option[RelTypeId], toLabel: Option[LabelId]): Cardinality =
      Cardinality(relTypeId.fold(5000)(_.id * 10) * _factor)

    def indexSelectivity(label: LabelId, property: PropertyKeyId): Option[Selectivity] =
      Selectivity.of(1.0 / ((property.id + 1) * _factor))

    def factor(factor: Long): Unit = {
     _factor = factor
    }
  }

  test("records queries and its observed values") {
    val snapshot = MutableGraphStatisticsSnapshot()
    val instrumentedStatistics = InstrumentedGraphStatistics(graphStatistics(), snapshot)
    instrumentedStatistics.nodesWithLabelCardinality(None)
    instrumentedStatistics.indexSelectivity(LabelId(0), PropertyKeyId(3))
    instrumentedStatistics.nodesWithLabelCardinality(Some(LabelId(4)))
    instrumentedStatistics.cardinalityByLabelsAndRelationshipType(Some(LabelId(2)), None, None)
    instrumentedStatistics.cardinalityByLabelsAndRelationshipType(None, Some(RelTypeId(1)), Some(LabelId(2)))
    snapshot.freeze.map should equal(Map(
      NodesWithLabelCardinality(None) -> 500,
      IndexSelectivity(LabelId(0), PropertyKeyId(3)) -> 0.25,
      NodesWithLabelCardinality(Some(LabelId(4))) -> 40,
      CardinalityByLabelsAndRelationshipType(Some(LabelId(2)), None, None) -> 5000,
      CardinalityByLabelsAndRelationshipType(None, Some(RelTypeId(1)), Some(LabelId(2))) -> 10
    ))
  }

  test("a snapshot shouldn't diverge from itself") {
    val snapshot = MutableGraphStatisticsSnapshot()
    val instrumentedStatistics = InstrumentedGraphStatistics(graphStatistics(), snapshot)
    instrumentedStatistics.nodesWithLabelCardinality(None)
    instrumentedStatistics.indexSelectivity(LabelId(0), PropertyKeyId(3))
    instrumentedStatistics.nodesWithLabelCardinality(Some(LabelId(4)))
    instrumentedStatistics.cardinalityByLabelsAndRelationshipType(Some(LabelId(2)), None, None)
    instrumentedStatistics.cardinalityByLabelsAndRelationshipType(None, Some(RelTypeId(1)), Some(LabelId(2)))

    val frozenSnapshot = snapshot.freeze
    val smallNumber: Double = 1e-10

    frozenSnapshot.diverges(frozenSnapshot, smallNumber) should equal(false)
  }

  test("a snapshot should pick up divergences") {
    val snapshot1 = MutableGraphStatisticsSnapshot()
    val statistics = graphStatistics()
    val instrumentedStatistics1 = InstrumentedGraphStatistics(statistics, snapshot1)
    instrumentedStatistics1.nodesWithLabelCardinality(None)
    instrumentedStatistics1.indexSelectivity(LabelId(0), PropertyKeyId(3))
    instrumentedStatistics1.nodesWithLabelCardinality(Some(LabelId(4)))

    val snapshot2 = MutableGraphStatisticsSnapshot()
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
    val snapshot1 = MutableGraphStatisticsSnapshot()
    val statistics = graphStatistics()
    val instrumentedStatistics1 = InstrumentedGraphStatistics(statistics, snapshot1)
    instrumentedStatistics1.nodesWithLabelCardinality(None)
    instrumentedStatistics1.indexSelectivity(LabelId(0), PropertyKeyId(3))
    instrumentedStatistics1.nodesWithLabelCardinality(Some(LabelId(4)))

    val snapshot2 = MutableGraphStatisticsSnapshot()
    val instrumentedStatistics2 = InstrumentedGraphStatistics(statistics, snapshot2)
    instrumentedStatistics2.nodesWithLabelCardinality(None)
    instrumentedStatistics2.nodesWithLabelCardinality(Some(LabelId(4)))

    statistics.factor(Long.MaxValue)
    instrumentedStatistics2.indexSelectivity(LabelId(0), PropertyKeyId(3))

    val frozen1 = snapshot1.freeze
    val frozen2 = snapshot2.freeze

    frozen1.diverges(frozen2, 1.0) should equal(false)
  }
}
