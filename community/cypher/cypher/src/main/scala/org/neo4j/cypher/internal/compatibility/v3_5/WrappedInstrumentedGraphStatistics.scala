/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.compatibility.v3_5

import org.neo4j.cypher.internal.planner.v3_5.spi.{GraphStatistics => GraphStatisticsV3_5, IndexDescriptor => IndexDescriptorV3_5, InstrumentedGraphStatistics => InstrumentedGraphStatisticsV3_5, MutableGraphStatisticsSnapshot => MutableGraphStatisticsSnapshotV3_5}
import org.neo4j.cypher.internal.planner.v4_0.spi._
import org.neo4j.cypher.internal.v3_5.util.{Cardinality => CardinalityV3_5, LabelId => LabelIdV3_5, PropertyKeyId => PropertyKeyIdV3_5, RelTypeId => RelTypeIdV3_5, Selectivity => SelectivityV3_5}
import org.neo4j.cypher.internal.v4_0.util.{LabelId, PropertyKeyId, RelTypeId}

/**
  * This class will act as a 3.5 InstrumentedGraphStatistics, but will update instead a map of values
  * for 4.0 so that the query plan cache can still work. It extends the original InstrumentedGraphStatistics
  * so that existing 3.5 code will see it as the same type. However it overrides all behaviour in that class.
  *
  * @param innerV3_5 - the inner 3.5 graph statistics used by the planner to make the plans
  * @param snapshotv4_0 - the 4.0 version of the graph statistics snapshot used to remember what was used by the planner
  */
class WrappedInstrumentedGraphStatistics(innerV3_5: GraphStatisticsV3_5, snapshotv4_0: MutableGraphStatisticsSnapshot) extends InstrumentedGraphStatisticsV3_5(innerV3_5, new MutableGraphStatisticsSnapshotV3_5()) {
  override def nodesWithLabelCardinality(labelId: Option[LabelIdV3_5]): CardinalityV3_5 =
    if (labelId.isEmpty) {
      snapshotv4_0.map.getOrElseUpdate(NodesWithLabelCardinality(None), 1)
    } else {
      snapshotv4_0.map.getOrElseUpdate(NodesWithLabelCardinality(labelId), inner.nodesWithLabelCardinality(labelId).amount)
    }

  override def cardinalityByLabelsAndRelationshipType(fromLabel: Option[LabelIdV3_5], relTypeId: Option[RelTypeIdV3_5], toLabel: Option[LabelIdV3_5]): CardinalityV3_5 =
    snapshotv4_0.map.getOrElseUpdate(
      CardinalityByLabelsAndRelationshipType(fromLabel, relTypeId, toLabel),
      inner.cardinalityByLabelsAndRelationshipType(fromLabel, relTypeId, toLabel).amount
    )

  override def uniqueValueSelectivity(index: IndexDescriptorV3_5): Option[SelectivityV3_5] = {
    val selectivity = inner.uniqueValueSelectivity(index)
    snapshotv4_0.map.getOrElseUpdate(IndexSelectivity(index), selectivity.fold(0.0)(_.factor))
    selectivity
  }

  override def indexPropertyExistsSelectivity(index: IndexDescriptorV3_5): Option[SelectivityV3_5] = {
    val selectivity = inner.indexPropertyExistsSelectivity(index)
    snapshotv4_0.map.getOrElseUpdate(IndexPropertyExistsSelectivity(index), selectivity.fold(0.0)(_.factor))
    selectivity
  }

  override def nodesAllCardinality(): CardinalityV3_5 = snapshotv4_0.map.getOrElseUpdate(NodesAllCardinality, inner.nodesAllCardinality().amount)

  implicit def to3_5l(labelId: LabelIdV3_5): LabelId = LabelId(labelId.id)

  implicit def to3_5lbl(labelId: Option[LabelIdV3_5]): Option[LabelId] = labelId.map(l => LabelId(l.id))

  implicit def to3_5rel(relTypeId: Option[RelTypeIdV3_5]): Option[RelTypeId] = relTypeId.map(l => RelTypeId(l.id))

  implicit def to3_5props(properties: Seq[PropertyKeyIdV3_5]): Seq[PropertyKeyId] = properties.map(p => PropertyKeyId(p.id))

  implicit def to3_5index(index: IndexDescriptorV3_5): IndexDescriptor = IndexDescriptor(index.label, index.properties)
}
