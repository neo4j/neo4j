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
package org.neo4j.cypher.internal.compatibility.v3_3

import org.neo4j.cypher.internal.compiler.v3_3.{IndexDescriptor => IndexDescriptorV3_3}
import org.neo4j.cypher.internal.compiler.v3_3.spi.{GraphStatistics => GraphStatisticsV3_3, InstrumentedGraphStatistics => InstrumentedGraphStatisticsV3_3, MutableGraphStatisticsSnapshot => MutableGraphStatisticsSnapshotV3_3}
import org.neo4j.cypher.internal.frontend.v3_3.{LabelId => LabelIdV3_3, PropertyKeyId => PropertyKeyIdV3_3, RelTypeId => RelTypeIdV3_3}
import org.neo4j.cypher.internal.ir.v3_3.{Cardinality => CardinalityV3_3, Selectivity => SelectivityV3_3}
import org.neo4j.cypher.internal.planner.v3_4.spi._
import org.neo4j.cypher.internal.util.v3_4.{LabelId, PropertyKeyId, RelTypeId}

/**
  * This class will act as a v3.3 InstrumentedGraphStatistics, but will update instead a map of values
  * for 3.4 so that the query plan cache can still work. It extends the original InstrumentedGraphStatistics
  * so that existing 3.3 code will see it as the same type. However it overrides all behaviour in that class.
  *
  * @param innerV3_3 - the inner 3.3 graph statistics used by the planner to make the plans
  * @param snapshotV3_4 - the 3.4 version of the graph statistics snapshot used to remember what was used by the planner
  */
class WrappedInstrumentedGraphStatistics(innerV3_3: GraphStatisticsV3_3, snapshotV3_4: MutableGraphStatisticsSnapshot) extends InstrumentedGraphStatisticsV3_3(innerV3_3, new MutableGraphStatisticsSnapshotV3_3()) {
  override def nodesWithLabelCardinality(labelId: Option[LabelIdV3_3]): CardinalityV3_3 =
    if (labelId.isEmpty) {
      snapshotV3_4.map.getOrElseUpdate(NodesWithLabelCardinality(None), 1)
    } else {
      snapshotV3_4.map.getOrElseUpdate(NodesWithLabelCardinality(labelId), inner.nodesWithLabelCardinality(labelId).amount)
    }

  override def cardinalityByLabelsAndRelationshipType(fromLabel: Option[LabelIdV3_3], relTypeId: Option[RelTypeIdV3_3], toLabel: Option[LabelIdV3_3]): CardinalityV3_3 =
    snapshotV3_4.map.getOrElseUpdate(
      CardinalityByLabelsAndRelationshipType(fromLabel, relTypeId, toLabel),
      inner.cardinalityByLabelsAndRelationshipType(fromLabel, relTypeId, toLabel).amount
    )

  override def indexSelectivity(index: IndexDescriptorV3_3): Option[SelectivityV3_3] = {
    val selectivity = inner.indexSelectivity(index)
    snapshotV3_4.map.getOrElseUpdate(IndexSelectivity(index), selectivity.fold(0.0)(_.factor))
    selectivity
  }

  override def indexPropertyExistsSelectivity(index: IndexDescriptorV3_3): Option[SelectivityV3_3] = {
    val selectivity = inner.indexPropertyExistsSelectivity(index)
    snapshotV3_4.map.getOrElseUpdate(IndexPropertyExistsSelectivity(index), selectivity.fold(0.0)(_.factor))
    selectivity
  }

  override def nodesAllCardinality(): CardinalityV3_3 = snapshotV3_4.map.getOrElseUpdate(NodesAllCardinality, inner.nodesAllCardinality().amount)

  implicit def to3_4l(labelId: LabelIdV3_3): LabelId = LabelId(labelId.id)

  implicit def to3_4lbl(labelId: Option[LabelIdV3_3]): Option[LabelId] = labelId.map(l => LabelId(l.id))

  implicit def to3_4rel(relTypeId: Option[RelTypeIdV3_3]): Option[RelTypeId] = relTypeId.map(l => RelTypeId(l.id))

  implicit def to3_4props(properties: Seq[PropertyKeyIdV3_3]): Seq[PropertyKeyId] = properties.map(p => PropertyKeyId(p.id))

  implicit def to3_4index(index: IndexDescriptorV3_3): IndexDescriptor = IndexDescriptor(index.label, index.properties)
}
