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
package org.neo4j.cypher.internal.compiler.v3_4.planner

import org.neo4j.cypher.internal.planner.v3_4.spi.{GraphStatistics, IndexDescriptor}
import org.neo4j.cypher.internal.util.v3_4.{Cardinality, LabelId, RelTypeId, Selectivity}

case object HardcodedGraphStatistics extends GraphStatistics {
  val NODES_CARDINALITY = Cardinality(10000)
  val NODES_WITH_LABEL_SELECTIVITY = Selectivity.of(0.2).get
  val NODES_WITH_LABEL_CARDINALITY = NODES_CARDINALITY * NODES_WITH_LABEL_SELECTIVITY
  val RELATIONSHIPS_CARDINALITY = Cardinality(50000)
  val INDEX_SELECTIVITY = Selectivity.of(.02).get
  val INDEX_PROPERTY_EXISTS_SELECTIVITY = Selectivity.of(.5).get

  def indexSelectivity(index: IndexDescriptor): Option[Selectivity] =
    Some(INDEX_SELECTIVITY * Selectivity.of(index.properties.length).get)

  def indexPropertyExistsSelectivity(index: IndexDescriptor): Option[Selectivity] =
    Some(INDEX_PROPERTY_EXISTS_SELECTIVITY * Selectivity.of(index.properties.length).get)

  def nodesWithLabelCardinality(labelId: Option[LabelId]): Cardinality =
    labelId.map(_ => NODES_WITH_LABEL_CARDINALITY).getOrElse(Cardinality.SINGLE)

  def cardinalityByLabelsAndRelationshipType(fromLabel: Option[LabelId], relTypeId: Option[RelTypeId], toLabel: Option[LabelId]): Cardinality =
    RELATIONSHIPS_CARDINALITY

  override def nodesAllCardinality(): Cardinality = NODES_CARDINALITY
}
