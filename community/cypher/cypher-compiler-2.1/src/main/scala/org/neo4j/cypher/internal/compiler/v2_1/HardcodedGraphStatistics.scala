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
package org.neo4j.cypher.internal.compiler.v2_1

import org.neo4j.cypher.internal.compiler.v2_1.spi.GraphStatistics
import org.neo4j.graphdb.Direction


case object HardcodedGraphStatistics extends HardcodedGraphStatisticsValues

class HardcodedGraphStatisticsValues extends GraphStatistics {
  val NODES_CARDINALITY: Double = 10000.0
  val NODES_WITH_LABEL_CARDINALITY: Double = 2000.0
  val NODES_WITH_LABEL_SELECTIVITY: Double = 0.2
  val RELATIONSHIPS_WITH_TYPE_SELECTIVITY: Double = 0.2
  val DEGREE_BY_RELATIONSHIP_TYPE_AND_DIRECTION: Double = 5.0
  val DEGREE_BY_LABEL_RELATIONSHIP_TYPE_AND_DIRECTION: Double = 5.0

  def degreeByLabelRelationshipTypeAndDirection(labelId: LabelId, relTypeId: RelTypeId,
                                                direction: Direction): Double =
    DEGREE_BY_LABEL_RELATIONSHIP_TYPE_AND_DIRECTION

  def degreeByRelationshipTypeAndDirection(relTypeId: RelTypeId, direction: Direction): Double =
    DEGREE_BY_RELATIONSHIP_TYPE_AND_DIRECTION

  def relationshipsWithTypeSelectivity(relTypeId: RelTypeId): Double =
    RELATIONSHIPS_WITH_TYPE_SELECTIVITY

  def nodesWithLabelSelectivity(labelId: LabelId): Double =
    NODES_WITH_LABEL_SELECTIVITY

  def nodesWithLabelCardinality(labelId: LabelId): Double =
    NODES_WITH_LABEL_CARDINALITY

  def nodesCardinality: Double =
    NODES_CARDINALITY
}
