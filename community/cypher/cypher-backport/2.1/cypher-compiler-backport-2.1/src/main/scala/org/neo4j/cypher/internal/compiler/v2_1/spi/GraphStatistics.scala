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
package org.neo4j.cypher.internal.compiler.v2_1.spi

import org.neo4j.cypher.internal.compiler.v2_1.{RelTypeId, LabelId}
import org.neo4j.graphdb.Direction
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.{Multiplier, Cardinality}

trait GraphStatistics {
  def nodesCardinality: Cardinality
  def nodesWithLabelCardinality(labelId: LabelId): Cardinality
  def nodesWithLabelSelectivity(labelId: LabelId): Multiplier
  def relationshipsWithTypeSelectivity(relTypeId: RelTypeId): Multiplier
  def degreeByRelationshipTypeAndDirection(relTypeId: RelTypeId, direction: Direction): Multiplier
  def degreeByLabelRelationshipTypeAndDirection(labelId: LabelId, relTypeId: RelTypeId, direction: Direction): Multiplier
}
