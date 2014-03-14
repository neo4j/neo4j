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
package org.neo4j.cypher.internal.compiler.v2_1.planner

import org.neo4j.graphdb.Direction
import org.neo4j.cypher.internal.compiler.v2_1.{PropertyKeyId, RelTypeId, LabelId}

/*
This class is responsible for answering questions about cardinality. It does this by asking the database when this
information is available, or guessing when that's not possible.
 */
trait CardinalityEstimator {
  def estimateNodeByIdSeek(): Int
  def estimateRelationshipByIdSeek(): Int
  def estimateNodeByLabelScan(labelId: Option[LabelId]): Int
  def estimateAllNodes(): Int
  def estimateNodeIndexSeek(labelId: LabelId, propertyKeyId: PropertyKeyId): Int
  def estimateNodeIndexScan(labelId: LabelId, propertyKeyId: PropertyKeyId): Int
  def estimateExpandRelationship(labelIds: Seq[LabelId], relationshipType: Seq[RelTypeId], dir: Direction): Int
}
