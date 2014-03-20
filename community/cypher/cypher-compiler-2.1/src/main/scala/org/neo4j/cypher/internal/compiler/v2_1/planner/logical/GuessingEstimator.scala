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
package org.neo4j.cypher.internal.compiler.v2_1.planner.logical

import org.neo4j.cypher.internal.compiler.v2_1.{PropertyKeyId, RelTypeId, LabelId}
import org.neo4j.graphdb.Direction
import org.neo4j.cypher.internal.compiler.v2_1.ast.Expression

class GuessingEstimator extends CardinalityEstimator {

  def estimateExpandRelationship(labelIds: Seq[LabelId], relationshipType: Seq[RelTypeId], dir: Direction) = 20

  def estimateNodeByIdSeek() = 1

  def estimateRelationshipByIdSeek() = 2

  def estimateNodeByLabelScan(labelId: Option[LabelId]) = labelId match {
    case Some(id) => 100
    case None => 0
  }

  def estimateNodeIndexSeek(labelId: LabelId, propertyKeyId: PropertyKeyId) = 80

  def estimateNodeUniqueIndexSeek(labelId: LabelId, propertyKeyId: PropertyKeyId) = 50

  def estimateAllNodesScan() = 1000

  def estimateSelectivity(exp: Expression): Double = .2
}
