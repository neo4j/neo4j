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
import org.neo4j.cypher.internal.compiler.v2_1.ast.{HasLabels, Expression}

class GuessingEstimator extends CardinalityEstimator {
  private val ALL_NODES_SCAN_CARDINALITY: Int = 1000
  private val LABEL_NOT_FOUND_CARDINALITY: Int = 0
  private val ID_SEEK_CARDINALITY: Int = 1

  private val LABEL_SELECTIVITY: Double = 0.1
  private val PREDICATE_SELECTIVITY: Double = 0.2
  private val INDEX_SEEK_SELECTIVITY: Double = 0.08
  private val UNIQUE_INDEX_SEEK_SELECTIVITY: Double = 0.05
  private val EXPAND_RELATIONSHIP_SELECTIVITY: Double = 0.02

  def estimateExpandRelationship(labelIds: Seq[LabelId], relationshipType: Seq[RelTypeId], dir: Direction) =
    (ALL_NODES_SCAN_CARDINALITY * EXPAND_RELATIONSHIP_SELECTIVITY).toInt

  def estimateNodeByIdSeek() = ID_SEEK_CARDINALITY

  def estimateRelationshipByIdSeek() = ID_SEEK_CARDINALITY

  def estimateNodeByLabelScan(labelId: Option[LabelId]) = labelId match {
    case Some(_) => (ALL_NODES_SCAN_CARDINALITY * LABEL_SELECTIVITY).toInt
    case None => LABEL_NOT_FOUND_CARDINALITY
  }

  def estimateNodeIndexSeek(labelId: LabelId, propertyKeyId: PropertyKeyId) =
    (ALL_NODES_SCAN_CARDINALITY * INDEX_SEEK_SELECTIVITY).toInt

  def estimateNodeUniqueIndexSeek(labelId: LabelId, propertyKeyId: PropertyKeyId) =
    (ALL_NODES_SCAN_CARDINALITY * UNIQUE_INDEX_SEEK_SELECTIVITY).toInt

  def estimateAllNodesScan() = ALL_NODES_SCAN_CARDINALITY

  def estimateSelectivity(exp: Expression): Double = exp match {
    case _: HasLabels => LABEL_SELECTIVITY
    case _ => PREDICATE_SELECTIVITY
  }
}
