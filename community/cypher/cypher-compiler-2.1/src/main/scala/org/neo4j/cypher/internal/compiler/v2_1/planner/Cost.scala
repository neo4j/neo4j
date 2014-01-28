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


case class Cost(cardinality: Int, effort: Int) extends Ordered[Cost] {
  // TODO: Include effort here
  def compare(that: Cost): Int = that match {
    case null => -1
    case _ => cardinality - that.cardinality
  }
}

trait CostCalculator {
  def costForLabelScan(cardinality: Int): Cost
  def costForAllNodes(cardinality: Int): Cost
  def costForExpandRelationship(cardinality: Int): Cost
}

trait CardinalityEstimator {
  def estimateLabelScan(labelId: Token): Int
  def estimateAllNodes(): Int
  def estimateExpandRelationship(labelId: Seq[Token], relationshipType: Seq[Token], dir: Direction): Int
}
