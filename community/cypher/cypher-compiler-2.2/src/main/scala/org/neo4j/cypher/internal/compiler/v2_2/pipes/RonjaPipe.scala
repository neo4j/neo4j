/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_2.pipes

import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.Cardinality

// Marks a pipe being used by Ronja
trait RonjaPipe {
  self: Pipe =>

  def estimation: Estimation
  def withEstimation(estimation: Estimation): Pipe with RonjaPipe
}

case class Estimation(operatorCardinality: Option[Double], producedRows: Option[Double])

object Estimation {
  val empty = Estimation(None, None)

  def apply(operatorCardinality: Cardinality, producedRows: Cardinality): Estimation =
    new Estimation(Some(operatorCardinality.amount), Some(producedRows.amount))
}
