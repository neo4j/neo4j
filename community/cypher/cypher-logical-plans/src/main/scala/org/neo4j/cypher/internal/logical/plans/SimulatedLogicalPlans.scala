/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.cypher.internal.logical.plans

import org.neo4j.cypher.internal.util.attribution.IdGen

/**
 * Marker trait of light-weight simulations of a basic plans that can be used to test or benchmark runtime frameworks
 * in isolation from the database.
 */
sealed trait SimulatedPlan

case class SimulatedNodeScan(idName: String, numberOfRows: Long)(implicit idGen: IdGen)
    extends NodeLogicalLeafPlan(idGen) with StableLeafPlan with SimulatedPlan {

  override val availableSymbols: Set[String] = Set(idName)

  override def usedVariables: Set[String] = Set.empty

  override def argumentIds: Set[String] = Set.empty

  override def withoutArgumentIds(argsToExclude: Set[String]): SimulatedNodeScan = this
}

/**
 * Expand incoming rows by the given factor
 */

case class SimulatedExpand(
  override val source: LogicalPlan,
  fromNode: String,
  relName: String,
  toNode: String,
  factor: Double
)(implicit idGen: IdGen)
    extends LogicalUnaryPlan(idGen) with SimulatedPlan {
  assert(factor >= 0.0d, "Factor must be greater or equal to 0")

  override def withLhs(newLHS: LogicalPlan)(idGen: IdGen): LogicalUnaryPlan = copy(source = newLHS)(idGen)

  override val availableSymbols: Set[String] = source.availableSymbols + relName + toNode
}

/**
 * Filter incoming rows by the given selectivity.
 */
case class SimulatedSelection(override val source: LogicalPlan, selectivity: Double)(implicit idGen: IdGen)
    extends LogicalUnaryPlan(idGen) with SimulatedPlan {
  assert(selectivity >= 0.0d && selectivity <= 1.0d, "Selectivity must be a fraction between 0 and 1")

  override def withLhs(newLHS: LogicalPlan)(idGen: IdGen): LogicalUnaryPlan = copy(source = newLHS)(idGen)

  val availableSymbols: Set[String] = source.availableSymbols
}
