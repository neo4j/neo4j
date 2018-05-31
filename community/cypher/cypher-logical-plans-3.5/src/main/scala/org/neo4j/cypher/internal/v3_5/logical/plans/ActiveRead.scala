/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.cypher.internal.v3_5.logical.plans

import org.opencypher.v9_0.util.attribution.IdGen

/**
  * Change the reads of all source plans to target the active tx-state instead of the stable. This is used for MERGE
  * to make sure that each merge row can observe the writes of previous rows.
  */
case class ActiveRead(source: LogicalPlan)(implicit idGen: IdGen) extends LogicalPlan(idGen) with LazyLogicalPlan {

  val lhs = Some(source)
  val rhs = None

  override val availableSymbols: Set[String] = source.availableSymbols
}
