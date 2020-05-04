/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
 * Variant of NodeHashJoin. Also builds a hash table using 'left' and produces merged left and right rows using this
 * table. In addition, also produces left rows with missing key values, and right rows that were not matched
 * in the hash table. In these additional rows, variables from the opposing stream are set to NO_VALUE.
 *
 * This is equivalent to a right outer join in relational algebra.
 */
case class RightOuterHashJoin(nodes: Set[String],
                             left: LogicalPlan,
                             right: LogicalPlan)
                            (implicit idGen: IdGen) extends LogicalPlan(idGen) with EagerLogicalPlan {

  val lhs = Some(left)
  val rhs = Some(right)

  val availableSymbols: Set[String] = left.availableSymbols ++ right.availableSymbols

}
