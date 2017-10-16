/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.v3_4.logical.plans

import org.neo4j.cypher.internal.ir.v3_4.{CardinalityEstimation, IdName, PlannerQuery}

/**
  * Variant of NodeHashJoin. Also builds a hash table using 'left' and produces merged left and right rows using this
  * table. In addition, also produces left and right rows with missing key values, and right rows that do not match
  * in the hash table. In these additional rows, variables from the opposing stream are set to NO_VALUE.
  *
  * This is equivalent to an outer join in relational algebra.
  */
case class OuterHashJoin(nodes: Set[IdName], left: LogicalPlan, right: LogicalPlan)
                        (val solved: PlannerQuery with CardinalityEstimation)
  extends LogicalPlan with EagerLogicalPlan {

  val lhs = Some(left)
  val rhs = Some(right)

  val availableSymbols: Set[IdName] = left.availableSymbols ++ right.availableSymbols
}
