/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.cypher.internal.v3_4.logical.plans

import org.neo4j.cypher.internal.util.v3_4.attribution.IdGen

/**
  * For every row in 'left', set that row as the argument, and apply to 'right'. Produce left row, but only if right
  * produces at least one row.
  *
  * for ( leftRow <- left ) {
  *   right.setArgument( leftRow )
  *   if ( right.nonEmpty ) {
  *     produce leftRow
  *   }
  * }
  */
case class SemiApply(left: LogicalPlan, right: LogicalPlan)(implicit idGen: IdGen)
  extends AbstractSemiApply(left, right)(idGen)

/**
  * For every row in 'left', set that row as the argument, and apply to 'right'. Produce left row, but only if right
  * produces no rows.
  *
  * for ( leftRow <- left ) {
  *   right.setArgument( leftRow )
  *   if ( right.isEmpty ) {
  *     produce leftRow
  *   }
  * }
  */
case class AntiSemiApply(left: LogicalPlan, right: LogicalPlan)(implicit idGen: IdGen)
  extends AbstractSemiApply(left, right)(idGen)

abstract class AbstractSemiApply(left: LogicalPlan, right: LogicalPlan)(idGen: IdGen)
  extends LogicalPlan(idGen) with LazyLogicalPlan {
  val lhs = Some(left)
  val rhs = Some(right)

  val availableSymbols: Set[String] = left.availableSymbols
}
