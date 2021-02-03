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
case class SemiApply(override val left: LogicalPlan, override val right: LogicalPlan)(implicit idGen: IdGen)
  extends AbstractSemiApply(left)(idGen) {
  override def withLhs(newLHS: LogicalPlan)(idGen: IdGen): LogicalBinaryPlan = copy(left = newLHS)(idGen)
  override def withRhs(newRHS: LogicalPlan)(idGen: IdGen): LogicalBinaryPlan = copy(right = newRHS)(idGen)
}

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
case class AntiSemiApply(override val left: LogicalPlan, override val right: LogicalPlan)(implicit idGen: IdGen)
  extends AbstractSemiApply(left)(idGen) {
  override def withLhs(newLHS: LogicalPlan)(idGen: IdGen): LogicalBinaryPlan = copy(left = newLHS)(idGen)
  override def withRhs(newRHS: LogicalPlan)(idGen: IdGen): LogicalBinaryPlan = copy(right = newRHS)(idGen)
}

abstract class AbstractSemiApply(left: LogicalPlan)(idGen: IdGen)
  extends LogicalBinaryPlan(idGen) with ApplyPlan with SingleFromRightLogicalPlan {

  val availableSymbols: Set[String] = left.availableSymbols
}
