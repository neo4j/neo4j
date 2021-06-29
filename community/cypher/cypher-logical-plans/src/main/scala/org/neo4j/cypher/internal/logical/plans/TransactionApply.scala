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
 * For every row in left:
 *   Begin a new transaction
 *   Evaluate RHS with left row as an argument
 *   Record right output rows
 *   Once the output from RHS is depleted:
 *     Commit transaction
 *     Produce all recorded output rows
 *
 * {{{
 * for ( leftRow <- left ) {
 *   beginTx()
 *   right.setArgument( leftRow )
 *   for ( rightRow <- right ) {
 *     record( rightRow )
 *   }
 *   commitTx()
 *   for ( r <- recordedRows ) {
 *     produce r
 *   }
 * }
 * }}}
 */

case class TransactionApply(override val left: LogicalPlan, override val right: LogicalPlan)(implicit idGen: IdGen)
  extends LogicalBinaryPlan(idGen) with ApplyPlan {

  override def withLhs(newLHS: LogicalPlan)(idGen: IdGen): TransactionApply = copy(left = newLHS)(idGen)
  override def withRhs(newRHS: LogicalPlan)(idGen: IdGen): TransactionApply = copy(right = newRHS)(idGen)

  override val availableSymbols: Set[String] = left.availableSymbols ++ right.availableSymbols
}
