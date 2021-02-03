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
 * OnMatchApply produces the left-hand side and for each produced row from the left-hand side it performs
 * its right-hand side as a side-effect.
 */
case class OnMatchApply(input: LogicalPlan, onMatch: LogicalPlan)(implicit idGen: IdGen) extends LogicalBinaryPlan(idGen) with ApplyPlan {
  override def left: LogicalPlan = input

  override def right: LogicalPlan = onMatch

  override def withLhs(newLHS: LogicalPlan)(idGen: IdGen): LogicalBinaryPlan = copy(input = newLHS)(idGen)
  override def withRhs(newRHS: LogicalPlan)(idGen: IdGen): LogicalBinaryPlan = copy(onMatch = newRHS)(idGen)

  override def availableSymbols: Set[String] = input.availableSymbols ++ onMatch.availableSymbols
}
