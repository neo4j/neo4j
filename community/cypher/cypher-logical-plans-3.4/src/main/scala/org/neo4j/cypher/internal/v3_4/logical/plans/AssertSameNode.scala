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
  * For every row in left, assert that all rows in right produce the same value
  * for the variable IdName. Produce the rows from left.
  *
  * for ( leftRow <- left )
  *   for ( rightRow <- right )
  *     assert( leftRow(node) == rightRow(node) )
  *   produce leftRow
  *
  * This operator is planned for merges using unique index seeks.
  */
case class AssertSameNode(node: IdName, left: LogicalPlan, right: LogicalPlan)(val solved: PlannerQuery with CardinalityEstimation)
  extends LogicalPlan with LazyLogicalPlan {

  val lhs = Some(left)
  val rhs = Some(right)

  def availableSymbols: Set[IdName] = left.availableSymbols ++ right.availableSymbols + node
}
