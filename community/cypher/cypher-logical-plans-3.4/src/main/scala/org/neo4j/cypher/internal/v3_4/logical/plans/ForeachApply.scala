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

import org.neo4j.cypher.internal.v3_4.expressions.Expression
import org.neo4j.cypher.internal.util.v3_4.attribution.IdGen

/**
  * ForeachApply is a side-effect type apply, which operates on a list value. Each left row is used to compute a
  * list, and each value in this list applied as the argument to right. Left rows are produced unchanged.
  *
  * for ( leftRow <- left)
  *   list <- expression.evaluate( leftRow )
  *   for ( value <- list )
  *     right.setArgument( value )
  *     for ( rightRow <- right )
  *       // just consume
  *
  *   produce leftRow
  */
case class ForeachApply(left: LogicalPlan, right: LogicalPlan, variable: String, expression: Expression)(implicit idGen: IdGen)
  extends LogicalPlan(idGen) with LazyLogicalPlan {

  val lhs = Some(left)
  val rhs = Some(right)

  override val availableSymbols: Set[String] = left.availableSymbols // NOTE: right.availableSymbols and variable are not available outside
}
