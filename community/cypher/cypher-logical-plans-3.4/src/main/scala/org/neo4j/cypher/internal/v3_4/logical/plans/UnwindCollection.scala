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
  * For each source row, evaluate 'expression'. If 'expression' evaluates to a list, produce one row per list
  * element, containing the source row and the element assigned to 'variable'. If 'expression' does not evaluate to a
  * list, produce nothing.
  */
case class UnwindCollection(source: LogicalPlan, variable: String, expression: Expression)(implicit idGen: IdGen)
  extends LogicalPlan(idGen) with LazyLogicalPlan {
  val lhs = Some(source)
  def rhs = None

  val availableSymbols: Set[String] = source.availableSymbols + variable
}
