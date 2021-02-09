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
 * Either executes the left-hand side or if the left-hand side is empty it executes the right-hand side.
 */
case class EitherPlan(left: LogicalPlan, right: LogicalPlan)(implicit idGen: IdGen) extends LogicalPlan(idGen)  {
  override def lhs: Option[LogicalPlan] = Some(left)

  override def rhs: Option[LogicalPlan] = Some(right)

  override def availableSymbols: Set[String] = left.availableSymbols ++ right.availableSymbols
}

