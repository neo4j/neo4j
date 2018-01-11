/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import org.neo4j.cypher.internal.ir.v3_4.IdName
import org.neo4j.cypher.internal.util.v3_4.attribution.IdGen

/**
  * Produces source rows, unless source is empty. In that case, a single row is produced containing argument and any
  * non-argument variables set to NO_VALUE.
  */
case class Optional(source: LogicalPlan, protectedSymbols: Set[IdName] = Set.empty)
                   (implicit idGen: IdGen)
  extends LogicalPlan(idGen) with LazyLogicalPlan {

  val lhs = Some(source)
  val rhs = None

  def availableSymbols: Set[IdName] = source.availableSymbols
}
