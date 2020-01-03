/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

import org.neo4j.cypher.internal.v4_0.expressions.LogicalProperty
import org.neo4j.cypher.internal.v4_0.util.attribution.IdGen

/**
 * Reads properties of a set of nodes or relationships and caches them in the current row.
 * Later accesses to this property can then read from this cache instead of reading from the store.
 */
case class CacheProperties(source: LogicalPlan, properties: Set[LogicalProperty])(implicit idGen: IdGen)
  extends LogicalPlan(idGen) with LazyLogicalPlan {

  override val lhs = Some(source)
  override def rhs = None
  override val availableSymbols: Set[String] = source.availableSymbols
}
