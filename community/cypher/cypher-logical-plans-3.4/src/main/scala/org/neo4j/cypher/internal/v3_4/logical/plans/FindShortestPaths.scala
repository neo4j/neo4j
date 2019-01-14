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
import org.neo4j.cypher.internal.ir.v3_4.ShortestPathPattern
import org.neo4j.cypher.internal.util.v3_4.attribution.IdGen

/**
  * Find the shortest paths between two nodes, as specified by 'shortestPath'. For each shortest path found produce a
  * row containing the source row and the found path.
  */
case class FindShortestPaths(source: LogicalPlan, shortestPath: ShortestPathPattern,
                             predicates: Seq[Expression] = Seq.empty,
                             withFallBack: Boolean = false, disallowSameNode: Boolean = true)
                            (implicit idGen: IdGen)
  extends LogicalPlan(idGen) with LazyLogicalPlan {

  val lhs = Some(source)
  def rhs = None

  override val availableSymbols: Set[String] = source.availableSymbols ++ shortestPath.availableSymbols
}
