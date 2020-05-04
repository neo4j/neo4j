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

import org.neo4j.cypher.internal.util.attribution.IdGen

/**
 * Join two result streams using a hash table. 'Left' is completely consumed and buffered in a hash table, using a
 * tuple consisting of the values assigned to 'nodes'. For every 'right' row, lookup the corresponding 'left' rows
 * based on 'nodes'. For each corresponding left row, merge that with the current right row and produce.
 *
 * hashTable = {}
 * for ( leftRow <- left )
 *   group = hashTable.getOrUpdate( key( leftRow, nodes ), List[Row]() )
 *   group += leftRow
 *
 * for ( rightRow <- right )
 *   group = hashTable.get( key( rightRow, nodes ) )
 *   for ( leftRow <- group )
 *     produce (leftRow merge rightRow)
 */
case class NodeHashJoin(nodes: Set[String],
                        left: LogicalPlan,
                        right: LogicalPlan)
                       (implicit idGen: IdGen) extends LogicalPlan(idGen) with EagerLogicalPlan {

  val lhs = Some(left)
  val rhs = Some(right)

  override val availableSymbols: Set[String] = left.availableSymbols ++ right.availableSymbols
}
