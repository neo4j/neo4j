/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.runtime.slotted.pipes

import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.runtime.ClosingIterator
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.PeekingIterator
import org.neo4j.cypher.internal.runtime.ReadableRow
import org.neo4j.cypher.internal.runtime.interpreted.pipes.OrderedUnionPipe.OrderedUnionIterator
import org.neo4j.cypher.internal.runtime.interpreted.pipes.Pipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.runtime.slotted.SlottedPipeMapper.RowMapping
import org.neo4j.cypher.internal.runtime.slotted.pipes.UnionSlottedPipe.mapRow
import org.neo4j.cypher.internal.util.attribution.Id

import java.util.Comparator

case class OrderedUnionSlottedPipe(
  lhs: Pipe,
  rhs: Pipe,
  slots: SlotConfiguration,
  lhsMapping: RowMapping,
  rhsMapping: RowMapping,
  comparator: Comparator[ReadableRow]
)(val id: Id = Id.INVALID_ID) extends Pipe {

  protected def internalCreateResults(state: QueryState): ClosingIterator[CypherRow] = {
    val leftRows = new PeekingIterator(lhs.createResults(state).map(mapRow(slots, lhsMapping, _, state)))
    val rightRows = new PeekingIterator(rhs.createResults(state).map(mapRow(slots, rhsMapping, _, state)))
    new OrderedUnionIterator[CypherRow](leftRows, rightRows, comparator)
  }
}
