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

import org.neo4j.cypher.internal.expressions.RelationshipTypeToken
import org.neo4j.cypher.internal.logical.plans.IndexOrder
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.physicalplanning.SlottedIndexedProperty
import org.neo4j.cypher.internal.runtime.ClosingIterator
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.interpreted.pipes.Pipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.util.attribution.Id

case class UndirectedRelationshipIndexScanSlottedPipe(
  ident: String,
  startNode: String,
  endNode: String,
  relType: RelationshipTypeToken,
  properties: IndexedSeq[SlottedIndexedProperty],
  queryIndexId: Int,
  indexOrder: IndexOrder,
  slots: SlotConfiguration
)(val id: Id = Id.INVALID_ID) extends Pipe with IndexSlottedPipeWithValues {

  override val offset: Int = slots.getLongOffsetFor(ident)

  override val indexPropertyIndices: Array[Int] =
    properties.zipWithIndex.filter(_._1.getValueFromIndex).map(_._2).toArray

  override val indexPropertySlotOffsets: Array[Int] =
    properties.map(_.maybeCachedEntityPropertySlot).collect { case Some(o) => o }.toArray
  private val needsValues: Boolean = indexPropertyIndices.nonEmpty

  protected def internalCreateResults(state: QueryState): ClosingIterator[CypherRow] = {
    val cursor = state.query.relationshipIndexScan(state.queryIndexes(queryIndexId), needsValues, indexOrder)

    new SlottedUndirectedRelationshipIndexIterator(
      state,
      slots.getLongOffsetFor(startNode),
      slots.getLongOffsetFor(endNode),
      cursor
    )
  }
}
