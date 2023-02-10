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
package org.neo4j.cypher.internal.runtime.slotted.expressions

import org.neo4j.collection.trackable.HeapTrackingLongHashSet
import org.neo4j.cypher.internal.physicalplanning.Slot
import org.neo4j.cypher.internal.physicalplanning.SlotConfigurationUtils.makeGetPrimitiveRelationshipFromSlotFunctionFor
import org.neo4j.cypher.internal.runtime.ReadableRow
import org.neo4j.cypher.internal.runtime.interpreted.commands.AstNode
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.values.storable.BooleanValue
import org.neo4j.values.storable.Values.booleanValue

case class TrailRelationshipsUniqueExpression(trailStateMetadataSlotOffset: Int, innerRelationships: Set[Slot])
    extends Expression
    with SlottedExpression {

  private[this] val innerRelGetters =
    innerRelationships.map(s => makeGetPrimitiveRelationshipFromSlotFunctionFor(s)).toArray

  override def apply(row: ReadableRow, state: QueryState): BooleanValue = {
    val allInnerUnique = allInnerRelationshipsUnique(row)
    booleanValue(allInnerUnique && allRelationshipsSeenUnique(row))
  }

  override def children: collection.Seq[AstNode[_]] = Seq.empty

  private def allInnerRelationshipsUnique(row: ReadableRow): Boolean = {
    if (innerRelGetters.length <= 1) true
    else if (innerRelGetters.length == 2) {
      innerRelGetters(0).applyAsLong(row) != innerRelGetters(1).applyAsLong(row)
    } else {
      val innerRelationships = innerRelGetters.map(g => g.applyAsLong(row))
      var i = 0
      var allRelationshipsUnique = true
      while (allRelationshipsUnique && i < innerRelationships.length - 1) {
        var j = i + 1
        while (allRelationshipsUnique && j < innerRelationships.length) {
          allRelationshipsUnique = innerRelationships(i) != innerRelationships(j)
          j = j + 1
        }
        i = i + 1
      }
      allRelationshipsUnique
    }
  }

  private def allRelationshipsSeenUnique(row: ReadableRow): Boolean = {
    val relationshipsSeen = row.getRefAt(trailStateMetadataSlotOffset).asInstanceOf[TrailState].relationshipsSeen
    var i = 0
    while (i < innerRelGetters.length) {
      val r = innerRelGetters(i)
      if (relationshipsSeen.contains(r.applyAsLong(row))) {
        return false
      }
      i += 1
    }
    true
  }
}

trait TrailState {
  def relationshipsSeen: HeapTrackingLongHashSet
}
