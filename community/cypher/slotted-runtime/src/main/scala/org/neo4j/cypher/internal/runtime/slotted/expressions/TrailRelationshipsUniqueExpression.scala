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

case class TrailRelationshipsUniqueExpression(trailStateMetadataSlotOffset: Int, innerRelationship: Slot)
    extends Expression
    with SlottedExpression {

  private[this] val innerRelGetter = makeGetPrimitiveRelationshipFromSlotFunctionFor(innerRelationship)

  override def apply(row: ReadableRow, state: QueryState): BooleanValue = {
    booleanValue(allRelationshipsSeenUnique(row))
  }

  override def children: collection.Seq[AstNode[_]] = Seq.empty

  private def allRelationshipsSeenUnique(row: ReadableRow): Boolean = {
    val relationshipsSeen = row.getRefAt(trailStateMetadataSlotOffset).asInstanceOf[TrailState].relationshipsSeen
    !relationshipsSeen.contains(innerRelGetter.applyAsLong(row))
  }
}

trait TrailState {
  def relationshipsSeen: HeapTrackingLongHashSet
}
