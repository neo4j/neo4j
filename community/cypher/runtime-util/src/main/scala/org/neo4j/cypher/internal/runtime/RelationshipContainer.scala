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
package org.neo4j.cypher.internal.runtime

import org.neo4j.memory.MemoryTracker
import org.neo4j.values.virtual.ListValue
import org.neo4j.values.virtual.VirtualRelationshipValue
import org.neo4j.values.virtual.VirtualValues.EMPTY_LIST

/**
 * Utility class that has constant time `append`, `contains`, and `size` methods
 */
class RelationshipContainer private (val asList: ListValue, val size: Int, set: HeapTrackingLongImmutableSet) {

  def append(rel: VirtualRelationshipValue): RelationshipContainer = {
    new RelationshipContainer(asList.append(rel), size + 1, set + rel.id())
  }
  def contains(rel: VirtualRelationshipValue): Boolean = set.contains(rel.id())
  def contains(relId: Long): Boolean = set.contains(relId)

  def reverse: RelationshipContainer = new RelationshipContainer(asList.reverse(), size, set)

  def close(): Unit = {
    set.close()
  }
}

object RelationshipContainer {

  def empty(memoryTracker: MemoryTracker) =
    new RelationshipContainer(EMPTY_LIST, 0, HeapTrackingLongImmutableSet.emptySet(memoryTracker))
}
