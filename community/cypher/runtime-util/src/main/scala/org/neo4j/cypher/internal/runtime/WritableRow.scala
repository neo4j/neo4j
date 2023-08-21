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

import org.neo4j.values.AnyValue

/**
 * Cypher row which allows writing variables and invalidating cached properties.
 */
trait WritableRow extends CachedPropertiesRow {

  def setLongAt(offset: Int, value: Long): Unit
  def setRefAt(offset: Int, value: AnyValue): Unit

  def set(newEntries: collection.Seq[(String, AnyValue)]): Unit
  def set(key: String, value: AnyValue): Unit
  def set(key1: String, value1: AnyValue, key2: String, value2: AnyValue): Unit
  def set(key1: String, value1: AnyValue, key2: String, value2: AnyValue, key3: String, value3: AnyValue): Unit

  def mergeWith(other: ReadableRow, entityById: EntityById, checkNullability: Boolean = true): Unit
  def copyAllFrom(input: ReadableRow): Unit
  def copyFrom(input: ReadableRow, nLongs: Int, nRefs: Int): Unit
  def copyLongsFrom(input: ReadableRow, fromOffset: Int, toOffset: Int, length: Int): Unit
  def copyRefsFrom(input: ReadableRow, fromOffset: Int, toOffset: Int, length: Int): Unit

  def copyFromOffset(
    input: ReadableRow,
    sourceLongOffset: Int,
    sourceRefOffset: Int,
    targetLongOffset: Int,
    targetRefOffset: Int
  ): Unit

  /**
   * Invalidate all cached node properties for the given node id
   */
  def invalidateCachedNodeProperties(node: Long): Unit

  def invalidateCachedProperties(): Unit

  /**
   * Invalidate all cached relationship properties for the given relationship id
   */
  def invalidateCachedRelationshipProperties(rel: Long): Unit

  // Linenumber and filename specifics
  // ===================================
  def setLinenumber(line: Option[ResourceLinenumber]): Unit
}
