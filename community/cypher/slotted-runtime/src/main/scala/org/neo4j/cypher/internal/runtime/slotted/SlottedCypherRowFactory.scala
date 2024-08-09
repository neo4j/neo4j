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
package org.neo4j.cypher.internal.runtime.slotted

import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.ReadableRow
import org.neo4j.cypher.internal.runtime.interpreted.pipes.CypherRowFactory
import org.neo4j.values.AnyValue

case class SlottedCypherRowFactory(slots: SlotConfiguration, argumentSize: SlotConfiguration.Size)
    extends CypherRowFactory {
  private val nLongArgs = argumentSize.nLongs
  private val nRefArgs = argumentSize.nReferences

  override def newRow(): CypherRow =
    SlottedRow(slots)

  override def copyArgumentOf(init: ReadableRow): CypherRow = {
    val newCtx = SlottedRow(slots)
    newCtx.copyFrom(init, nLongArgs, nRefArgs)
    newCtx
  }

  override def copyWith(row: ReadableRow): CypherRow = {
    val newCtx = SlottedRow(slots)
    newCtx.copyAllFrom(row)
    newCtx
  }

  override def copyWith(row: ReadableRow, key: String, value: AnyValue): CypherRow = {
    val newCtx = SlottedRow(slots)
    newCtx.copyAllFrom(row)
    newCtx.set(key, value)
    newCtx
  }

  override def copyWith(row: ReadableRow, key1: String, value1: AnyValue, key2: String, value2: AnyValue): CypherRow = {
    val newCopy = SlottedRow(slots)
    newCopy.copyAllFrom(row)
    newCopy.set(key1, value1)
    newCopy.set(key2, value2)
    newCopy
  }

  override def copyWith(
    row: ReadableRow,
    key1: String,
    value1: AnyValue,
    key2: String,
    value2: AnyValue,
    key3: String,
    value3: AnyValue
  ): CypherRow = {
    val newCopy = SlottedRow(slots)
    newCopy.copyAllFrom(row)
    newCopy.set(key1, value1)
    newCopy.set(key2, value2)
    newCopy.set(key3, value3)
    newCopy
  }
}
