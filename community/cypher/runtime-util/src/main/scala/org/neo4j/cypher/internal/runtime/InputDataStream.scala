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
 * Input data stream for Cypher query execution.
 */
trait InputDataStream {

  /**
   * Get a cursor which traverses a batch of the input stream. Thread-safe.
   *
   * @return initialized input cursor, or `null` if there is not more input
   */
  def nextInputBatch(): InputCursor
}

/**
 * Cursor which traverses a batch of cypher query input rows.
 */
trait InputCursor extends AutoCloseable {

  /**
   * Advance the cursor to the new input row.
   *
   * @return true if the cursor is not positioned at a new row, false if there are no more rows.
   */
  def next(): Boolean

  /**
   * Get a value in the input row.
   *
   * @param offset at which to get the value
   * @return the input value
   */
  def value(offset: Int): AnyValue
}

object NoInput extends InputDataStream {
  override def nextInputBatch(): InputCursor = null
}
