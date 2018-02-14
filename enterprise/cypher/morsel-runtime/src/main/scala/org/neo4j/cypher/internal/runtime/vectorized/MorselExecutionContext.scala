/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.runtime.vectorized

import org.neo4j.cypher.internal.runtime.interpreted.ExecutionContext
import org.neo4j.values.AnyValue

class MorselExecutionContext(morsel: Morsel, longsPerRow: Int, refsPerRow: Int, var currentRow: Int) extends ExecutionContext {

  def moveToNextRow(): Int = {
    currentRow += 1
    currentRow
  }

  override def copyTo(target: ExecutionContext, fromLongOffset: Int = 0, fromRefOffset: Int = 0, toLongOffset: Int = 0, toRefOffset: Int = 0): Unit = ???

  override def copyFrom(input: ExecutionContext, nLongs: Int, nRefs: Int): Unit = ???

  override def setLongAt(offset: Int, value: Long): Unit = morsel.longs(currentRow * longsPerRow + offset) = value

  override def getLongAt(offset: Int): Long = morsel.longs(currentRow * longsPerRow + offset)

  override def longs(): Array[Long] = ???

  override def setRefAt(offset: Int, value: AnyValue): Unit = morsel.refs(currentRow * refsPerRow + offset) = value

  override def getRefAt(offset: Int): AnyValue = morsel.refs(currentRow * refsPerRow + offset)

  override def refs(): Array[AnyValue] = ???

  override def set(newEntries: Seq[(String, AnyValue)]): ExecutionContext = ???

  override def set(key1: String, value1: AnyValue): ExecutionContext = ???

  override def set(key1: String, value1: AnyValue, key2: String, value2: AnyValue): ExecutionContext = ???

  override def set(key1: String, value1: AnyValue, key2: String, value2: AnyValue, key3: String, value3: AnyValue): ExecutionContext = ???

  override def mergeWith(other: ExecutionContext): ExecutionContext = ???

  override def createClone(): ExecutionContext = ???

  override def +=(kv: (String, AnyValue)): MorselExecutionContext.this.type = ???

  override def -=(key: String): MorselExecutionContext.this.type = ???

  override def get(key: String): Option[AnyValue] = ???

  override def iterator: Iterator[(String, AnyValue)] = ???

  override def copyWith(key1: String, value1: AnyValue) = ???

  override def copyWith(key1: String, value1: AnyValue, key2: String, value2: AnyValue) = ???

  override def copyWith(key1: String, value1: AnyValue, key2: String, value2: AnyValue, key3: String, value3: AnyValue): ExecutionContext = ???

  override def copyWith(newEntries: Seq[(String, AnyValue)]): ExecutionContext = ???

  override def boundEntities(materializeNode: Long => AnyValue, materializeRelationship: Long => AnyValue): Map[String, AnyValue] = ???

  override def isNull(key: String): Boolean = ???
}
