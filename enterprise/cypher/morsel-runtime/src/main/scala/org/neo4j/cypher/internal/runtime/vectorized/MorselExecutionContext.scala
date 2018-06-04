/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.cypher.internal.runtime.vectorized

import org.neo4j.cypher.internal.runtime.interpreted.ExecutionContext
import org.neo4j.values.AnyValue
import org.opencypher.v9_0.util.InternalException

object MorselExecutionContext {
  def apply(morsel: Morsel, pipeline: Pipeline) = new MorselExecutionContext(morsel,
    pipeline.slots.numberOfLongs, pipeline.slots.numberOfReferences, 0)
  def apply(morsel: Morsel, numberOfLongs: Int, numberOfRows: Int) = new MorselExecutionContext(morsel,
    numberOfLongs, numberOfRows, 0)
}

class MorselExecutionContext(private val morsel: Morsel, private val longsPerRow: Int, private val refsPerRow: Int, private var currentRow: Int) extends ExecutionContext {

  def moveToNextRow(): Unit = {
    currentRow += 1
  }

  def getCurrentRow: Int = currentRow

  def getLongsPerRow: Int = longsPerRow

  def getRefsPerRow: Int = refsPerRow

  def moveToRow(row: Int): Unit = currentRow = row

  def resetToFirstRow(): Unit = currentRow = 0

  /**
    * Checks if the morsel has more rows
    */
  def hasMoreRows: Boolean = currentRow < morsel.validRows

  def numberOfRows: Int = morsel.validRows

  /**
    * Check so that there is at least one valid row of data
    */
  def hasData: Boolean = morsel.validRows > 0

  /**
    * Set the valid rows of the morsel to the current position, which usually
    * happens after one operator finishes writing to a morsel.
    */
  def finishedWriting(): Unit = morsel.validRows = currentRow

  def copyAllRowsFrom(input: ExecutionContext): Unit = input match {
    case other:MorselExecutionContext =>
      System.arraycopy(other.morsel.longs, 0, morsel.longs, 0, other.morsel.longs.length)
      System.arraycopy(other.morsel.refs, 0, morsel.refs, 0, other.morsel.refs.length)
    case _ => fail()
  }

  override def copyTo(target: ExecutionContext, fromLongOffset: Int = 0, fromRefOffset: Int = 0, toLongOffset: Int = 0, toRefOffset: Int = 0): Unit = ???

  override def copyFrom(input: ExecutionContext, nLongs: Int, nRefs: Int): Unit = input match {
    case other:MorselExecutionContext =>
      if (nLongs > longsPerRow || nRefs > refsPerRow)
        throw new InternalException("Tried to copy too much data.")
      else {
        System.arraycopy(other.morsel.longs, other.longsAtCurrentRow, morsel.longs, longsAtCurrentRow, nLongs)
        System.arraycopy(other.morsel.refs, other.refsAtCurrentRow, morsel.refs, refsAtCurrentRow, nRefs)
      }
    case _ => fail()
  }

  override def toString(): String = {
    s"MorselExecutionContext(morsel=$morsel, longsPerRow=$longsPerRow, refsPerRow=$refsPerRow, currentRow=$currentRow)"
  }

  /**
    * Copies the whole row from input to this.
    * @param input
    */
  def copyFrom(input: MorselExecutionContext): Unit = copyFrom(input, input.longsPerRow, input.refsPerRow)

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

  override def createClone(): MorselExecutionContext = new MorselExecutionContext(morsel, longsPerRow, refsPerRow, currentRow)

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

  private def longsAtCurrentRow: Int = currentRow * longsPerRow

  private def refsAtCurrentRow: Int = currentRow * refsPerRow

  private def fail(): Nothing = throw new InternalException("Tried using a wrong context.")
}
