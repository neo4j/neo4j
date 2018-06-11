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
package org.neo4j.cypher.internal.runtime.vectorized.operators

import java.util.Comparator

import org.neo4j.cypher.internal.compatibility.v3_5.runtime.{LongSlot, RefSlot}
import org.neo4j.cypher.internal.runtime.slotted.pipes.ColumnOrder
import org.neo4j.cypher.internal.runtime.vectorized.{Morsel, MorselExecutionContext}
import org.neo4j.values.AnyValue

object MorselSorting {

  def compareMorselIndexesByColumnOrder(row: MorselExecutionContext)(order: ColumnOrder): Comparator[Integer] = order.slot match {
    case LongSlot(offset, _, _) =>
      new Comparator[Integer] {
        override def compare(idx1: Integer, idx2: Integer): Int = {
          // TODO this is kind of weird
          row.moveToRow(idx1)
          val aVal = row.getLongAt(offset)
          row.moveToRow(idx2)
          val bVal = row.getLongAt(offset)
          order.compareLongs(aVal, bVal)
        }
      }

    case RefSlot(offset, _, _) =>
      new Comparator[Integer] {
        override def compare(idx1: Integer, idx2: Integer): Int = {
          row.moveToRow(idx1)
          val aVal = row.getRefAt(offset)
          row.moveToRow(idx2)
          val bVal = row.getRefAt(offset)
          order.compareValues(aVal, bVal)
        }
      }
  }

  def createMorselIndexesArray(row: MorselExecutionContext): Array[Integer] = {
    val rows = row.numberOfRows
    val list = new Array[Integer](rows)
    var idx = 0
    while (idx < rows) {
      list(idx) = idx
      idx += 1
    }
    list
  }


  /**
    * Sorts the morsel data from array of ordered indices.
    *
    * Does this by sorting into a temp morsel first and then copying back the sorted data.
    */
  def createSortedMorselData(inputRow: MorselExecutionContext, outputToInputIndexes: Array[Integer]): Unit = {
    // Create a temporary morsel
    // TODO: Do this without creating extra arrays
    val tempMorsel = new Morsel(new Array[Long](inputRow.numberOfRows * inputRow.getLongsPerRow), new Array[AnyValue](inputRow.numberOfRows * inputRow.getRefsPerRow), inputRow.numberOfRows)
    val outputRow = MorselExecutionContext(tempMorsel, inputRow.getLongsPerRow, inputRow.getRefsPerRow)

    while (outputRow.hasMoreRows) {
      val fromIndex = outputToInputIndexes(outputRow.getCurrentRow)
      inputRow.moveToRow(fromIndex)

      outputRow.copyFrom(inputRow)
      outputRow.moveToNextRow()
    }

    // Copy from output morsel back to inout morsel
    inputRow.copyAllRowsFrom(outputRow)
  }

  def createMorselComparator(order: ColumnOrder): Comparator[MorselExecutionContext] = order.slot match {
    case LongSlot(offset, _, _) =>
      new Comparator[MorselExecutionContext] {
        override def compare(m1: MorselExecutionContext, m2: MorselExecutionContext): Int = {
          val aVal = m1.getLongAt(offset)
          val bVal = m2.getLongAt(offset)
          order.compareLongs(aVal, bVal)
        }
      }
    case RefSlot(offset, _, _) =>
      new Comparator[MorselExecutionContext] {
        override def compare(m1: MorselExecutionContext, m2: MorselExecutionContext): Int = {
          val aVal = m1.getRefAt(offset)
          val bVal = m2.getRefAt(offset)
          order.compareValues(aVal, bVal)
        }
      }

  }

}
