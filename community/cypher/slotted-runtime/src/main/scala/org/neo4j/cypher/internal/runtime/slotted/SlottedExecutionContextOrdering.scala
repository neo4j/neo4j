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

import org.neo4j.cypher.internal.physicalplanning.LongSlot
import org.neo4j.cypher.internal.physicalplanning.RefSlot
import org.neo4j.cypher.internal.physicalplanning.Slot
import org.neo4j.cypher.internal.runtime.ReadableRow
import org.neo4j.cypher.internal.util.symbols.CTNode
import org.neo4j.cypher.internal.util.symbols.CTRelationship
import org.neo4j.exceptions.CantCompileQueryException
import org.neo4j.values.AnyValue
import org.neo4j.values.AnyValues
import org.neo4j.values.virtual.VirtualNodeValue
import org.neo4j.values.virtual.VirtualRelationshipValue
import org.neo4j.values.virtual.VirtualValues

import java.util.Comparator

object SlottedExecutionContextOrdering {

  /**
   * Create a comparator for a [[ColumnOrder]] that uses the same slot for every comparable row.
   */
  def comparator(order: ColumnOrder): Comparator[ReadableRow] = order.slot match {
    case LongSlot(offset, true, _) =>
      new Comparator[ReadableRow] {

        override def compare(a: ReadableRow, b: ReadableRow): Int = {
          val aVal = a.getLongAt(offset)
          val bVal = b.getLongAt(offset)
          order.compareNullableLongs(aVal, bVal)
        }
      }

    case LongSlot(offset, false, _) =>
      new Comparator[ReadableRow] {

        override def compare(a: ReadableRow, b: ReadableRow): Int = {
          val aVal = a.getLongAt(offset)
          val bVal = b.getLongAt(offset)
          order.compareLongs(aVal, bVal)
        }
      }

    case RefSlot(offset, _, _) =>
      new Comparator[ReadableRow] {

        override def compare(a: ReadableRow, b: ReadableRow): Int = {
          val aVal = a.getRefAt(offset)
          val bVal = b.getRefAt(offset)
          order.compareValues(aVal, bVal)
        }
      }
  }

  /**
   * Create a comparator for a [[ColumnOrder2]] that uses two different slots for
   * rows originating from lhs or rhs.
   */
  def comparator2(order: ColumnOrder2): Comparator[ReadableRow] = (order.slot, order.rhsSlot) match {
    case (LongSlot(offsetA, false, _), LongSlot(offsetB, false, _)) =>
      new Comparator[ReadableRow] {

        override def compare(a: ReadableRow, b: ReadableRow): Int = {
          val aVal = a.getLongAt(offsetA)
          val bVal = b.getLongAt(offsetB)
          order.compareLongs(aVal, bVal)
        }
      }

    case (LongSlot(offsetA, _, _), LongSlot(offsetB, _, _)) =>
      new Comparator[ReadableRow] {

        override def compare(a: ReadableRow, b: ReadableRow): Int = {
          val aVal = a.getLongAt(offsetA)
          val bVal = b.getLongAt(offsetB)
          order.compareNullableLongs(aVal, bVal)
        }
      }

    case (LongSlot(offsetA, _, CTNode), RefSlot(offsetB, _, CTNode)) =>
      new Comparator[ReadableRow] {

        override def compare(a: ReadableRow, b: ReadableRow): Int = {
          val aVal = a.getLongAt(offsetA)
          val bVal = b.getRefAt(offsetB).asInstanceOf[VirtualNodeValue].id()
          order.compareNullableLongs(aVal, bVal)
        }
      }

    case (LongSlot(offsetA, _, CTRelationship), RefSlot(offsetB, _, CTRelationship)) =>
      new Comparator[ReadableRow] {

        override def compare(a: ReadableRow, b: ReadableRow): Int = {
          val aVal = a.getLongAt(offsetA)
          val bVal = b.getRefAt(offsetB).asInstanceOf[VirtualRelationshipValue].id()
          order.compareNullableLongs(aVal, bVal)
        }
      }

    case (RefSlot(offsetA, _, CTNode), LongSlot(offsetB, _, CTNode)) =>
      new Comparator[ReadableRow] {

        override def compare(a: ReadableRow, b: ReadableRow): Int = {
          val aVal = a.getRefAt(offsetA).asInstanceOf[VirtualNodeValue].id()
          val bVal = b.getLongAt(offsetB)
          order.compareNullableLongs(aVal, bVal)
        }
      }

    case (RefSlot(offsetA, _, CTRelationship), LongSlot(offsetB, _, CTRelationship)) =>
      new Comparator[ReadableRow] {

        override def compare(a: ReadableRow, b: ReadableRow): Int = {
          val aVal = a.getRefAt(offsetA).asInstanceOf[VirtualRelationshipValue].id()
          val bVal = b.getLongAt(offsetB)
          order.compareNullableLongs(aVal, bVal)
        }
      }

    case (LongSlot(offsetA, _, CTNode), RefSlot(offsetB, _, _)) =>
      new Comparator[ReadableRow] {

        override def compare(a: ReadableRow, b: ReadableRow): Int = {
          val aVal = VirtualValues.node(a.getLongAt(offsetA))
          val bVal = b.getRefAt(offsetB)
          order.compareValues(aVal, bVal)
        }
      }

    case (LongSlot(offsetA, _, CTRelationship), RefSlot(offsetB, _, _)) =>
      new Comparator[ReadableRow] {

        override def compare(a: ReadableRow, b: ReadableRow): Int = {
          val aVal = VirtualValues.relationship(a.getLongAt(offsetA))
          val bVal = b.getRefAt(offsetB)
          order.compareValues(aVal, bVal)
        }
      }

    case (RefSlot(offsetA, _, _), LongSlot(offsetB, _, CTNode)) =>
      new Comparator[ReadableRow] {

        override def compare(a: ReadableRow, b: ReadableRow): Int = {
          val aVal = a.getRefAt(offsetA)
          val bVal = VirtualValues.node(b.getLongAt(offsetB))
          order.compareValues(aVal, bVal)
        }
      }

    case (RefSlot(offsetA, _, _), LongSlot(offsetB, _, CTRelationship)) =>
      new Comparator[ReadableRow] {

        override def compare(a: ReadableRow, b: ReadableRow): Int = {
          val aVal = a.getRefAt(offsetA)
          val bVal = VirtualValues.relationship(b.getLongAt(offsetB))
          order.compareValues(aVal, bVal)
        }
      }

    case (RefSlot(offsetA, _, _), RefSlot(offsetB, _, _)) =>
      new Comparator[ReadableRow] {

        override def compare(a: ReadableRow, b: ReadableRow): Int = {
          val aVal = a.getRefAt(offsetA)
          val bVal = b.getRefAt(offsetB)
          order.compareValues(aVal, bVal)
        }
      }

    case _ =>
      throw new CantCompileQueryException(s"Do not know how to create comparator for $order")
  }

  /**
   * Compose a comparator for a sequence of [[ColumnOrder]]s that uses the same slot every comparable row.
   */
  def asComparator(orderBy: collection.Seq[ColumnOrder]): Comparator[ReadableRow] =
    composeComparator[ReadableRow, ColumnOrder](SlottedExecutionContextOrdering.comparator)(orderBy)

  /**
   * Compose a comparator for a sequence of [[ColumnOrder2]]s that uses two different slots for
   * rows originating from lhs or rhs.
   */
  def asComparator2(orderBy: collection.Seq[ColumnOrder2]): Comparator[ReadableRow] =
    composeComparator[ReadableRow, ColumnOrder2](SlottedExecutionContextOrdering.comparator2)(orderBy)

  def composeComparator[T, C <: ColumnOrder](singleComparatorCreator: C => Comparator[T])(orderBy: collection.Seq[C])
    : Comparator[T] = {
    val size = orderBy.size

    // For size 1 and 2 the overhead of doing the foreach is measurable
    if (size == 1) {
      singleComparatorCreator(orderBy.head)
    } else if (size == 2) {
      val first = singleComparatorCreator(orderBy.head)
      val second = singleComparatorCreator(orderBy.last)
      (a, b) => {
        val i = first.compare(a, b)
        if (i == 0) {
          second.compare(a, b)
        } else {
          i
        }
      }
    } else {
      val comparators = new Array[Comparator[T]](size)
      var i = 0
      orderBy.foreach { columnOrder =>
        comparators(i) = singleComparatorCreator(columnOrder)
        i += 1
      }

      // For larger ORDER BY the overhead is negligible
      new Comparator[T] {
        override def compare(a: T, b: T): Int = {
          var c = 0
          while (c < comparators.length) {
            val i = comparators(c).compare(a, b)
            if (i != 0) {
              return i;
            }
            c += 1
          }
          0
        }
      }
    }
  }
}

sealed trait ColumnOrder {
  def slot: Slot

  def compareValues(a: AnyValue, b: AnyValue): Int
  def compareLongs(a: Long, b: Long): Int
  def compareNullableLongs(a: Long, b: Long): Int
}

case class Ascending(slot: Slot) extends ColumnOrder {
  override def compareValues(a: AnyValue, b: AnyValue): Int = AnyValues.COMPARATOR.compare(a, b)
  override def compareLongs(a: Long, b: Long): Int = java.lang.Long.compare(a, b)
  override def compareNullableLongs(a: Long, b: Long): Int = java.lang.Long.compareUnsigned(a, b)
}

case class Descending(slot: Slot) extends ColumnOrder {
  override def compareValues(a: AnyValue, b: AnyValue): Int = AnyValues.COMPARATOR.compare(b, a)
  override def compareLongs(a: Long, b: Long): Int = java.lang.Long.compare(b, a)
  override def compareNullableLongs(a: Long, b: Long): Int = java.lang.Long.compareUnsigned(b, a)
}

/**
 * This is used when comparing rows from two different downstreams that may have
 * different slot configurations,
 * i.e. the same column variable may be located in different slots depending on
 * if the row is from the left-hand side or the right-hand side.
 */
sealed trait ColumnOrder2 extends ColumnOrder {
  def rhsSlot: Slot
}

case class Ascending2(slot: Slot, rhsSlot: Slot) extends ColumnOrder2 {
  override def compareValues(a: AnyValue, b: AnyValue): Int = AnyValues.COMPARATOR.compare(a, b)
  override def compareLongs(a: Long, b: Long): Int = java.lang.Long.compare(a, b)
  override def compareNullableLongs(a: Long, b: Long): Int = java.lang.Long.compareUnsigned(a, b)
}

case class Descending2(slot: Slot, rhsSlot: Slot) extends ColumnOrder2 {
  override def compareValues(a: AnyValue, b: AnyValue): Int = AnyValues.COMPARATOR.compare(b, a)
  override def compareLongs(a: Long, b: Long): Int = java.lang.Long.compare(b, a)
  override def compareNullableLongs(a: Long, b: Long): Int = java.lang.Long.compareUnsigned(b, a)
}
