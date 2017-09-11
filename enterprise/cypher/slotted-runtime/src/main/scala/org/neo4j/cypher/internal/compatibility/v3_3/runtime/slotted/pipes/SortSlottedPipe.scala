/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.compatibility.v3_3.runtime.slotted.pipes

import java.util.Comparator

import org.neo4j.cypher.internal.compatibility.v3_3.runtime._
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.pipes.{Pipe, PipeWithSource, QueryState}
import org.neo4j.cypher.internal.v3_3.logical.plans.LogicalPlanId
import org.neo4j.values.{AnyValue, AnyValues}

case class SortSlottedPipe(source: Pipe, orderBy: Seq[ColumnOrder], pipelineInformation: PipelineInformation)(val id: LogicalPlanId = LogicalPlanId.DEFAULT)
  extends PipeWithSource(source) {
  assert(orderBy.nonEmpty)

  private val comparator = orderBy
    .map(ExecutionContextOrdering.comparator(_))
    .reduceLeft[Comparator[ExecutionContext]]((a, b) => a.thenComparing(b))

  override protected def internalCreateResults(input: Iterator[ExecutionContext], state: QueryState): Iterator[ExecutionContext] = {
    val array = input.toArray
    java.util.Arrays.sort(array, comparator)
    array.toIterator
  }
}

object ExecutionContextOrdering {
  def comparator(order: ColumnOrder): scala.Ordering[ExecutionContext] = order.slot match {
    case LongSlot(offset, _, _, _) =>
      new scala.Ordering[ExecutionContext] {
        override def compare(a: ExecutionContext, b: ExecutionContext): Int = {
          val aVal = a.getLongAt(offset)
          val bVal = b.getLongAt(offset)
          order.compareLongs(aVal, bVal)
        }
      }

    case RefSlot(offset, _, _, _) =>
      new scala.Ordering[ExecutionContext] {
        override def compare(a: ExecutionContext, b: ExecutionContext): Int = {
          val aVal = a.getRefAt(offset)
          val bVal = b.getRefAt(offset)
          order.compareValues(aVal, bVal)
        }
      }
  }
}

sealed trait ColumnOrder {
  def slot: Slot

  def compareValues(a: AnyValue, b: AnyValue): Int
  def compareLongs(a: Long, b: Long): Int
}

case class Ascending(slot: Slot) extends ColumnOrder {
  override def compareValues(a: AnyValue, b: AnyValue): Int = AnyValues.COMPARATOR.compare(a, b)
  override def compareLongs(a: Long, b: Long): Int = java.lang.Long.compare(a, b)
}

case class Descending(slot: Slot) extends ColumnOrder {
  override def compareValues(a: AnyValue, b: AnyValue): Int = AnyValues.COMPARATOR.compare(b, a)
  override def compareLongs(a: Long, b: Long): Int = java.lang.Long.compare(b, a)
}
