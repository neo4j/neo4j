/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.cypher.internal.runtime.slotted

import java.util.Comparator

import org.neo4j.cypher.internal.compatibility.v3_4.runtime.{LongSlot, RefSlot}
import org.neo4j.cypher.internal.runtime.interpreted.ExecutionContext
import org.neo4j.cypher.internal.runtime.slotted.pipes.ColumnOrder

object ExecutionContextOrdering {
  def comparator(order: ColumnOrder): scala.Ordering[ExecutionContext] = order.slot match {
    case LongSlot(offset, true, _) =>
      new scala.Ordering[ExecutionContext] {
        override def compare(a: ExecutionContext, b: ExecutionContext): Int = {
          val aVal = a.getLongAt(offset)
          val bVal = b.getLongAt(offset)
          order.compareNullableLongs(aVal, bVal)
        }
      }

    case LongSlot(offset, false, _) =>
      new scala.Ordering[ExecutionContext] {
        override def compare(a: ExecutionContext, b: ExecutionContext): Int = {
          val aVal = a.getLongAt(offset)
          val bVal = b.getLongAt(offset)
          order.compareLongs(aVal, bVal)
        }
      }

    case RefSlot(offset, _, _) =>
      new scala.Ordering[ExecutionContext] {
        override def compare(a: ExecutionContext, b: ExecutionContext): Int = {
          val aVal = a.getRefAt(offset)
          val bVal = b.getRefAt(offset)
          order.compareValues(aVal, bVal)
        }
      }
  }

  def asComparator(orderBy: Seq[ColumnOrder]): Comparator[ExecutionContext] =
    orderBy.map(ExecutionContextOrdering.comparator)
    .reduceLeft[Comparator[ExecutionContext]]((a, b) => a.thenComparing(b))
}
