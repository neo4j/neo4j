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
package org.neo4j.cypher.internal.runtime.interpreted

import org.neo4j.cypher.internal.runtime.ReadableRow
import org.neo4j.values.AnyValue
import org.neo4j.values.AnyValues

import java.util.Comparator

case class InterpretedExecutionContextOrdering(order: ColumnOrder) extends scala.Ordering[ReadableRow] {

  override def compare(a: ReadableRow, b: ReadableRow): Int = {
    val column = order.id
    val aVal = a.getByName(column)
    val bVal = b.getByName(column)
    order.compareValues(aVal, bVal)
  }
}

object InterpretedExecutionContextOrdering {

  def asComparator(orderBy: Seq[ColumnOrder]): Comparator[ReadableRow] =
    orderBy.map(InterpretedExecutionContextOrdering.apply)
      .reduceLeft[Comparator[ReadableRow]]((a, b) => a.thenComparing(b))
}

sealed trait ColumnOrder {
  def id: String

  def compareValues(a: AnyValue, b: AnyValue): Int
}

case class Ascending(id: String) extends ColumnOrder {
  override def compareValues(a: AnyValue, b: AnyValue): Int = AnyValues.COMPARATOR.compare(a, b)
}

case class Descending(id: String) extends ColumnOrder {
  override def compareValues(a: AnyValue, b: AnyValue): Int = AnyValues.COMPARATOR.compare(b, a)
}
