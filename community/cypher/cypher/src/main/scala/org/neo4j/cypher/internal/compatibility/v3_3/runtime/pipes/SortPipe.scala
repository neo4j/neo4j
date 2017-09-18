/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.compatibility.v3_3.runtime.pipes

import java.util.Comparator

import org.neo4j.cypher.internal.compatibility.v3_3.runtime.ExecutionContext
import org.neo4j.cypher.internal.v3_4.logical.plans.LogicalPlanId
import org.neo4j.values.{AnyValue, AnyValues}

case class SortPipe(source: Pipe, orderBy: Seq[ColumnOrder])
                   (val id: LogicalPlanId = LogicalPlanId.DEFAULT)
  extends PipeWithSource(source) {
  assert(orderBy.nonEmpty)

  private val comparator = orderBy
    .map(new ExecutionContextOrdering(_))
    .reduceLeft[Comparator[ExecutionContext]]((a, b) => a.thenComparing(b))

  protected def internalCreateResults(input: Iterator[ExecutionContext], state: QueryState): Iterator[ExecutionContext] = {
    val array = input.toArray
    java.util.Arrays.sort(array, comparator)
    array.toIterator
  }
}

private class ExecutionContextOrdering(order: ColumnOrder) extends scala.Ordering[ExecutionContext] {
  override def compare(a: ExecutionContext, b: ExecutionContext): Int = {
    val column = order.id
    val aVal = a(column)
    val bVal = b(column)
    order.compareValues(aVal, bVal)
  }
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
