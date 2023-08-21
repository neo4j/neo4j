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
package org.neo4j.cypher.internal.runtime.interpreted.pipes

import org.neo4j.cypher.internal.runtime.ClosingIterator
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.PeekingIterator
import org.neo4j.cypher.internal.runtime.ReadableRow
import org.neo4j.cypher.internal.runtime.interpreted.pipes.OrderedUnionPipe.OrderedUnionIterator
import org.neo4j.cypher.internal.util.attribution.Id

import java.util.Comparator

case class OrderedUnionPipe(lhs: Pipe, rhs: Pipe, comparator: Comparator[ReadableRow])(val id: Id = Id.INVALID_ID)
    extends Pipe {

  protected def internalCreateResults(state: QueryState): ClosingIterator[CypherRow] = {
    val leftRows = new PeekingIterator(lhs.createResults(state))
    val rightRows = new PeekingIterator(rhs.createResults(state))
    new OrderedUnionIterator[CypherRow](leftRows, rightRows, comparator)
  }
}

object OrderedUnionPipe {

  class OrderedUnionIterator[T](
    leftRows: PeekingIterator[T],
    rightRows: PeekingIterator[T],
    comparator: Comparator[_ >: T]
  ) extends ClosingIterator[T] {

    override protected[this] def closeMore(): Unit = {
      leftRows.close()
      rightRows.close()
    }

    override protected[this] def innerHasNext: Boolean = leftRows.hasNext || rightRows.hasNext

    override def next(): T = {
      if (!rightRows.hasNext) {
        leftRows.next()
      } else if (!leftRows.hasNext) {
        rightRows.next()
      } else if (comparator.compare(leftRows.peek(), rightRows.peek()) <= 0) {
        leftRows.next()
      } else {
        rightRows.next()
      }
    }
  }
}
