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

import org.eclipse.collections.impl.set.mutable.primitive.LongHashSet
import org.neo4j.cypher.internal.runtime.ClosingIterator
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.IsNoValue
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.exceptions.CypherTypeException
import org.neo4j.values.AnyValue
import org.neo4j.values.virtual.VirtualNodeValue

import scala.collection.Iterator
import scala.collection.mutable.ListBuffer

case class TriadicSelectionPipe(
  positivePredicate: Boolean,
  left: Pipe,
  source: String,
  seen: String,
  target: String,
  right: Pipe
)(val id: Id = Id.INVALID_ID)
    extends PipeWithSource(left) {

  override protected def internalCreateResults(
    input: ClosingIterator[CypherRow],
    state: QueryState
  ): ClosingIterator[CypherRow] = {
    var triadicState: LongHashSet = null
    // 1. Build
    new LazyGroupingIterator[CypherRow](input) {
      override def getKey(row: CypherRow): AnyValue = row.getByName(source)

      override def getValue(row: CypherRow): Option[Long] = row.getByName(seen) match {
        case n: VirtualNodeValue => Some(n.id())
        case IsNoValue()         => None
        case x                   => throw new CypherTypeException(s"Expected a node at `$seen` but got $x")
      }

      override def setState(triadicSet: LongHashSet): Unit = triadicState = triadicSet

      // 2. pass through 'right'
    }.flatMap { outerContext =>
      val innerState = state.withInitialContext(outerContext)
      right.createResults(innerState)

    // 3. Probe
    }.filter { ctx =>
      ctx.getByName(target) match {
        case n: VirtualNodeValue =>
          if (positivePredicate) triadicState.contains(n.id()) else !triadicState.contains(n.id())
        case _ => false
      }
    }
  }
}

abstract class LazyGroupingIterator[ROW >: Null <: AnyRef](val input: ClosingIterator[ROW])
    extends ClosingIterator[ROW] {
  def setState(state: LongHashSet): Unit
  def getKey(row: ROW): Any
  def getValue(row: ROW): Option[Long]

  var current: Iterator[ROW] = _
  var nextRow: ROW = _

  override def next(): ROW = if (hasNext) current.next() else Iterator.empty.next()

  override protected[this] def innerHasNext: Boolean = {
    if (current != null && current.hasNext)
      true
    else {
      val firstRow =
        if (nextRow != null) {
          val row = nextRow
          nextRow = null
          row
        } else if (input.hasNext) {
          input.next()
        } else null
      if (firstRow == null) {
        current = null
        setState(null)
        false
      } else {
        val buffer = new ListBuffer[ROW]
        val valueSet = new LongHashSet()
        setState(valueSet)
        buffer += firstRow
        update(valueSet, firstRow)
        val key = getKey(firstRow)
        var shouldContinue = input.hasNext
        while (shouldContinue) {
          val row = input.next()
          val s = getKey(row)
          if (s == key) {
            update(valueSet, row)
            buffer += row
            shouldContinue = input.hasNext
          } else {
            nextRow = row
            shouldContinue = false
          }
        }
        current = buffer.iterator
        true
      }
    }
  }

  override protected[this] def closeMore(): Unit = {
    input.close()
  }

  def update(triadicSet: LongHashSet, row: ROW): AnyVal = {
    for (value <- getValue(row))
      triadicSet.add(value)
  }
}
