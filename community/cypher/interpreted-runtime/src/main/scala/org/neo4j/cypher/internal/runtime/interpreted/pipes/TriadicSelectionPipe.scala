/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.runtime.interpreted.pipes

import org.eclipse.collections.impl.set.mutable.primitive.LongHashSet
import org.neo4j.cypher.internal.runtime.interpreted.ExecutionContext
import org.opencypher.v9_0.util.CypherTypeException
import org.opencypher.v9_0.util.attribution.Id
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.VirtualNodeValue

import scala.collection.mutable.ListBuffer
import scala.collection.{AbstractIterator, Iterator}

case class TriadicSelectionPipe(positivePredicate: Boolean, left: Pipe, source: String, seen: String, target: String, right: Pipe)
                               (val id: Id = Id.INVALID_ID)
extends PipeWithSource(left) {

  override protected def internalCreateResults(input: Iterator[ExecutionContext], state: QueryState) = {
    var triadicState: LongHashSet = null
    // 1. Build
    new LazyGroupingIterator[ExecutionContext](input) {
      override def getKey(row: ExecutionContext) = row(source)

      override def getValue(row: ExecutionContext) = row(seen) match {
        case n: VirtualNodeValue => Some(n.id())
        case Values.NO_VALUE => None
        case x => throw new CypherTypeException(s"Expected a node at `$seen` but got $x")
      }

      override def setState(triadicSet: LongHashSet) = triadicState = triadicSet

    // 2. pass through 'right'
    }.flatMap { (outerContext) =>
      val original = outerContext.createClone()
      val innerState = state.withInitialContext(outerContext)
      val innerResults = right.createResults(innerState)
      innerResults.map { context => context mergeWith original }

    // 3. Probe
    }.filter { ctx =>
      ctx(target) match {
        case n: VirtualNodeValue => if(positivePredicate) triadicState.contains(n.id()) else !triadicState.contains(n.id())
        case _ => false
      }
    }
  }
}

abstract class LazyGroupingIterator[ROW >: Null <: AnyRef](val input: Iterator[ROW]) extends AbstractIterator[ROW] {
  def setState(state: LongHashSet)
  def getKey(row: ROW): Any
  def getValue(row: ROW): Option[Long]

  var current: Iterator[ROW] = null
  var nextRow: ROW = null

  override def next() = if(hasNext) current.next() else Iterator.empty.next()

  override def hasNext: Boolean = {
    if (current != null && current.hasNext)
      true
    else {
      val firstRow = if(nextRow != null) {
        val row = nextRow
        nextRow = null
        row
      } else if(input.hasNext) {
        input.next()
      } else null
      if (firstRow == null) {
        current = null
        setState(null)
        false
      }
      else {
        val buffer = new ListBuffer[ROW]
        val valueSet = new LongHashSet()
        setState(valueSet)
        buffer += firstRow
        update(valueSet, firstRow)
        val key = getKey(firstRow)
        // N.B. should we rewrite takeWhile to a while-loop?
        buffer ++= input.takeWhile{ row =>
          val s = getKey(row)
          if (s == key) {
            update(valueSet, row)
            true
          } else {
            nextRow = row
            false
          }
        }
        current = buffer.iterator
        true
      }
    }
  }

  def update(triadicSet: LongHashSet, row: ROW): AnyVal = {
    for (value <- getValue(row))
      triadicSet.add(value)
  }
}
