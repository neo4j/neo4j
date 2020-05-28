/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

import java.util.Comparator

import org.neo4j.cypher.internal.collection.DefaultComparatorTopTable
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.ReadableRow
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.NumericHelper
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.exceptions.InvalidArgumentException
import org.neo4j.memory.ScopedMemoryTracker
import org.neo4j.values.storable.FloatingPointValue

import scala.collection.Iterator.empty
import scala.collection.JavaConverters.asScalaIteratorConverter

/*
 * TopPipe is used when a query does a ORDER BY ... LIMIT query. Instead of ordering the whole result set and then
 * returning the matching top results, we only keep the top results in heap, which allows us to release memory earlier
 */
case class TopNPipe(source: Pipe, countExpression: Expression, comparator: Comparator[ReadableRow])
                   (val id: Id = Id.INVALID_ID) extends PipeWithSource(source) {

  protected override def internalCreateResults(input: Iterator[CypherRow], state: QueryState): Iterator[CypherRow] = {
    val limitNumber = NumericHelper.evaluateStaticallyKnownNumber(countExpression, state)
    if (limitNumber.isInstanceOf[FloatingPointValue]) {
      val limit = limitNumber.doubleValue()
      throw new InvalidArgumentException(s"LIMIT: Invalid input. '$limit' is not a valid value. Must be a non-negative integer.")
    }
    val limit = limitNumber.longValue()

    if (limit < 0) {
      throw new InvalidArgumentException(s"LIMIT: Invalid input. '$limit' is not a valid value. Must be a non-negative integer.")
    }

    if (limit == 0 || input.isEmpty) return empty

    val scopedMemoryTracker = state.memoryTracker.memoryTrackerForOperator(id.x).getScopedMemoryTracker
    val topTable = new DefaultComparatorTopTable[CypherRow](comparator, limit, scopedMemoryTracker)

    var i = 1L
    while (input.hasNext) {
      val row = input.next()
      val evictedRow = topTable.addAndGetEvicted(row)
      if (row ne evictedRow) {
        scopedMemoryTracker.allocateHeap(row.estimatedHeapUsage())
        if (evictedRow != null)
          scopedMemoryTracker.releaseHeap(evictedRow.estimatedHeapUsage())
      }
      i += 1
    }

    topTable.sort()

    topTable.autoClosingIterator(scopedMemoryTracker).asScala
  }
}

/*
 * Special case for when we only have one element, in this case it is no idea to store
 * an array, instead just store a single value.
 */
case class Top1Pipe(source: Pipe, comparator: Comparator[ReadableRow])
                   (val id: Id = Id.INVALID_ID) extends PipeWithSource(source) {

  protected override def internalCreateResults(input: Iterator[CypherRow],
                                               state: QueryState): Iterator[CypherRow] = {
    if (input.isEmpty) Iterator.empty
    else {

      val first = input.next()
      var result = first

      while (input.hasNext) {
        val ctx = input.next()
        if (comparator.compare(ctx, result) < 0) {
          result = ctx
        }
      }
      Iterator.single(result)
    }
  }
}

/*
 * Special case for when we only want one element, and all others that have the same value (tied for first place)
 */
case class Top1WithTiesPipe(source: Pipe, comparator: Comparator[ReadableRow])
                           (val id: Id = Id.INVALID_ID) extends PipeWithSource(source) {

  protected override def internalCreateResults(input: Iterator[CypherRow],
                                               state: QueryState): Iterator[CypherRow] = {
    if (input.isEmpty)
      Iterator.empty
    else {
      val first = input.next()
      var best = first
      val matchingRows = init(best)

      while (input.hasNext) {
        val ctx = input.next()
        val comparison = comparator.compare(ctx, best)
        if (comparison < 0) { // Found a new best
          best = ctx
          matchingRows.clear()
          matchingRows += ctx
        }

        if (comparison == 0) { // Found a tie
          matchingRows += ctx
        }
      }
      matchingRows.result().iterator
    }
  }

  @inline
  private def init(first: CypherRow) = {
    val builder = Vector.newBuilder[CypherRow]
    builder += first
    builder
  }
}