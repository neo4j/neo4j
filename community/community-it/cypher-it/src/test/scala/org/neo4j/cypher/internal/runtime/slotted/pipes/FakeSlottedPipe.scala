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
package org.neo4j.cypher.internal.runtime.slotted.pipes

import org.neo4j.cypher.internal.expressions.ASTCachedProperty
import org.neo4j.cypher.internal.physicalplanning.LongSlot
import org.neo4j.cypher.internal.physicalplanning.RefSlot
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.runtime.ClosingIterator
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.interpreted.pipes
import org.neo4j.cypher.internal.runtime.interpreted.pipes.FakePipe.CountingIterator
import org.neo4j.cypher.internal.runtime.interpreted.pipes.Pipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.runtime.slotted.SlottedRow
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.kernel.impl.util.ValueUtils
import org.neo4j.values.storable.Value

import scala.collection.mutable

case class FakeSlottedPipe(slots: SlotConfiguration, data: Iterable[Map[Any, Any]]*)
    extends Pipe {

  private val dataIterator = data.iterator
  private val createdResults = mutable.Buffer.empty[CountingIterator[CypherRow]]

  def internalCreateResults(state: QueryState): ClosingIterator[CypherRow] = {
    val next =
      if (!dataIterator.hasNext) {
        Iterable.empty
      } else {
        dataIterator.next()
      }
    val it = next.iterator.map { values =>
      val result = SlottedRow(slots)

      values foreach {
        case (key: String, value) =>
          slots(key) match {
            case LongSlot(offset, _, _) if value == null =>
              result.setLongAt(offset, -1)

            case LongSlot(offset, _, _) =>
              result.setLongAt(offset, value.asInstanceOf[Number].longValue())

            case RefSlot(offset, _, _) =>
              result.setRefAt(offset, ValueUtils.of(value))

            case _ => throw new IllegalArgumentException(s"Failed to find slot for $key -> $value")
          }
        case (cachedProp: ASTCachedProperty, value) =>
          slots.getCachedPropertySlot(cachedProp.runtimeKey).foreach(refSlot =>
            result.setCachedPropertyAt(refSlot.offset, ValueUtils.of(value).asInstanceOf[Value])
          )
        case other => throw new IllegalArgumentException(s"Unknown value $other")
      }
      result
    }
    val countingIt = new pipes.FakePipe.CountingIterator[CypherRow](it)
    createdResults += countingIt
    countingIt
  }

  var id: Id = Id.INVALID_ID

  def wasClosed: Boolean = currentIterator.wasClosed
  def allWasClosed: Boolean = createdResults.forall(_.wasClosed)

  def currentIterator: CountingIterator[CypherRow] = createdResults.last
  def allIterators: Seq[CountingIterator[CypherRow]] = createdResults.toSeq
}
