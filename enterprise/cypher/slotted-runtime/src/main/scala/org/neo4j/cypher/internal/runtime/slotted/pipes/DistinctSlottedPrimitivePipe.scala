/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.runtime.slotted.pipes

import org.neo4j.cypher.internal.compatibility.v3_4.runtime.{Slot, SlotConfiguration}
import org.neo4j.cypher.internal.runtime.interpreted.ExecutionContext
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.{Pipe, PipeWithSource, QueryState}
import org.neo4j.cypher.internal.runtime.slotted.SlottedExecutionContext
import org.neo4j.cypher.internal.runtime.slotted.helpers.SlottedPipeBuilderUtils
import org.neo4j.cypher.internal.util.v3_4.attribution.Id

import scala.collection.immutable

case class DistinctSlottedPrimitivePipe(source: Pipe,
                                        slots: SlotConfiguration,
                                        primitiveSlots: Array[Int],
                                        projections: Map[Slot, Expression])
                                       (val id: Id = Id.INVALID_ID)
  extends PipeWithSource(source) {

  //===========================================================================
  // Compile-time initializations
  //===========================================================================
  private val setValuesInOutput: immutable.Iterable[(ExecutionContext, QueryState, ExecutionContext) => Unit] = projections.map {
    case (slot, expression) =>
      val f = SlottedPipeBuilderUtils.makeSetValueInSlotFunctionFor(slot)
      (incomingContext: ExecutionContext, state: QueryState, outgoingContext: ExecutionContext) =>
        f(outgoingContext, expression(incomingContext, state))
  }

  projections.values.foreach(_.registerOwningPipe(this))

  //===========================================================================
  // Runtime code
  //===========================================================================
  protected def internalCreateResults(input: Iterator[ExecutionContext],
                                      state: QueryState): Iterator[ExecutionContext] = {

    new Iterator[ExecutionContext] {
      private val seen = new LongArraySet(32, projections.size)
      private var buffer: ExecutionContext = _

      pullNextElementFromSource()

      private def pullNextElementFromSource(): Unit = {
        buffer = null
        while (input.nonEmpty) { // Let's pull data until we find something not already seen
          val next = input.next()
          val keys: Array[Long] = primitiveSlots.map(next.getLongAt)
          if (!seen.contains(keys)) {
            seen.add(keys)
            // Found something! Set it as the next element to yield, and exit
            val outgoing = SlottedExecutionContext(slots)
            setValuesInOutput.foreach {
              _ (next, state, outgoing)
            }
            buffer = outgoing
            return
          }
        }
      }

      override def hasNext: Boolean = buffer != null

      override def next(): ExecutionContext =
        if (isEmpty)
          Iterator.empty.next() // Throw a good exception
        else {
          val current = buffer
          pullNextElementFromSource()
          current
        }
    }
  }
}
