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
package org.neo4j.cypher.internal.runtime.slotted.pipes

import org.neo4j.cypher.internal.compatibility.v3_4.runtime.{Slot, SlotConfiguration}
import org.neo4j.cypher.internal.runtime.interpreted.ExecutionContext
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.{Pipe, PipeWithSource, QueryState}
import org.neo4j.cypher.internal.runtime.slotted.SlottedExecutionContext
import org.neo4j.cypher.internal.runtime.slotted.helpers.SlottedPipeBuilderUtils
import org.neo4j.cypher.internal.util.v3_4.attribution.Id
import org.neo4j.values.AnyValue
import org.neo4j.values.virtual.VirtualValues

import scala.collection.mutable

case class DistinctSlottedPipe(source: Pipe,
                               slots: SlotConfiguration,
                               groupingExpressions: Map[Slot, Expression])
                              (val id: Id = Id.INVALID_ID)
  extends PipeWithSource(source) {

  //===========================================================================
  // Compile-time initializations
  //===========================================================================
  private val groupingSetInSlotFunctions = groupingExpressions.map {
    case (slot, expression) =>
      val f = SlottedPipeBuilderUtils.makeSetValueInSlotFunctionFor(slot)
      (incomingContext: ExecutionContext, state: QueryState, outgoingContext: ExecutionContext) =>
        f(outgoingContext, expression(incomingContext, state))
  }

  private val groupingGetFromSlotFunctions = groupingExpressions.map {
    case (slot, _) =>
      SlottedPipeBuilderUtils.makeGetValueFromSlotFunctionFor(slot)
  }.toSeq

  groupingExpressions.values.foreach(_.registerOwningPipe(this))

  //===========================================================================
  // Runtime code
  //===========================================================================
  protected def internalCreateResults(input: Iterator[ExecutionContext],
                                      state: QueryState): Iterator[ExecutionContext] = {
    // For each incoming row, run expression and put it into the correct slot in the context
    val result = input.map(incoming => {
      val outgoing = SlottedExecutionContext(slots)
      groupingSetInSlotFunctions.foreach { _(incoming, state, outgoing) }
      outgoing
    })

    /*
     * Filter out rows we have already seen
     */
    var seen = mutable.Set[AnyValue]()
    result.filter { ctx =>
      val values = VirtualValues.list(groupingGetFromSlotFunctions.map(f => f(ctx)): _*)
      if (seen.contains(values)) {
        false
      } else {
        seen += values
        true
      }
    }
  }
}
