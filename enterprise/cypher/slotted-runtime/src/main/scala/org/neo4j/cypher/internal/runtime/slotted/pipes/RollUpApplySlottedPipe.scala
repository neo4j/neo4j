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

import org.neo4j.cypher.internal.compatibility.v3_4.runtime.{LongSlot, RefSlot, SlotConfiguration}
import org.neo4j.cypher.internal.runtime.interpreted.ExecutionContext
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.{Pipe, PipeWithSource, QueryState}
import org.neo4j.cypher.internal.util.v3_4.attribution.Id
import org.neo4j.values.storable.Values.NO_VALUE
import org.neo4j.values.virtual.VirtualValues

case class RollUpApplySlottedPipe(lhs: Pipe, rhs: Pipe,
                                  collectionRefSlotOffset: Int,
                                  identifierToCollect: (String, Expression),
                                  nullableIdentifiers: Set[String],
                                  slots: SlotConfiguration)
                                 (val id: Id = Id.INVALID_ID)
  extends PipeWithSource(lhs) {

  private val getValueToCollectFunction = {
    val expression: Expression = identifierToCollect._2
    (state: QueryState) => (ctx: ExecutionContext) => expression(ctx, state)
  }

  private val hasNullValuePredicates: Seq[(ExecutionContext) => Boolean] =
    nullableIdentifiers.toSeq.map { elem =>
      val elemSlot = slots.get(elem)
      elemSlot match {
        case Some(LongSlot(offset, true, _)) => { (ctx: ExecutionContext) => ctx.getLongAt(offset) == -1 }
        case Some(RefSlot(offset, true, _)) => { (ctx: ExecutionContext) => ctx.getRefAt(offset) == NO_VALUE }
        case _ => { (ctx: ExecutionContext) => false }
      }
    }

  private def hasNullValue(ctx: ExecutionContext): Boolean =
    hasNullValuePredicates.exists(p => p(ctx))

  override protected def internalCreateResults(input: Iterator[ExecutionContext], state: QueryState) = {
    input.map {
      ctx =>
        if (hasNullValue(ctx)) {
          ctx.setRefAt(collectionRefSlotOffset, NO_VALUE)
        }
        else {
          val innerState = state.withInitialContext(ctx)
          val innerResults: Iterator[ExecutionContext] = rhs.createResults(innerState)
          val collection = VirtualValues.list(innerResults.map(getValueToCollectFunction(state)).toArray: _*)
          ctx.setRefAt(collectionRefSlotOffset, collection)
        }
        ctx
    }
  }
}
