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
package org.neo4j.cypher.internal.compatibility.v3_3.runtime.slotted.pipes

import org.neo4j.cypher.internal.compatibility.v3_3.runtime._
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.pipes.{Pipe, PipeWithSource, QueryState}
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.slotted.PrimitiveExecutionContext
import org.neo4j.cypher.internal.v3_3.logical.plans.LogicalPlanId
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values
import org.neo4j.values.storable.Values.NO_VALUE
import org.neo4j.values.virtual.VirtualValues

case class RollUpApplySlottedPipe(lhs: Pipe, rhs: Pipe, collectionName: String, identifierToCollect: String, nullableIdentifiers: Set[String], pipelineInformation: PipelineInformation)
                                 (val id: LogicalPlanId = LogicalPlanId.DEFAULT)
  extends PipeWithSource(lhs) {

  private val slot = pipelineInformation.get(collectionName).get

  private val setCollection: (ExecutionContext, AnyValue) => Unit = {
    slot match {
      case RefSlot(offset, _, _, _) =>
        (ctx: ExecutionContext, value: AnyValue) => ctx.setRefAt(offset, value)
      case _ => throw new InternalError("Expected collection to be allocated to a ref slot")
    }
  }

  override protected def internalCreateResults(input: Iterator[ExecutionContext], state: QueryState) = {
    input.map {
      ctx =>
        val outputRow = PrimitiveExecutionContext(pipelineInformation)
        ctx.copyTo(outputRow)

        if (nullableIdentifiers.exists { elem =>
          val elemSlot = pipelineInformation.get(elem)
          elemSlot match {
            case Some(LongSlot(offset, true, _, _)) if (ctx.getLongAt(offset) == -1) => true
            case Some(RefSlot(offset, true, _, _)) if (ctx.getRefAt(offset) == NO_VALUE) => true
            case _ => false
          }
        }) {
          setCollection(outputRow, NO_VALUE)
        }
        else {
          val innerState = state.withInitialContext(outputRow)
          val innerResults: Iterator[ExecutionContext] = rhs.createResults(innerState)
          val collection = VirtualValues.list(innerResults.map { m =>
            //TODO: Is this correct?
            val maybeSlot = pipelineInformation.get(identifierToCollect)
            if (maybeSlot.isEmpty) {
              NO_VALUE
            } else {
              val rhsSlot = maybeSlot.get
              rhsSlot match {
                case LongSlot(offset, _, _, _) => Values.longValue(m.getLongAt(offset))
                case RefSlot(offset, _, _, _) => m.getRefAt(offset)
              }
            }
          }.toArray: _*)
          setCollection(outputRow, collection)
        }
        outputRow
    }
  }
}
