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

import org.neo4j.collection.primitive.PrimitiveLongIterator
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.helpers.PrimitiveLongHelper
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.{Slot, SlotConfiguration}
import org.neo4j.cypher.internal.runtime.interpreted.ExecutionContext
import org.neo4j.cypher.internal.runtime.interpreted.commands.predicates.Predicate
import org.neo4j.cypher.internal.runtime.interpreted.pipes._
import org.neo4j.cypher.internal.runtime.slotted.SlottedExecutionContext
import org.neo4j.cypher.internal.runtime.slotted.helpers.NullChecker.entityIsNull
import org.neo4j.cypher.internal.runtime.slotted.helpers.SlottedPipeBuilderUtils.makeGetPrimitiveNodeFromSlotFunctionFor
import org.neo4j.cypher.internal.util.v3_4.attribution.Id
import org.neo4j.cypher.internal.v3_4.expressions.SemanticDirection

case class OptionalExpandIntoSlottedPipe(source: Pipe,
                                         fromSlot: Slot,
                                         relOffset: Int,
                                         toSlot: Slot,
                                         dir: SemanticDirection,
                                         lazyTypes: LazyTypes,
                                         predicate: Predicate,
                                         slots: SlotConfiguration)
                                        (val id: Id = Id.INVALID_ID)
  extends PipeWithSource(source) with PrimitiveCachingExpandInto {
  self =>

  //===========================================================================
  // Compile-time initializations
  //===========================================================================
  private final val CACHE_SIZE = 100000
  private val getFromNodeFunction = makeGetPrimitiveNodeFromSlotFunctionFor(fromSlot)
  private val getToNodeFunction = makeGetPrimitiveNodeFromSlotFunctionFor(toSlot)

  //===========================================================================
  // Runtime code
  //===========================================================================
  protected def internalCreateResults(input: Iterator[ExecutionContext], state: QueryState): Iterator[ExecutionContext] = {
    //cache of known connected nodes
    val relCache = new PrimitiveRelationshipsCache(CACHE_SIZE)

    input.flatMap {
      (inputRow: ExecutionContext) =>
        val fromNode = getFromNodeFunction(inputRow)
        val toNode = getToNodeFunction(inputRow)

        if (entityIsNull(fromNode) || entityIsNull(toNode)) {
          Iterator(withNulls(inputRow))
        } else {
          val relationships: PrimitiveLongIterator = relCache.get(fromNode, toNode, dir)
            .getOrElse(findRelationships(state.query, fromNode, toNode, relCache, dir, lazyTypes.types(state.query)))

          val matchIterator = PrimitiveLongHelper.map(relationships, relId => {
            val outputRow = SlottedExecutionContext(slots)
            inputRow.copyTo(outputRow)
            outputRow.setLongAt(relOffset, relId)
            outputRow
          }).filter(ctx => predicate.isTrue(ctx, state))

          if (matchIterator.isEmpty)
            Iterator(withNulls(inputRow))
          else
            matchIterator
        }
    }
  }

  private def withNulls(inputRow: ExecutionContext) = {
    val outputRow = SlottedExecutionContext(slots)
    inputRow.copyTo(outputRow)
    outputRow.setLongAt(relOffset, -1)
    outputRow
  }

}
