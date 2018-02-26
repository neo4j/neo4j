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
package org.neo4j.cypher.internal.runtime.vectorized.operators

import org.neo4j.cypher.internal.compatibility.v3_4.runtime.SlotConfiguration
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.interpreted.pipes.LazyTypes
import org.neo4j.cypher.internal.runtime.slotted.helpers.NullChecker.entityIsNull
import org.neo4j.cypher.internal.runtime.vectorized._
import org.neo4j.cypher.internal.util.v3_4.InternalException
import org.neo4j.cypher.internal.v3_4.expressions.SemanticDirection
import org.neo4j.internal.kernel.api.helpers.RelationshipSelectionCursor

class ExpandAllOperator(toSlots: SlotConfiguration,
                        fromSlots: SlotConfiguration,
                        fromOffset: Int,
                        relOffset: Int,
                        toOffset: Int,
                        dir: SemanticDirection,
                        types: LazyTypes) extends Operator {

  override def operate(source: Message,
                       output: Morsel,
                       context: QueryContext,
                       state: QueryState): Continuation = {

    /*
    This might look wrong, but it's like this by design. This allows the loop to terminate early and still be
    picked up at any point again - all without impacting the tight loop.
    The mutable state is an unfortunate cost for this feature.
     */
    var readPos = 0
    var writePos = 0
    var relationships: RelationshipSelectionCursor = null
    var input: Morsel = null
    var iterationState: Iteration = null

    source match {
      case StartLoopWithSingleMorsel(data, is) =>
        input = data
        iterationState = is
      case ContinueLoopWith(ContinueWithData(data, index, is)) =>
        input = data
        readPos = index
        iterationState = is
      case ContinueLoopWith(ContinueWithDataAndSource(data, index, rels, is)) =>
        input = data
        readPos = index
        iterationState = is
        relationships = rels.asInstanceOf[RelationshipSelectionCursor]
      case _ =>
        throw new InternalException("Unknown continuation received")
    }

    val inputLongCount = fromSlots.numberOfLongs
    val inputRefCount = fromSlots.numberOfReferences
    val outputLongCount = toSlots.numberOfLongs
    val outputRefCount = toSlots.numberOfReferences

    while (readPos < input.validRows && writePos < output.validRows) {

      val fromNode = input.longs(readPos * inputLongCount + fromOffset)
      if (entityIsNull(fromNode))
      readPos += 1
      else {
        if (relationships == null) {
          relationships = context.getRelationshipsCursor(fromNode, dir, types.types(context))
        }

        while (writePos < output.validRows && relationships.next()) {
          val relId = relationships.relationshipReference()
          val otherSide = relationships.otherNodeReference()

          // Now we have everything needed to create a row.
          System.arraycopy(input.longs, readPos * inputLongCount, output.longs, writePos * outputLongCount, inputLongCount)
          System.arraycopy(input.refs, readPos * inputRefCount, output.refs, writePos * outputRefCount, inputRefCount)
          output.longs(writePos * outputLongCount + relOffset) = relId
          output.longs(writePos * outputLongCount + toOffset) = otherSide
          writePos += 1
        }

        //we haven't filled up the rows
        if (writePos < output.validRows) {
          relationships = null
          readPos += 1
        }
      }
    }

    val next = if (readPos < input.validRows || relationships != null) {
      if(relationships == null)
        ContinueWithData(input, readPos, iterationState)
      else
        ContinueWithDataAndSource(input, readPos, relationships, iterationState)
    } else {
      if (relationships != null) {
        relationships.close()
        relationships = null
      }
      EndOfLoop(iterationState)
    }

    output.validRows = writePos
    next
  }

  override def addDependency(pipeline: Pipeline): Dependency = Lazy(pipeline)
}
