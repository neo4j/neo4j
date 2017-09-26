/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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

import org.neo4j.cypher.internal.compatibility.v3_4.runtime.PipelineInformation
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.slotted.helpers.NullChecker.nodeIsNull
import org.neo4j.cypher.internal.runtime.vectorized._
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.interpreted.pipes.LazyTypes
import org.neo4j.cypher.internal.util.v3_4.InternalException
import org.neo4j.cypher.internal.v3_4.expressions.SemanticDirection
import org.neo4j.kernel.impl.api.RelationshipVisitor
import org.neo4j.kernel.impl.api.store.RelationshipIterator

class ExpandAllOperator(toPipeline: PipelineInformation,
                        fromPipeline: PipelineInformation,
                        fromOffset: Int,
                        relOffset: Int,
                        toOffset: Int,
                        dir: SemanticDirection,
                        types: LazyTypes) extends Operator {

  override def operate( source: Message,
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
    var relationships: RelationshipIterator = null
    var input: Morsel = null
    var iterationState: Iteration = null

    source match {
      case WorkWithLazyData(data, is) =>
        input = data
        iterationState = is
      case ContinueWith(ContinueWithData(data, index, is)) =>
        input = data
        readPos = index
        iterationState = is
      case ContinueWith(ContinueWithDataAndSource(data, index, rels, is)) =>
        input = data
        readPos = index
        iterationState = is
        relationships = rels.asInstanceOf[RelationshipIterator]
      case _ =>
        throw new InternalException("Unknown continuation received")
    }

    while (readPos < input.validRows && writePos < output.validRows) {

      val inputLongRow = readPos * fromPipeline.numberOfLongs
      val fromNode = input.longs(inputLongRow + fromOffset)

      if (nodeIsNull(fromNode))
        readPos += 1
      else {
        if (relationships == null) {
          relationships = context.getRelationshipsForIdsPrimitive(fromNode, dir, types.types(context))
        }

        var otherSide: Long = 0
        val relVisitor = new RelationshipVisitor[InternalException] {
          override def visit(relationshipId: Long, typeId: Int, startNodeId: Long, endNodeId: Long): Unit =
            if (fromNode == startNodeId)
              otherSide = endNodeId
            else
              otherSide = startNodeId
        }

        while (writePos < output.validRows && relationships.hasNext) {
          val relId = relationships.next()
          relationships.relationshipVisit(relId, relVisitor)

          // Now we have everything needed to create a row.
          val outputRow = toPipeline.numberOfLongs * writePos
          System.arraycopy(input.longs, inputLongRow, output.longs, outputRow, fromPipeline.numberOfLongs)
          System.arraycopy(input.refs, fromPipeline.numberOfReferences * readPos, output.refs, toPipeline.numberOfReferences * writePos, fromPipeline.numberOfReferences)
          output.longs(outputRow + relOffset) = relId
          output.longs(outputRow + toOffset) = otherSide
          writePos += 1
        }

        if (!relationships.hasNext) {
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
    } else
      EndOfLoop(iterationState)

    output.validRows = writePos
    next
  }

  override def addDependency(pipeline: Pipeline): Dependency = Lazy(pipeline)
}
