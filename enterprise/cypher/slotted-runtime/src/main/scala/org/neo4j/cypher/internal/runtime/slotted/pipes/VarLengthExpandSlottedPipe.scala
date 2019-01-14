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
import org.neo4j.cypher.internal.runtime.interpreted.commands.predicates.Predicate
import org.neo4j.cypher.internal.runtime.interpreted.pipes.{LazyTypes, Pipe, PipeWithSource, QueryState}
import org.neo4j.cypher.internal.runtime.slotted.SlottedExecutionContext
import org.neo4j.cypher.internal.runtime.slotted.helpers.NullChecker.entityIsNull
import org.neo4j.cypher.internal.runtime.slotted.helpers.SlottedPipeBuilderUtils.makeGetPrimitiveNodeFromSlotFunctionFor
import org.neo4j.cypher.internal.util.v3_4.InternalException
import org.neo4j.cypher.internal.util.v3_4.attribution.Id
import org.neo4j.cypher.internal.v3_4.expressions.SemanticDirection
import org.neo4j.kernel.impl.api.RelationshipVisitor
import org.neo4j.kernel.impl.api.store.RelationshipIterator
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.{RelationshipValue, VirtualValues}

import scala.collection.mutable

case class VarLengthExpandSlottedPipe(source: Pipe,
                                      fromSlot: Slot,
                                      relOffset: Int,
                                      toSlot: Slot,
                                      dir: SemanticDirection,
                                      projectedDir: SemanticDirection,
                                      types: LazyTypes,
                                      min: Int,
                                      maxDepth: Option[Int],
                                      shouldExpandAll: Boolean,
                                      slots: SlotConfiguration,
                                      tempNodeOffset: Int,
                                      tempEdgeOffset: Int,
                                      nodePredicate: Predicate,
                                      edgePredicate: Predicate,
                                      argumentSize: SlotConfiguration.Size)
                                     (val id: Id = Id.INVALID_ID) extends PipeWithSource(source) {
  type LNode = Long

  //===========================================================================
  // Compile-time initializations
  //===========================================================================
  private val getFromNodeFunction = makeGetPrimitiveNodeFromSlotFunctionFor(fromSlot)
  private val getToNodeFunction =
    if (shouldExpandAll) null // We only need this getter in the ExpanInto case
    else makeGetPrimitiveNodeFromSlotFunctionFor(toSlot)
  private val toOffset = toSlot.offset

  //===========================================================================
  // Runtime code
  //===========================================================================

  private def varLengthExpand(node: LNode,
                              state: QueryState,
                              row: ExecutionContext): Iterator[(LNode, Seq[RelationshipValue])] = {
    val stack = new mutable.Stack[(LNode, Seq[RelationshipValue])]
    stack.push((node, Seq.empty))

    new Iterator[(LNode, Seq[RelationshipValue])] {
      override def next(): (LNode, Seq[RelationshipValue]) = {
        val (fromNode, rels) = stack.pop()
        if (rels.length < maxDepth.getOrElse(Int.MaxValue)) {
          val relationships: RelationshipIterator = state.query.getRelationshipsForIdsPrimitive(fromNode, dir, types.types(state.query))

          var relationship: RelationshipValue = null

          val relVisitor = new RelationshipVisitor[InternalException] {
            override def visit(relationshipId: Long, typeId: Int, startNodeId: LNode, endNodeId: LNode): Unit = {

              relationship = state.query.getRelationshipFor(relationshipId, typeId, startNodeId, endNodeId)
            }
          }

          while (relationships.hasNext) {
            val relId = relationships.next()
            relationships.relationshipVisit(relId, relVisitor)
            val relationshipIsUniqueInPath = !rels.contains(relationship)

            if (relationshipIsUniqueInPath) {
              row.setLongAt(tempEdgeOffset, relId)
              row.setLongAt(tempNodeOffset, relationship.otherNodeId(fromNode))
              // Before expanding, check that both the edge and node in question fulfil the predicate
              if (edgePredicate.isTrue(row, state) && nodePredicate.isTrue(row, state)) {
                // TODO: This call creates an intermediate NodeProxy which should not be necessary
                stack.push((relationship.otherNodeId(fromNode), rels :+ relationship))
              }
            }
          }
        }
        val needsFlipping = if (dir == SemanticDirection.BOTH)
          projectedDir == SemanticDirection.INCOMING
        else
          dir != projectedDir

        val projectedRels = if (needsFlipping)
          rels.reverse
        else
          rels

        (fromNode, projectedRels)
      }

      override def hasNext: Boolean = stack.nonEmpty
    }
  }

  protected def internalCreateResults(input: Iterator[ExecutionContext], state: QueryState): Iterator[ExecutionContext] = {
    input.flatMap {
      inputRow =>
        val fromNode = getFromNodeFunction(inputRow)
        if (entityIsNull(fromNode)) {
          val resultRow = SlottedExecutionContext(slots)
          resultRow.copyFrom(inputRow, argumentSize.nLongs, argumentSize.nReferences)
          resultRow.setRefAt(relOffset, Values.NO_VALUE)
          if (shouldExpandAll)
            resultRow.setLongAt(toOffset, -1L)
          Iterator(resultRow)
        }
        else {
          // We set the fromNode on the temp node offset as well, to be able to run our node predicate and make sure
          // the start node is valid
          inputRow.setLongAt(tempNodeOffset, fromNode)
          if (nodePredicate.isTrue(inputRow, state)) {

            val paths: Iterator[(LNode, Seq[RelationshipValue])] = varLengthExpand(fromNode, state, inputRow)
            paths collect {
              case (toNode: LNode, rels: Seq[RelationshipValue])
                if rels.length >= min && isToNodeValid(inputRow, toNode) =>
                val resultRow = SlottedExecutionContext(slots)
                resultRow.copyFrom(inputRow, argumentSize.nLongs, argumentSize.nReferences)
                if (shouldExpandAll)
                  resultRow.setLongAt(toOffset, toNode)
                resultRow.setRefAt(relOffset, VirtualValues.list(rels.toArray: _*))
                resultRow
            }
          }
          else
            Iterator.empty
        }
    }
  }


  private def isToNodeValid(row: ExecutionContext, node: LNode): Boolean =
    shouldExpandAll || getToNodeFunction(row) == node
}
