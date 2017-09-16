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
package org.neo4j.cypher.internal.compatibility.v3_3.runtime.slotted.pipes

import org.neo4j.cypher.internal.compatibility.v3_3.runtime.commands.predicates.Predicate
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.pipes.{LazyTypes, Pipe, PipeWithSource, QueryState}
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.slotted.PrimitiveExecutionContext
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.{ExecutionContext, PipelineInformation}
import org.neo4j.cypher.internal.v3_3.logical.plans.LogicalPlanId
import org.neo4j.cypher.internal.frontend.v3_3.{InternalException, SemanticDirection}
import org.neo4j.graphdb.Relationship
import org.neo4j.helpers.ValueUtils
import org.neo4j.kernel.impl.api.RelationshipVisitor
import org.neo4j.kernel.impl.api.store.RelationshipIterator

import scala.collection.mutable

case class VarLengthExpandSlottedPipe(source: Pipe,
                                      fromOffset: Int,
                                      relOffset: Int,
                                      toOffset: Int,
                                      dir: SemanticDirection,
                                      projectedDir: SemanticDirection,
                                      types: LazyTypes,
                                      min: Int,
                                      maxDepth: Option[Int],
                                      shouldExpandAll: Boolean,
                                      pipeline: PipelineInformation,
                                      tempNodeOffset: Int,
                                      tempEdgeOffset: Int,
                                      nodePredicate: Predicate,
                                      edgePredicate: Predicate,
                                      longsToCopy: Int)
                                     (val id: LogicalPlanId = LogicalPlanId.DEFAULT) extends PipeWithSource(source) {
  type LNode = Long

  private def varLengthExpand(node: LNode,
                              state: QueryState,
                              row: ExecutionContext): Iterator[(LNode, Seq[Relationship])] = {
    val stack = new mutable.Stack[(LNode, Seq[Relationship])]
    stack.push((node, Seq.empty))

    new Iterator[(LNode, Seq[Relationship])] {
      override def next(): (LNode, Seq[Relationship]) = {
        val (fromNode, rels) = stack.pop()
        if (rels.length < maxDepth.getOrElse(Int.MaxValue)) {
          val relationships: RelationshipIterator = state.query.getRelationshipsForIdsPrimitive(fromNode, dir, types.types(state.query))

          var relationship: Relationship = null

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
              row.setLongAt(tempNodeOffset, relationship.getOtherNodeId(fromNode))
              // Before expanding, check that both the edge and node in question fulfil the predicate
              if (edgePredicate.isTrue(row, state) && nodePredicate.isTrue(row, state)) {
                // TODO: This call creates an intermediate NodeProxy which should not be necessary
                stack.push((relationship.getOtherNodeId(fromNode), rels :+ relationship))
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
      inputRowWithFromNode =>
        val fromNode = inputRowWithFromNode.getLongAt(fromOffset)

        // We set the fromNode on the temp node offset as well, to be able to run our node predicate and make sure
        // the start node is valid
        inputRowWithFromNode.setLongAt(tempNodeOffset, fromNode)
        if (nodePredicate.isTrue(inputRowWithFromNode, state)) {

          val paths: Iterator[(LNode, Seq[Relationship])] = varLengthExpand(fromNode, state, inputRowWithFromNode)
          paths collect {
            case (toNode: LNode, rels: Seq[Relationship])
              if rels.length >= min && isToNodeValid(inputRowWithFromNode, toNode) =>
              val resultRow = PrimitiveExecutionContext(pipeline)
              resultRow.copyFrom(inputRowWithFromNode, longsToCopy, pipeline .initialNumberOfReferences)
              resultRow.setLongAt(toOffset, toNode)
              resultRow.setRefAt(relOffset, ValueUtils.asListOfEdges(rels.toArray))
              resultRow
          }
        }
        else
          Iterator.empty
    }
  }


  private def isToNodeValid(row: ExecutionContext, node: LNode): Boolean =
    shouldExpandAll || row.getLongAt(toOffset) == node
}
