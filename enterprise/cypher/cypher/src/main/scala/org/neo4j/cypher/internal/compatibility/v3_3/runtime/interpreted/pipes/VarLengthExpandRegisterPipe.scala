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
package org.neo4j.cypher.internal.compatibility.v3_3.runtime.interpreted.pipes

import org.neo4j.cypher.internal.compatibility.v3_3.runtime.interpreted.PrimitiveExecutionContext
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.pipes.{LazyTypes, Pipe, PipeWithSource, QueryState}
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.planDescription.Id
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.{ExecutionContext, PipelineInformation}
import org.neo4j.cypher.internal.frontend.v3_3.{InternalException, SemanticDirection}
import org.neo4j.graphdb.Relationship
import org.neo4j.kernel.impl.api.RelationshipVisitor
import org.neo4j.kernel.impl.api.store.RelationshipIterator
import org.neo4j.values.AnyValues

import scala.collection.mutable

case class VarLengthExpandRegisterPipe(source: Pipe,
                                       fromOffset: Int,
                                       relOffset: Int,
                                       toOffset: Int,
                                       dir: SemanticDirection,
                                       projectedDir: SemanticDirection,
                                       types: LazyTypes,
                                       min: Int,
                                       maxDepth: Option[Int],
                                       shouldExpandAll: Boolean,
                                       filteringStep: VarLengthRegisterPredicate,
                                       pipeline: PipelineInformation)
                                      (val id: Id = new Id) extends PipeWithSource(source) {
  /*  Since Long is used for both edges and nodes, the "why" of these aliases is to make the code a easier to read.*/

  type LNode = Long

  private def varLengthExpand(node: LNode,
                              state: QueryState,
                              row: ExecutionContext): Iterator[(LNode, Seq[Relationship])] = {
    val stack = new mutable.Stack[(LNode, Seq[Relationship])]
    stack.push((node, Seq.empty))

    new Iterator[(LNode, Seq[Relationship])] {
      override def next(): (LNode, Seq[Relationship]) = {
        val (fromNode, rels) = stack.pop()
        if (rels.length < maxDepth.getOrElse(Int.MaxValue) && filteringStep.filterNode(row, state)(fromNode)) {
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
              // TODO: This call creates an intermediate NodeProxy which should not be necessary
              stack.push((relationship.getOtherNodeId(fromNode), rels :+ relationship))
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
        val paths: Iterator[(LNode, Seq[Relationship])] = varLengthExpand(fromNode, state, inputRowWithFromNode)
        paths collect {
          case (toNode: LNode, rels: Seq[Relationship])
            if rels.length >= min && isToNodeValid(inputRowWithFromNode, toNode) =>
            val resultRow = PrimitiveExecutionContext(pipeline)
            resultRow.copyFrom(inputRowWithFromNode, pipeline.initialNumberOfLongs, pipeline.initialNumberOfReferences)
            resultRow.setLongAt(toOffset, toNode)
            resultRow.setRefAt(relOffset, AnyValues.asListOfEdges(rels.toArray))
            resultRow
        }
    }
  }

  private def isToNodeValid(row: ExecutionContext, node: LNode): Boolean =
    shouldExpandAll || row.getLongAt(toOffset) == node
}

trait VarLengthRegisterPredicate {
  def filterNode(row: ExecutionContext, state: QueryState)(node: Long): Boolean

  def filterRelationship(row: ExecutionContext, state: QueryState)(rel: Long): Boolean
}

object VarLengthRegisterPredicate {
  val NONE: VarLengthRegisterPredicate = new VarLengthRegisterPredicate {

    override def filterNode(row: ExecutionContext, state: QueryState)(node: Long): Boolean = true

    override def filterRelationship(row: ExecutionContext, state: QueryState)(rel: Long): Boolean = true
  }
}
