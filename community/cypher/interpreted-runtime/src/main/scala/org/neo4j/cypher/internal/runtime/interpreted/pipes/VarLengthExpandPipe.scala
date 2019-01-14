/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.runtime.interpreted.pipes

import org.neo4j.cypher.internal.runtime.interpreted.ExecutionContext
import org.neo4j.cypher.internal.util.v3_4.InternalException
import org.neo4j.cypher.internal.util.v3_4.attribution.Id
import org.neo4j.cypher.internal.v3_4.expressions.SemanticDirection
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual._

import scala.collection.mutable

trait VarLengthPredicate {
  def filterNode(row: ExecutionContext, state:QueryState)(node: NodeValue): Boolean
  def filterRelationship(row: ExecutionContext, state:QueryState)(rel: RelationshipValue): Boolean
}

object VarLengthPredicate {

  val NONE: VarLengthPredicate = new VarLengthPredicate {

    override def filterNode(row: ExecutionContext, state:QueryState)(node: NodeValue): Boolean = true

    override def filterRelationship(row: ExecutionContext, state:QueryState)(rel: RelationshipValue): Boolean = true
  }
}
case class VarLengthExpandPipe(source: Pipe,
                               fromName: String,
                               relName: String,
                               toName: String,
                               dir: SemanticDirection,
                               projectedDir: SemanticDirection,
                               types: LazyTypes,
                               min: Int,
                               max: Option[Int],
                               nodeInScope: Boolean,
                               filteringStep: VarLengthPredicate= VarLengthPredicate.NONE)
                              (val id: Id = Id.INVALID_ID) extends PipeWithSource(source) {
  private def varLengthExpand(node: NodeValue, state: QueryState, maxDepth: Option[Int],
                              row: ExecutionContext): Iterator[(NodeValue, Seq[RelationshipValue])] = {
    val stack = new mutable.Stack[(NodeValue, Seq[RelationshipValue])]
    stack.push((node, Seq.empty))

    new Iterator[(NodeValue, Seq[RelationshipValue])] {
      def next(): (NodeValue, Seq[RelationshipValue]) = {
        val (node, rels) = stack.pop()
        if (rels.length < maxDepth.getOrElse(Int.MaxValue) && filteringStep.filterNode(row,state)(node)) {
          val relationships: Iterator[RelationshipValue] = state.query.getRelationshipsForIds(node.id(), dir,
                                                                                      types.types(state.query))

          relationships.filter(filteringStep.filterRelationship(row, state)).foreach { rel =>
            val otherNode = rel.otherNode(node)
            if (!rels.contains(rel) && filteringStep.filterNode(row,state)(otherNode)) {
              stack.push((otherNode, rels :+ rel))
            }
          }
        }
        val needsFlipping = if (dir == SemanticDirection.BOTH) projectedDir == SemanticDirection.INCOMING else dir != projectedDir
        val projectedRels = if (needsFlipping) {
          rels.reverse
        } else {
          rels
        }
        (node, projectedRels)
      }

      def hasNext: Boolean = stack.nonEmpty
    }
  }

  protected def internalCreateResults(input: Iterator[ExecutionContext], state: QueryState): Iterator[ExecutionContext] = {
    def expand(row: ExecutionContext, n: NodeValue) = {
      val paths = varLengthExpand(n, state, max, row)
      paths.collect {
        case (node, rels) if rels.length >= min && isToNodeValid(row, state, node) =>
          executionContextFactory.copyWith(row, relName, VirtualValues.list(rels: _*), toName, node)
      }
    }

    input.flatMap {
      row => {
        fetchFromContext(row, state, fromName) match {
          case node: NodeValue =>
            expand(row, node)

          case nodeRef: NodeReference =>
            val node = state.query.nodeOps.getById(nodeRef.id)
            expand(row, node)

          case Values.NO_VALUE =>
            if (nodeInScope)
              Iterator(row.set(relName, Values.NO_VALUE))
            else
              Iterator(row.set(relName, Values.NO_VALUE, toName, Values.NO_VALUE))

          case value => throw new InternalException(s"Expected to find a node at '$fromName' but found $value instead")
        }
      }
    }
  }

  private def isToNodeValid(row: ExecutionContext, state: QueryState, node: VirtualNodeValue): Boolean =
    !nodeInScope || {
      fetchFromContext(row, state, toName) match {
        case toNode: VirtualNodeValue =>
          toNode.id == node.id
        case _ =>
          false
      }
    }

  def fetchFromContext(row: ExecutionContext, state: QueryState, name: String): Any =
    row.getOrElse(name, throw new InternalException(s"Expected to find a node at '$name' but found nothing"))
}
