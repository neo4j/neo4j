/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.compatibility.v3_3.runtime.pipes

import org.neo4j.cypher.internal.compatibility.v3_3.runtime.ExecutionContext
import org.neo4j.cypher.internal.frontend.v3_3.{InternalException, SemanticDirection}
import org.neo4j.cypher.internal.v3_3.logical.plans.LogicalPlanId
import org.neo4j.kernel.impl.util.ValueUtils
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.{EdgeValue, NodeValue, VirtualValues}

trait VarLengthPredicate {
  def filterNode(row: ExecutionContext, state:QueryState)(node: NodeValue): Boolean
  def filterRelationship(row: ExecutionContext, state:QueryState)(rel: EdgeValue): Boolean
}

object VarLengthPredicate {

  val NONE: VarLengthPredicate = new VarLengthPredicate {

    override def filterNode(row: ExecutionContext, state:QueryState)(node: NodeValue): Boolean = true

    override def filterRelationship(row: ExecutionContext, state:QueryState)(rel: EdgeValue): Boolean = true
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
                              (val id: LogicalPlanId = LogicalPlanId.DEFAULT) extends PipeWithSource(source) {
  private def varLengthExpand(node: NodeValue, state: QueryState, maxDepth: Option[Int],
                              row: ExecutionContext): Iterator[(NodeValue, Seq[EdgeValue])] = {
    var stack: List[(NodeValue, Seq[EdgeValue])] = Nil
    def push(item: (NodeValue, Seq[EdgeValue])) {stack = item :: stack}
    def pop(): (NodeValue, Seq[EdgeValue]) = {
      val item = stack.head
      stack = stack.tail
      item
    }
    push((node, Seq.empty))

    new Iterator[(NodeValue, Seq[EdgeValue])] {
      def next(): (NodeValue, Seq[EdgeValue]) = {
        val (node, rels) = pop()
        if (rels.length < maxDepth.getOrElse(Int.MaxValue) && filteringStep.filterNode(row,state)(node)) {
          val relationships: Iterator[EdgeValue] = state.query.getRelationshipsForIds(node.id(), dir,
                                                                                      types.types(state.query)).map(ValueUtils.fromRelationshipProxy)

          relationships.filter(filteringStep.filterRelationship(row, state)).foreach { rel =>
            val otherNode = rel.otherNode(node)
            if (!rels.contains(rel) && filteringStep.filterNode(row,state)(otherNode)) {
              push((otherNode, rels :+ rel))
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
    input.flatMap {
      row => {
        fetchFromContext(row, fromName) match {
          case n: NodeValue =>
            val paths = varLengthExpand(n, state, max, row)
            paths.collect {
              case (node, rels) if rels.length >= min && isToNodeValid(row, node) =>
                row.newWith2(relName, VirtualValues.list(rels:_*), toName, node)
            }

          case Values.NO_VALUE => Iterator(row.newWith2(relName, Values.NO_VALUE, toName, Values.NO_VALUE))

          case value => throw new InternalException(s"Expected to find a node at $fromName but found $value instead")
        }
      }
    }
  }

  private def isToNodeValid(row: ExecutionContext, node: Any): Boolean =
    !nodeInScope || fetchFromContext(row, toName) == node

  def fetchFromContext(row: ExecutionContext, name: String): Any =
    row.getOrElse(name, throw new InternalException(s"Expected to find a node at $name but found nothing"))
}
