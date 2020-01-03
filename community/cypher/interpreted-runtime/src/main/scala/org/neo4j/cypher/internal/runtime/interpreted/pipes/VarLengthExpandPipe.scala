/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

import org.neo4j.cypher.internal.runtime.interpreted.commands.predicates.Predicate
import org.neo4j.cypher.internal.runtime.interpreted.pipes.VarLengthExpandPipe.projectBackwards
import org.neo4j.cypher.internal.runtime.{ExecutionContext, IsNoValue, RelationshipContainer}
import org.neo4j.cypher.internal.v4_0.expressions.SemanticDirection
import org.neo4j.cypher.internal.v4_0.util.attribution.Id
import org.neo4j.exceptions.InternalException
import org.neo4j.values.virtual._

import scala.collection.mutable

trait VarLengthPredicate {
  def filterNode(row: ExecutionContext, state:QueryState)(node: NodeValue): Boolean
  def filterRelationship(row: ExecutionContext, state:QueryState)(rel: RelationshipValue): Boolean
  def predicateExpressions: Seq[Predicate]
}

object VarLengthPredicate {
  val NONE: VarLengthPredicate = new VarLengthPredicate {
    override def filterNode(row: ExecutionContext, state:QueryState)(node: NodeValue): Boolean = true
    override def filterRelationship(row: ExecutionContext, state:QueryState)(rel: RelationshipValue): Boolean = true
    override def predicateExpressions: Seq[Predicate] = Seq.empty
  }
}

case class VarLengthExpandPipe(source: Pipe,
                               fromName: String,
                               relName: String,
                               toName: String,
                               dir: SemanticDirection,
                               projectedDir: SemanticDirection,
                               types: RelationshipTypes,
                               min: Int,
                               max: Option[Int],
                               nodeInScope: Boolean,
                               filteringStep: VarLengthPredicate = VarLengthPredicate.NONE)
                              (val id: Id = Id.INVALID_ID) extends PipeWithSource(source) {

  filteringStep.predicateExpressions.foreach(_.registerOwningPipe(this))

  private def varLengthExpand(node: NodeValue, state: QueryState, maxDepth: Option[Int],
                              row: ExecutionContext): Iterator[(NodeValue, RelationshipContainer)] = {
    val stack = new mutable.Stack[(NodeValue, RelationshipContainer)]
    stack.push((node, RelationshipContainer.EMPTY))

    new Iterator[(NodeValue, RelationshipContainer)] {
      def next(): (NodeValue, RelationshipContainer) = {
        val (node, rels) = stack.pop()
        if (rels.size < maxDepth.getOrElse(Int.MaxValue) && filteringStep.filterNode(row, state)(node)) {
          val relationships: Iterator[RelationshipValue] = state.query.getRelationshipsForIds(node.id(), dir, types.types(state.query))

          val relationshipValues = relationships.filter(filteringStep.filterRelationship(row, state))
          while (relationshipValues.hasNext) {
            val rel = relationshipValues.next()
            val otherNode = rel.otherNode(node)
            if (!rels.contains(rel) && filteringStep.filterNode(row,state)(otherNode)) {
              stack.push((otherNode, rels.append(rel)))
            }
          }
        }
        val projectedRels =
          if (projectBackwards(dir, projectedDir)) {
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
      if (filteringStep.filterNode(row, state)(n)) {
        val paths = varLengthExpand(n, state, max, row)
        paths.collect {
          case (node, rels) if rels.size >= min && isToNodeValid(row, state, node) =>
            executionContextFactory.copyWith(row, relName, rels.asList, toName, node)
        }
      } else {
        Iterator.empty
      }
    }

    input.flatMap {
      row => {
        row.getByName(fromName) match {
          case node: NodeValue =>
            expand(row, node)

          case nodeRef: NodeReference =>
            val node = state.query.nodeOps.getById(nodeRef.id)
            expand(row, node)

          case IsNoValue() => Iterator.empty
          case value => throw new InternalException(s"Expected to find a node at '$fromName' but found $value instead")
        }
      }
    }
  }

  private def isToNodeValid(row: ExecutionContext, state: QueryState, node: VirtualNodeValue): Boolean =
    !nodeInScope || {
      row.getByName(toName) match {
        case toNode: VirtualNodeValue =>
          toNode.id == node.id
        case _ =>
          false
      }
    }
  }

object VarLengthExpandPipe {
  def projectBackwards(dir: SemanticDirection, projectedDir: SemanticDirection): Boolean =
    if (dir == SemanticDirection.BOTH) {
      projectedDir == SemanticDirection.INCOMING
    } else {
      dir != projectedDir
    }
}
