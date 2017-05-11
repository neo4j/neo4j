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
package org.neo4j.cypher.internal.compiler.v3_2.pipes

import org.neo4j.cypher.internal.compiler.v3_2.ExecutionContext
import org.neo4j.cypher.internal.compiler.v3_2.planDescription.Id
import org.neo4j.cypher.internal.frontend.v3_2.{InternalException, SemanticDirection}
import org.neo4j.graphdb.{Node, Relationship}

import scala.collection.mutable

trait VarLengthPredicate {
  def filterNode(row: ExecutionContext, state:QueryState)(node: Node): Boolean
  def filterRelationship(row: ExecutionContext, state:QueryState)(rel: Relationship): Boolean
}

object VarLengthPredicate {

  val NONE = new VarLengthPredicate {

    override def filterNode(row: ExecutionContext, state:QueryState)(node: Node): Boolean = true

    override def filterRelationship(row: ExecutionContext, state:QueryState)(rel: Relationship): Boolean = true
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
                              (val id: Id = new Id)
                              (implicit pipeMonitor: PipeMonitor) extends PipeWithSource(source, pipeMonitor) {
  private def varLengthExpand(node: Node, state: QueryState, maxDepth: Option[Int],
                              row: ExecutionContext): Iterator[(Node, Seq[Relationship])] = {
    val stack = new mutable.Stack[(Node, Seq[Relationship])]
    stack.push((node, Seq.empty))

    new Iterator[(Node, Seq[Relationship])] {
      def next(): (Node, Seq[Relationship]) = {
        val (node, rels) = stack.pop()
        if (rels.length < maxDepth.getOrElse(Int.MaxValue) && filteringStep.filterNode(row,state)(node)) {
          val relationships = state.query.getRelationshipsForIds(node, dir, types.types(state.query))
          try
          relationships.filter(filteringStep.filterRelationship(row, state)).foreach { rel =>
            val otherNode = rel.getOtherNode(node)
            if (!rels.contains(rel) && filteringStep.filterNode(row,state)(otherNode)) {
              stack.push((otherNode, rels :+ rel))
            }
          }
          finally relationships.close()
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
          case n: Node =>
            val paths = varLengthExpand(n, state, max, row)
            paths.collect {
              case (node, rels) if rels.length >= min && isToNodeValid(row, node) =>
                row.newWith2(relName, rels, toName, node)
            }

          case null => Iterator(row.newWith2(relName, null, toName, null))

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
