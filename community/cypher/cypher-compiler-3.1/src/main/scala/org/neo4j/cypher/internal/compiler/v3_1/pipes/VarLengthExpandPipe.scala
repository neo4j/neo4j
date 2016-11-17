/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v3_1.pipes

import org.neo4j.cypher.internal.compiler.v3_1.ExecutionContext
import org.neo4j.cypher.internal.compiler.v3_1.executionplan.{Effects, ReadsAllNodes, ReadsAllRelationships}
import org.neo4j.cypher.internal.compiler.v3_1.planDescription.InternalPlanDescription.Arguments.ExpandExpression
import org.neo4j.cypher.internal.frontend.v3_1.symbols._
import org.neo4j.cypher.internal.frontend.v3_1.{InternalException, SemanticDirection}
import org.neo4j.graphdb.{Node, Relationship}

import scala.collection.mutable

trait VarlenghtPredicate {
  def filterNode(row: ExecutionContext, state:QueryState)(node: Node): Boolean
  def filterRelationship(row: ExecutionContext, state:QueryState)(rel: Relationship): Boolean
}

object VarlenghtPredicate {

  val NONE = new VarlenghtPredicate {

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
                               filteringStep: VarlenghtPredicate = VarlenghtPredicate.NONE)
                              (val estimatedCardinality: Option[Double] = None)
                              (implicit pipeMonitor: PipeMonitor) extends PipeWithSource(source, pipeMonitor) with RonjaPipe {

  private def varLengthExpand(node: Node, state: QueryState, maxDepth: Option[Int],
                              row: ExecutionContext): Iterator[(Node, Seq[Relationship])] = {
    val stack = new mutable.Stack[(Node, Seq[Relationship])]
    stack.push((node, Seq.empty))

    new Iterator[(Node, Seq[Relationship])] {
      def next(): (Node, Seq[Relationship]) = {
        val (node, rels) = stack.pop()
        if (rels.length < maxDepth.getOrElse(Int.MaxValue) && filteringStep.filterNode(row,state)(node)) {
          val relationships: Iterator[Relationship] = state.query.getRelationshipsForIds(node, dir, types.types(state.query))

          relationships.filter(filteringStep.filterRelationship(row, state)).foreach { rel =>
            val otherNode = rel.getOtherNode(node)
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
    //register as parent so that stats are associated with this pipe
    state.decorator.registerParentPipe(this)

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

  def planDescriptionWithoutCardinality = {
    val expandExpr = ExpandExpression(fromName, relName, types.names, toName, dir, minLength = min, maxLength = max)
    source.planDescription.
      andThen(this.id, s"VarLengthExpand(${if (nodeInScope) "Into" else "All"})", variables, expandExpr)
  }

  def symbols = source.symbols.add(toName, CTNode).add(relName, CTList(CTRelationship))

  override def localEffects = Effects(ReadsAllNodes, ReadsAllRelationships)

  def dup(sources: List[Pipe]): Pipe = {
    val (head :: Nil) = sources
    copy(head)(estimatedCardinality)
  }

  def withEstimatedCardinality(estimated: Double) = copy()(Some(estimated))

}
