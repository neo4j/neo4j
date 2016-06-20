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

trait VarLengthPredicate {
  def filterNode(row: ExecutionContext, state: QueryState)(node: Node): Boolean

  def filterRelationship(row: ExecutionContext, state: QueryState)(rel: Relationship): Boolean
}

object VarLengthPredicate {

  val NONE = new VarLengthPredicate {
    override def filterNode(row: ExecutionContext, state: QueryState)(node: Node): Boolean = true

    override def filterRelationship(row: ExecutionContext, state: QueryState)(rel: Relationship): Boolean = true
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
                               filteringStep: VarLengthPredicate = VarLengthPredicate.NONE)
                              (val estimatedCardinality: Option[Double] = None)
                              (implicit pipeMonitor: PipeMonitor) extends PipeWithSource(source, pipeMonitor) with RonjaPipe {

  private val needsFlipping = if (dir == SemanticDirection.BOTH)
    projectedDir == SemanticDirection.INCOMING
  else
    dir != projectedDir

  val stack = new mutable.ArrayBuffer[(Node, Int, List[Relationship])]()
  val relSet = mutable.HashSet[Relationship]()

  private def varLengthExpand(node: Node, state: QueryState, maxDepth: Option[Int],
                              row: ExecutionContext): Iterator[(Node, Seq[Relationship])] = {

    val nodeFilter: (Node) => Boolean = filteringStep.filterNode(row, state)

    if (!nodeFilter(node)) return Iterator((node, List.empty))

    val relationshipFilter: (Relationship) => Boolean = filteringStep.filterRelationship(row, state)

    val maxDepthValue = maxDepth.getOrElse(Int.MaxValue)
    val relTypes = types.types(state.query)

    if (stack.nonEmpty) stack.clear()

    stack.append((node, 0, List.empty))

    new Iterator[(Node, Seq[Relationship])] {
      override def next(): (Node, Seq[Relationship]) = {
        val (node, count, relsFromStack) = stack.remove(stack.length - 1)
        if (count < maxDepthValue) {
          val relationships: Iterator[Relationship] = state.query
            .getRelationshipsForIds(node, dir, relTypes)
            .filter(relationshipFilter)

          if (relationships.nonEmpty) {
            relSet.clear()
            relsFromStack.foreach(relSet.add)

            relationships.foreach { rel =>
              val otherNode = rel.getOtherNode(node)
              if (!relSet.contains(rel) && nodeFilter(otherNode)) {
                stack.append((otherNode, count+1, (rel :: relsFromStack)))
              }
            }
          }
        }
        val projectedRels = if (needsFlipping) {
          relsFromStack
        } else {
          relsFromStack.reverse
        }
        (node, projectedRels)
      }

      override def hasNext: Boolean = stack.nonEmpty
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

  def planDescriptionWithoutCardinality = source.planDescription.
    andThen(this.id, s"VarLengthExpand(${if (nodeInScope) "Into" else "All"})", variables, ExpandExpression(fromName, relName, types.names, toName, projectedDir, min, Some(max)))

  def symbols = source.symbols.add(toName, CTNode).add(relName, CTList(CTRelationship))

  override def localEffects = Effects(ReadsAllNodes, ReadsAllRelationships)

  def dup(sources: List[Pipe]): Pipe = {
    val (head :: Nil) = sources
    copy(head)(estimatedCardinality)
  }

  def withEstimatedCardinality(estimated: Double) = copy()(Some(estimated))

}
