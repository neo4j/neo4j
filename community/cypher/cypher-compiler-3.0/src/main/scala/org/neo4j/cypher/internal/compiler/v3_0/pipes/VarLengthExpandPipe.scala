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
package org.neo4j.cypher.internal.compiler.v3_0.pipes

import org.neo4j.cypher.internal.compiler.v3_0.ExecutionContext
import org.neo4j.cypher.internal.compiler.v3_0.executionplan.{Effects, ReadsAllNodes, ReadsAllRelationships}
import org.neo4j.cypher.internal.compiler.v3_0.planDescription.InternalPlanDescription.Arguments.ExpandExpression
import org.neo4j.cypher.internal.frontend.v3_0.symbols._
import org.neo4j.cypher.internal.frontend.v3_0.{InternalException, SemanticDirection}
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

   trait ContainsRelationship {
    def contains(r : Relationship) : Boolean
  }

  /*
  suggestion from SP
  trait ContainsOps[T[X]] { def contains[X](container: T[X], elt: X): Boolean }

  object SetOps extends ContainsOps[Set] { … }
  object VecOps extends ContainsOps[Vector] { … }

  def yourCode[T <: Iterable[Foo]](container: T, ops: ContainsOps[T]) { … ops.contains(container, elt) … }

  */
  private def varLengthExpand(node: Node, state: QueryState, maxDepth: Option[Int],
                              row: ExecutionContext): Iterator[(Node, Seq[Relationship])] = {
    val maxDepthValue = maxDepth.getOrElse(Int.MaxValue)

    val nodeFilter: (Node) => Boolean = filteringStep.filterNode(row, state)

    val stack = new mutable.ArrayBuffer[(Node, Vector[Relationship])](1024*1024)
    stack.append((node, Vector[Relationship]()))
    val relSet = mutable.HashSet[Relationship]()
    relSet.sizeHint(1024)

    new Iterator[(Node, Vector[Relationship])] {
      def next(): (Node, Vector[Relationship]) = {
        val (node, rels: Vector[Relationship]) = stack.remove(stack.length-1)
        if (rels.length < maxDepthValue && nodeFilter(node)) {
          val relationships: Iterator[Relationship] = state.query
            .getRelationshipsForIds(node, dir, types.types(state.query))
            .filter(filteringStep.filterRelationship(row, state))

          if (relationships.nonEmpty) {
            // todo this code sucks, I need the scala way of unifying Vector.contains and Set.contains
            val set = if (rels.lengthCompare(8) > 0) {
              relSet.clear()
              rels.foreach(relSet.add)
              new ContainsRelationship {
                override def contains(r: Relationship): Boolean = relSet.contains(r)
              }
            } else {
              new ContainsRelationship {
                override def contains(r: Relationship): Boolean = rels.contains(r)
              }
            }

            relationships.foreach { rel =>
              val otherNode = rel.getOtherNode(node)
              if (!set.contains(rel) && nodeFilter(otherNode)) {
                stack.append((otherNode, rels :+ rel))
              }
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
