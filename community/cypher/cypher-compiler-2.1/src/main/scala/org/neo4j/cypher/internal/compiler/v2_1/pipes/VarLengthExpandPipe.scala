/**
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
package org.neo4j.cypher.internal.compiler.v2_1.pipes

import org.neo4j.cypher.InternalException
import org.neo4j.cypher.internal.compiler.v2_1.ExecutionContext
import org.neo4j.cypher.internal.compiler.v2_1.executionplan.Effects
import org.neo4j.cypher.internal.compiler.v2_1.planDescription.PlanDescription.Arguments.IntroducedIdentifier
import org.neo4j.cypher.internal.compiler.v2_1.symbols._
import org.neo4j.graphdb.{Direction, Node, Relationship}

import scala.collection.mutable

case class VarLengthExpandPipe(source: Pipe, fromName: String, relName: String, toName: String, dir: Direction,
                               types: Seq[String], min: Int, max: Option[Int])(implicit pipeMonitor: PipeMonitor)
  extends PipeWithSource(source, pipeMonitor) {

  private def varLengthExpand(node: Node, state: QueryState, maxDepth: Option[Int]): Iterator[(Node, Seq[Relationship])] = {
    val stack = new mutable.Stack[(Node, Seq[Relationship])]
    stack.push((node, Seq.empty))

    new Iterator[(Node, Seq[Relationship])] {
      def next(): (Node, Seq[Relationship]) = {
        val (node, rels) = stack.pop()
        if (rels.length < maxDepth.getOrElse(Int.MaxValue)) {
          val relationships: Iterator[Relationship] = state.query.getRelationshipsFor(node, dir, types)
          relationships.foreach { rel =>
            val otherNode = rel.getOtherNode(node)
            if (!rels.contains(rel)) {
              stack.push((otherNode, rels :+ rel))
            }
          }
        }
        (node, rels)
      }

      def hasNext: Boolean = stack.nonEmpty
    }
  }

  protected def internalCreateResults(input: Iterator[ExecutionContext], state: QueryState): Iterator[ExecutionContext] = {
    input.flatMap {
      row => {
        val fromNode: Any = getFromNode(row)
        fromNode match {
          case n: Node =>
            val paths = varLengthExpand(n, state, max)
            paths.collect {
              case (node, rels) if rels.length >= min =>
                row.newWith(Seq(relName -> rels, toName -> node))
            }

          case value => throw new InternalException(s"Expected to find a node at $fromName but found $value instead")
        }
      }
    }
  }

  def getFromNode(row: ExecutionContext): Any =
    row.getOrElse(fromName, throw new InternalException(s"Expected to find a node at $fromName but found nothing"))

  def planDescription = source.planDescription.
    andThen(this, "Var length expand",
      IntroducedIdentifier(fromName),
      IntroducedIdentifier(toName),
      IntroducedIdentifier(relName))

  def symbols = source.symbols.add(toName, CTNode).add(relName, CTRelationship)

  def dup(sources: List[Pipe]): Pipe = {
    val (head :: Nil) = sources
    copy(head)
  }

  override def localEffects = Effects.READS_ENTITIES
}
