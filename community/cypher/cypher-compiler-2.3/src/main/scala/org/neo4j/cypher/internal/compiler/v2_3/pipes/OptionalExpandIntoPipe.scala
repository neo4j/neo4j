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
package org.neo4j.cypher.internal.compiler.v2_3.pipes

import org.neo4j.cypher.internal.compiler.v2_3.ExecutionContext
import org.neo4j.cypher.internal.compiler.v2_3.commands.predicates.Predicate
import org.neo4j.cypher.internal.compiler.v2_3.executionplan.{ReadsAllNodes, Effects, ReadsRelationships}
import org.neo4j.cypher.internal.compiler.v2_3.planDescription.InternalPlanDescription.Arguments.ExpandExpression
import org.neo4j.cypher.internal.frontend.v2_3.{SemanticDirection, InternalException}
import org.neo4j.cypher.internal.frontend.v2_3.symbols._
import org.neo4j.graphdb.Node
import org.neo4j.helpers.collection.PrefetchingIterator

import scala.collection.JavaConverters._

case class OptionalExpandIntoPipe(source: Pipe, fromName: String, relName: String, toName: String,
                                  dir: SemanticDirection, types: LazyTypes, predicate: Predicate)
                                (val estimatedCardinality: Option[Double] = None)(implicit pipeMonitor: PipeMonitor)
  extends PipeWithSource(source, pipeMonitor) with RonjaPipe {

  protected def internalCreateResults(input: Iterator[ExecutionContext], state: QueryState): Iterator[ExecutionContext] = {
    //register as parent so that stats are associated with this pipe
    state.decorator.registerParentPipe(this)

    implicit val s = state

    input.flatMap {
      row =>
        val fromNode = getRowNode(row, fromName)
        fromNode match {
          case n: Node =>
            val relationships = state.query.getRelationshipsForIds(n, dir, types.types(state.query))
            val toNode: Node = getRowNode(row, toName)

            if (toNode == null) {
              Iterator(row.newWith1(relName, null))
            }
            else {
              val matchIterator = new PrefetchingIterator[ExecutionContext] {
                override def fetchNextOrNull(): ExecutionContext = {
                  while (relationships.hasNext) {
                    val rel = relationships.next()
                    val other = rel.getOtherNode(fromNode)
                    if (toNode == other) {
                      val candidateRow = row.newWith2(relName, rel, toName, toNode)
                      if (predicate.isTrue(candidateRow)) {
                        return candidateRow
                      }
                    }
                  }
                  null
                }
              }.asScala

              if (matchIterator.isEmpty)
                Iterator(row.newWith2(relName, null, toName, toNode))
              else
                matchIterator
            }

          case value if value == null =>
            Iterator(row.newWith1(relName, null))

          case value =>
            throw new InternalException(s"Expected to find a node at $fromName but found $value instead")
        }
    }
  }

  @inline
  private def getRowNode(row: ExecutionContext, col: String): Node = {
    row.getOrElse(col, throw new InternalException(s"Expected to find a node at $col but found nothing")) match {
      case n: Node => n
      case null    => null
      case value   => throw new InternalException(s"Expected to find a node at $col but found $value instead")
    }
  }

  def planDescriptionWithoutCardinality =
    source.planDescription.
      andThen(this.id, "OptionalExpand(Into)", identifiers, ExpandExpression(fromName, relName, types.names, toName, dir))

  def symbols = source.symbols.add(toName, CTNode).add(relName, CTRelationship)

  def dup(sources: List[Pipe]): Pipe = {
    val (head :: Nil) = sources
    copy(source = head)(estimatedCardinality)
  }

  override def localEffects = predicate.effects(symbols) | Effects(ReadsAllNodes, ReadsRelationships)

  def withEstimatedCardinality(estimated: Double) = copy()(Some(estimated))
}
