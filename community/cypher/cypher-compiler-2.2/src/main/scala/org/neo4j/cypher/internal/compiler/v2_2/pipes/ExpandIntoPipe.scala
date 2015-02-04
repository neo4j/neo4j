/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_2.pipes

import org.neo4j.cypher.internal.compiler.v2_2.executionplan.Effects
import org.neo4j.cypher.internal.compiler.v2_2.planDescription.InternalPlanDescription.Arguments.ExpandExpression
import org.neo4j.cypher.internal.compiler.v2_2.symbols._
import org.neo4j.cypher.internal.compiler.v2_2.{ExecutionContext, InternalException}
import org.neo4j.graphdb.{Direction, Node, Relationship}
import org.neo4j.helpers.collection.PrefetchingIterator
import scala.collection.JavaConverters._

case class ExpandIntoPipe(source: Pipe,
                          fromName: String,
                          relName: String,
                          toName: String,
                          dir: Direction,
                          types: LazyTypes)(val estimatedCardinality: Option[Double] = None)
                         (implicit pipeMonitor: PipeMonitor)
  extends PipeWithSource(source, pipeMonitor) with RonjaPipe {

  self =>

  protected def internalCreateResults(input: Iterator[ExecutionContext], state: QueryState): Iterator[ExecutionContext] = {
    input.flatMap {
      row =>
        val fromNode = getRowNode(row, fromName)
        fromNode match {
          case fromNode: Node =>
            val relationships: Iterator[Relationship] = state.query.getRelationshipsForIds(fromNode, dir, types.types(state.query))
            val toNode: Node = getRowNode(row, toName)

            if (toNode == null)
              Iterator.empty
            else
              new PrefetchingIterator[ExecutionContext] {
                override def fetchNextOrNull(): ExecutionContext = {
                  while (relationships.hasNext) {
                    val rel = relationships.next()
                    val other = rel.getOtherNode(fromNode)
                    if (toNode == other) {
                      return row.newWith2(relName, rel, toName, toNode)
                    }
                  }
                  null
                }
              }.asScala

          case null =>
            Iterator.empty
        }
    }
  }

  def typeNames = types.names

  @inline
  private def getRowNode(row: ExecutionContext, col: String): Node = {
    row.getOrElse(col, throw new InternalException(s"Expected to find a node at $col but found nothing")) match {
      case n: Node => n
      case null    => null
      case value   => throw new InternalException(s"Expected to find a node at $col but found $value instead")
    }
  }

  def planDescription = {
    source.planDescription.andThen(this, "Expand(Into)", identifiers, ExpandExpression(fromName, relName, typeNames, toName, dir))
  }

  val symbols = source.symbols.add(toName, CTNode).add(relName, CTRelationship)

  override def localEffects = Effects.READS_ENTITIES

  def dup(sources: List[Pipe]): Pipe = {
    val (source :: Nil) = sources
    copy(source = source)(estimatedCardinality)
  }

  def withEstimatedCardinality(estimated: Double) = copy()(Some(estimated))
}
