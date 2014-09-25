/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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

import org.neo4j.cypher.InternalException
import org.neo4j.cypher.internal.compiler.v2_2.ExecutionContext
import org.neo4j.cypher.internal.compiler.v2_2.executionplan.Effects
import org.neo4j.cypher.internal.compiler.v2_2.planDescription.PlanDescription.Arguments.IntroducedIdentifier
import org.neo4j.cypher.internal.compiler.v2_2.spi.QueryContext
import org.neo4j.cypher.internal.compiler.v2_2.symbols._
import org.neo4j.graphdb.{Direction, Node, Relationship}

case class ExpandPipe(source: Pipe, fromNode: String, relName: String, toNode: String, dir: Direction, types: Seq[String])
                     (val estimatedCardinality: Option[Long] = None)
                     (implicit pipeMonitor: PipeMonitor) extends PipeWithSource(source, pipeMonitor) with RonjaPipe {
  protected def internalCreateResults(input: Iterator[ExecutionContext], state: QueryState): Iterator[ExecutionContext] =
    new ExpandIterator(input, state.query)

  def planDescription = {
    val arguments = Seq(IntroducedIdentifier(relName), IntroducedIdentifier(toNode))
    source.planDescription.andThen(this, "Expand", arguments:_*)
  }

  val symbols = source.symbols.add(toNode, CTNode).add(relName, CTRelationship)

  override def localEffects = Effects.READS_ENTITIES

  def setEstimatedCardinality(estimated: Long) = copy()(Some(estimated))

  class ExpandIterator(input: Iterator[ExecutionContext], query: QueryContext) extends Iterator[ExecutionContext] {
    var row: ExecutionContext = null
    var node: Node = null
    var relationships: Iterator[Relationship] = Iterator.empty

    computeNextRelationships()

    def hasNext: Boolean = relationships.hasNext

    def next(): ExecutionContext = {
      val r = relationships.next()
      val result = row.newWith2(relName, r, toNode, r.getOtherNode(node))
      if (!relationships.hasNext)
        computeNextRelationships()
      result
    }

    def computeNextRelationships(): Unit = {
      while (input.hasNext) {
        row = input.next()
        val value = row(fromNode)
        if (value != null) {
          value match {
            case aNode: Node =>
              node = aNode
              relationships = query.getRelationshipsFor(node, dir, types)
              if (relationships.hasNext)
                return
            case _ =>
              throw new InternalException(s"Expected to find a node at $fromNode but found $value instead")
          }
        }
      }
      relationships = Iterator.empty
    }
  }
}
