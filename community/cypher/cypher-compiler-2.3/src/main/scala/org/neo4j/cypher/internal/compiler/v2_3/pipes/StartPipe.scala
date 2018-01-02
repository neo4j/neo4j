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

import org.neo4j.cypher.internal.compiler.v2_3._
import org.neo4j.cypher.internal.compiler.v2_3.executionplan.{Effects, _}
import org.neo4j.cypher.internal.frontend.v2_3.symbols._
import org.neo4j.graphdb.{Node, PropertyContainer, Relationship}

sealed abstract class StartPipe[T <: PropertyContainer](source: Pipe,
                                                        name: String,
                                                        createSource: EntityProducer[T],
                                                        pipeMonitor:PipeMonitor) extends PipeWithSource(source, pipeMonitor) with RonjaPipe {
  def identifierType: CypherType

  val symbols = source.symbols.add(name, identifierType)

  protected def internalCreateResults(input: Iterator[ExecutionContext], state: QueryState) = {
    input.flatMap(ctx => {
      val source = createSource(ctx, state)
      source.map(x => {
        ctx.newWith1(name, x)
      })
    })
  }

  def planDescriptionWithoutCardinality =
    source.planDescription
      .andThen(this.id, s"${createSource.producerType}", identifiers, createSource.arguments: _*)
}

case class NodeStartPipe(source: Pipe, name: String, createSource: EntityProducer[Node], itemEffects: Effects = Effects(ReadsAllNodes))(val estimatedCardinality: Option[Double] = None)(implicit pipeMonitor: PipeMonitor)
  extends StartPipe[Node](source, name, createSource, pipeMonitor) {
  def identifierType = CTNode
  override def localEffects = itemEffects

  def withEstimatedCardinality(estimated: Double) = copy()(Some(estimated))

  def dup(sources: List[Pipe]): Pipe = {
    val (head :: Nil) = sources
    copy(source = head)(estimatedCardinality)
  }
}

case class RelationshipStartPipe(source: Pipe, name: String, createSource: EntityProducer[Relationship])(val estimatedCardinality: Option[Double] = None)(implicit pipeMonitor: PipeMonitor)
  extends StartPipe[Relationship](source, name, createSource, pipeMonitor) {
  def identifierType = CTRelationship
  override def localEffects = Effects(ReadsRelationships)

  def withEstimatedCardinality(estimated: Double) = copy()(Some(estimated))

  def dup(sources: List[Pipe]): Pipe = {
    val (head :: Nil) = sources
    copy(source = head)(estimatedCardinality)
  }
}

