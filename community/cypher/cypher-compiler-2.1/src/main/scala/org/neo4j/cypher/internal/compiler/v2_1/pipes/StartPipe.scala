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

import org.neo4j.cypher.internal.compiler.v2_1._
import org.neo4j.cypher.internal.compiler.v2_1.executionplan.Effects
import org.neo4j.cypher.internal.compiler.v2_1.planDescription.PlanDescription.Arguments.IntroducedIdentifier
import org.neo4j.cypher.internal.compiler.v2_1.symbols._
import org.neo4j.graphdb.{Node, PropertyContainer, Relationship}

abstract class StartPipe[T <: PropertyContainer](source: Pipe,
                                                 name: String,
                                                 createSource: EntityProducer[T],
                                                 pipeMonitor:PipeMonitor) extends PipeWithSource(source, pipeMonitor) {
  def identifierType: CypherType

  val symbols = source.symbols.add(name, identifierType)

  protected def internalCreateResults(input: Iterator[ExecutionContext], state: QueryState) = {
    input.flatMap(ctx => {
      val source = createSource(ctx, state)
      source.map(x => {
        ctx.newWith(name -> x)
      })
    })
  }

  override def planDescription =
    source.planDescription
      .andThen(this, s"${createSource.producerType}", createSource.arguments :+ IntroducedIdentifier(name):_*)
}

case class NodeStartPipe(source: Pipe, name: String, createSource: EntityProducer[Node])(implicit pipeMonitor: PipeMonitor)
  extends StartPipe[Node](source, name, createSource, pipeMonitor) {
  def identifierType = CTNode
  override def localEffects = Effects.READS_NODES

  def dup(sources: List[Pipe]): Pipe = {
    val (head :: Nil) = sources
    copy(source = head)
  }
}

case class RelationshipStartPipe(source: Pipe, name: String, createSource: EntityProducer[Relationship])(implicit pipeMonitor: PipeMonitor)
  extends StartPipe[Relationship](source, name, createSource, pipeMonitor) {
  def identifierType = CTRelationship
  override def localEffects = Effects.READS_RELATIONSHIPS

  def dup(sources: List[Pipe]): Pipe = {
    val (head :: Nil) = sources
    copy(source = head)
  }
}
