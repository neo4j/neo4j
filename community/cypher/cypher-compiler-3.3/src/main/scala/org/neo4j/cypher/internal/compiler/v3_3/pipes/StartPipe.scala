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
package org.neo4j.cypher.internal.compiler.v3_3.pipes

import org.neo4j.cypher.internal.compiler.v3_3._
import org.neo4j.cypher.internal.compiler.v3_3.executionplan.{Effects, _}
import org.neo4j.cypher.internal.compiler.v3_3.planDescription.Id
import org.neo4j.cypher.internal.frontend.v3_2.symbols._
import org.neo4j.graphdb.{Node, PropertyContainer, Relationship}

sealed abstract class StartPipe[T <: PropertyContainer](source: Pipe,
                                                        name: String,
                                                        createSource: EntityProducer[T],
                                                        pipeMonitor:PipeMonitor) extends PipeWithSource(source, pipeMonitor) {
  def variableType: CypherType

  protected def internalCreateResults(input: Iterator[ExecutionContext], state: QueryState) = {
    input.flatMap(ctx => {
      val source = createSource(ctx, state)
      source.map(x => {
        ctx.newWith1(name, x)
      })
    })
  }
}

case class NodeStartPipe(source: Pipe,
                         name: String,
                         createSource: EntityProducer[Node],
                         itemEffects: Effects = Effects(ReadsAllNodes))
                        (val id: Id = new Id)
                        (implicit pipeMonitor: PipeMonitor)
  extends StartPipe[Node](source, name, createSource, pipeMonitor) {
  def variableType = CTNode
}

case class RelationshipStartPipe(source: Pipe, name: String, createSource: EntityProducer[Relationship])
                                (val id: Id = new Id)
                                (implicit pipeMonitor: PipeMonitor)
  extends StartPipe[Relationship](source, name, createSource, pipeMonitor) {
  def variableType = CTRelationship
}

