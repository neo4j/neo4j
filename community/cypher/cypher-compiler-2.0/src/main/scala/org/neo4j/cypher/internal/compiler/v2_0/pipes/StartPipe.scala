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
package org.neo4j.cypher.internal.compiler.v2_0.pipes

import org.neo4j.cypher.internal.compiler.v2_0._
import data.SimpleVal._
import symbols._
import org.neo4j.graphdb.{Relationship, Node, PropertyContainer}

abstract class StartPipe[T <: PropertyContainer](source: Pipe, name: String, createSource: EntityProducer[T]) extends PipeWithSource(source) {
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

  override def executionPlanDescription = {
    val description = createSource.description :+ (("identifier" -> fromStr(name)))
    source.executionPlanDescription
      .andThen(this, s"${createSource.producerType}", description: _*)
  }
}

class NodeStartPipe(source: Pipe, name: String, createSource: EntityProducer[Node])
  extends StartPipe[Node](source, name, createSource) {
  def identifierType = CTNode
}

class RelationshipStartPipe(source: Pipe, name: String, createSource: EntityProducer[Relationship])
  extends StartPipe[Relationship](source, name, createSource) {
  def identifierType = CTRelationship
}
