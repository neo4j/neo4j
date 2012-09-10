/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package org.neo4j.cypher.internal.pipes

import org.neo4j.graphdb.{Relationship, Node, PropertyContainer}
import java.lang.String
import org.neo4j.cypher.internal.symbols._
import collection.{Traversable, Iterable}

abstract class StartPipe[T <: PropertyContainer](inner: Pipe, name: String, createSource: ExecutionContext => Iterable[T]) extends Pipe {
  def this(inner: Pipe, name: String, sourceIterable: Iterable[T]) = this (inner, name, m => sourceIterable)

  def identifierType: CypherType

//  val symbols = inner.symbols.add(Identifier(name, identifierType))
  val symbols = inner.symbols.add(name, identifierType)

  def createResults(state: QueryState): Traversable[ExecutionContext] = {
    val map = inner.createResults(state).flatMap(ctx => {
      val source: Iterable[T] = createSource(ctx)
      source.map(x => {
        ctx.newWith(name -> x)
      })
    })
    map
  }

  def visibleName: String

  override def executionPlan(): String = inner.executionPlan() + "\r\n" + visibleName + "(" + name + ")"
}

class NodeStartPipe(inner: Pipe, name: String, createSource: ExecutionContext => Iterable[Node])
  extends StartPipe[Node](inner, name, createSource) {
  def identifierType = NodeType()

  def visibleName = "Nodes"
}

class RelationshipStartPipe(inner: Pipe, name: String, createSource: ExecutionContext => Iterable[Relationship])
  extends StartPipe[Relationship](inner, name, createSource) {
  def identifierType = RelationshipType()

  def visibleName = "Rels"
}