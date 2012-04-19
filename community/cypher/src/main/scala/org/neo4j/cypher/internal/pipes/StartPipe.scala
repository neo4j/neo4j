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
import org.neo4j.cypher.internal.symbols.{AnyType, NodeType, RelationshipType, Identifier}
import collection.mutable.Map
import collection.{Traversable, Iterable}

abstract class StartPipe[T <: PropertyContainer](inner: Pipe, name: String, createSource: Map[String, Any] => Iterable[T]) extends Pipe {
  def this(inner: Pipe, name: String, sourceIterable: Iterable[T]) = this (inner, name, m => sourceIterable)

  def identifierType: AnyType

  val symbols = inner.symbols.add(Identifier(name, identifierType))


  def createResults[U](params: Map[String, Any]): Traversable[Map[String, Any]] = {
    val map = inner.createResults(params).flatMap(sourceMap => {
      val source: Iterable[T] = createSource(sourceMap)
      source.map(x =>{
        val newMap: Map[String, Any] = sourceMap.clone().asInstanceOf[Map[String, Any]]
        newMap += name -> x
      })
    })
    map
  }

  def visibleName: String

  override def executionPlan(): String = inner.executionPlan() + "\r\n" + visibleName + "(" + name + ")"
}

class NodeStartPipe(inner: Pipe, name: String, createSource: Map[String, Any] => Iterable[Node])
  extends StartPipe[Node](inner, name, createSource) {
  def identifierType = NodeType()

  def visibleName: String = "Nodes"
}

class RelationshipStartPipe(inner: Pipe, name: String, createSource: Map[String, Any] => Iterable[Relationship])
  extends StartPipe[Relationship](inner, name, createSource) {
  def identifierType = RelationshipType()

  def visibleName: String = "Rels"
}