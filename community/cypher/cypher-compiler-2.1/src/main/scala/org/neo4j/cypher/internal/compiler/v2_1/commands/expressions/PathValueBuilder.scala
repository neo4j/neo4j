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
package org.neo4j.cypher.internal.compiler.v2_1.commands.expressions

import org.neo4j.graphdb.{Relationship, Node, PropertyContainer}
import org.neo4j.cypher.internal.PathImpl

final class PathValueBuilder {
  private val builder = Vector.newBuilder[PropertyContainer]

  def result(): PathImpl = new PathImpl(builder.result(): _*)

  def clear(): this.type =  {
    builder.clear()
    this
  }

  def addNode(node: Node): this.type = {
    builder += node
    this
  }

  def addIncomingRelationship(rel: Relationship): this.type = {
    builder += rel
    builder += rel.getStartNode
    this
  }

  def addOutgoingRelationship(rel: Relationship): this.type = {
    builder += rel
    builder += rel.getEndNode
    this
  }

  def addIncomingRelationships(rels: Iterable[Relationship]): this.type = addIncomingRelationships(rels.iterator)

  def addIncomingRelationships(rels: Iterator[Relationship]): this.type = {
    while (rels.hasNext)
      addIncomingRelationship(rels.next())
    this
  }

  def addOutgoingRelationships(rels: Iterable[Relationship]): this.type = addOutgoingRelationships(rels.iterator)

  def addOutgoingRelationships(rels: Iterator[Relationship]): this.type = {
    while (rels.hasNext)
      addOutgoingRelationship(rels.next())
    this
  }
}
