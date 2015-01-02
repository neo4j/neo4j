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
package org.neo4j.cypher.internal.compiler.v2_2.commands.expressions

import org.neo4j.cypher.internal.PathImpl
import org.neo4j.graphdb.{Node, PropertyContainer, Relationship}

final class PathValueBuilder {
  private val builder = Vector.newBuilder[PropertyContainer]
  private var nulled = false

  def result(): PathImpl = if (nulled) null else new PathImpl(builder.result(): _*)

  def clear(): PathValueBuilder =  {
    builder.clear()
    nulled = false
    this
  }

  def addNode(node: Node): PathValueBuilder = nullCheck(node) {
      builder += node
      this
  }

  def addIncomingRelationship(rel: Relationship): PathValueBuilder = nullCheck(rel) {
    builder += rel
    builder += rel.getStartNode
    this
  }

  def addOutgoingRelationship(rel: Relationship): PathValueBuilder = nullCheck(rel) {
    builder += rel
    builder += rel.getEndNode
    this
  }

  def addIncomingRelationships(rels: Iterable[Relationship]): PathValueBuilder = nullCheck(rels) {
    val iterator = rels.iterator
    while (iterator.hasNext)
      addIncomingRelationship(iterator.next())
    this
  }

  def addOutgoingRelationships(rels: Iterable[Relationship]): PathValueBuilder = nullCheck(rels) {
    val iterator = rels.iterator
    while (iterator.hasNext)
      addOutgoingRelationship(iterator.next())
    this
  }

  private def nullCheck[A](value: A)(f: => PathValueBuilder):PathValueBuilder = value match {
    case null =>
      nulled = true
      this

    case _ => f
  }

}
