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
import scala.collection.mutable
import org.neo4j.cypher.internal.PathImpl

final class PathBuilder extends mutable.Builder[PropertyContainer, org.neo4j.graphdb.Path] {
  private val builder = Vector.newBuilder[PropertyContainer]
  private var lastNode: Node = null

  def result(): PathImpl =
    if (lastNode == null) PathImpl.empty else new PathImpl(builder.result(): _*)

  def clear() {
    lastNode = null
    builder.clear()
  }

  def +=(elem: PropertyContainer): this.type = {
    elem match {
      case node: Node =>
        builder += node
        lastNode = node
      case rel: Relationship =>
        val otherNode = rel.getOtherNode(lastNode)
        builder += rel
        builder += otherNode
        lastNode = otherNode
    }

    this
  }
}
