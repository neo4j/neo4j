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
package org.neo4j.cypher.internal.compiler.v2_3.commands.expressions

import org.neo4j.graphdb.{Node, PropertyContainer, Relationship}
import org.neo4j.helpers.ThisShouldNotHappenError

final class PathValueBuilder {
  private val builder = Vector.newBuilder[PropertyContainer]
  private var nulled = false
  private var previousNode: Node = null
  def result(): PathImpl = if (nulled) null else new PathImpl(builder.result(): _*)

  def clear(): PathValueBuilder =  {
    builder.clear()
    nulled = false
    this
  }

  def addNode(node: Node): PathValueBuilder = nullCheck(node) {
    previousNode = node
    builder += node
    this
  }

  def addIncomingRelationship(rel: Relationship): PathValueBuilder = nullCheck(rel) {
    builder += rel
    previousNode = rel.getStartNode
    builder += previousNode
    this
  }

  def addOutgoingRelationship(rel: Relationship): PathValueBuilder = nullCheck(rel) {
    builder += rel
    previousNode = rel.getEndNode
    builder += previousNode
    this
  }

  def addUndirectedRelationship(rel: Relationship): PathValueBuilder = nullCheck(rel) {
    if (rel.getStartNode == previousNode) addOutgoingRelationship(rel)
    else if (rel.getEndNode == previousNode) addIncomingRelationship(rel)
    else throw new ThisShouldNotHappenError("pontus", s"Invalid usage of PathValueBuilder, $previousNode must be a node in $rel")
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

  def addUndirectedRelationships(rels: Iterable[Relationship]): PathValueBuilder = nullCheck(rels) {
    val relIterator = rels.iterator

    def consumeIterator(i: Iterator[Relationship]) =
      while (i.hasNext)
        addUndirectedRelationship(i.next())


    if (relIterator.hasNext) {
      val first = relIterator.next()
      val rightDirection = first.getStartNode == previousNode || first.getEndNode == previousNode

      if (rightDirection) {
        addUndirectedRelationship(first)
        consumeIterator(relIterator)
      } else {
        consumeIterator(relIterator.toSeq.reverseIterator)
        addUndirectedRelationship(first)
      }
    }
    this
  }

  private def nullCheck[A](value: A)(f: => PathValueBuilder):PathValueBuilder = value match {
    case null =>
      nulled = true
      this

    case _ => f
  }
}
