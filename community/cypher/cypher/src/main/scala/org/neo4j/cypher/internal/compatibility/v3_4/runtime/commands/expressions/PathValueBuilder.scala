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
package org.neo4j.cypher.internal.compatibility.v3_4.runtime.commands.expressions

import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual._

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

final class PathValueBuilder {

  private val nodes = ArrayBuffer.empty[NodeValue]
  private val rels = ArrayBuffer.empty[EdgeValue]
  private var nulled = false
  private var previousNode: NodeValue = null
  def result(): AnyValue = if (nulled) Values.NO_VALUE else VirtualValues.path(nodes.toArray, rels.toArray)

  def clear(): PathValueBuilder =  {
    nodes.clear()
    rels.clear()
    nulled = false
    this
  }

  def addNode(node: NodeValue): PathValueBuilder = nullCheck(node) {
    previousNode = node
    nodes += node
    this
  }

  def addIncomingRelationship(rel: EdgeValue): PathValueBuilder = nullCheck(rel) {
    rels += rel
    previousNode = rel.startNode()
    nodes += previousNode
    this
  }

  def addOutgoingRelationship(rel: EdgeValue): PathValueBuilder = nullCheck(rel) {
    rels += rel
    previousNode = rel.endNode()
    nodes += previousNode
    this
  }

  def addUndirectedRelationship(rel: EdgeValue): PathValueBuilder = nullCheck(rel) {
    if (rel.startNode() == previousNode) addOutgoingRelationship(rel)
    else if (rel.endNode() == previousNode) addIncomingRelationship(rel)
    else throw new IllegalArgumentException(s"Invalid usage of PathValueBuilder, $previousNode must be a node in $rel")
  }

  def addIncomingRelationships(rels: ListValue): PathValueBuilder = nullCheck(rels) {
    val iterator = rels.iterator
    while (iterator.hasNext)
      addIncomingRelationship(iterator.next().asInstanceOf[EdgeValue])
    this
  }

  def addOutgoingRelationships(rels: ListValue): PathValueBuilder = nullCheck(rels) {
    val iterator = rels.iterator
    while (iterator.hasNext)
      addOutgoingRelationship(iterator.next().asInstanceOf[EdgeValue])
    this
  }

  def addUndirectedRelationships(rels: ListValue): PathValueBuilder = nullCheck(rels) {
    val relIterator = rels.iterator

    def consumeIterator(i: Iterator[AnyValue]) =
      while (i.hasNext)
        addUndirectedRelationship(i.next().asInstanceOf[EdgeValue])


    if (relIterator.hasNext) {
      val first = relIterator.next().asInstanceOf[EdgeValue]
      val rightDirection = first.startNode() == previousNode || first.endNode() == previousNode

      if (rightDirection) {
        addUndirectedRelationship(first)
        consumeIterator(relIterator.asScala)
      } else {
        consumeIterator(relIterator.asScala.toIndexedSeq.reverseIterator)
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
