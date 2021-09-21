/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.runtime.interpreted.commands.expressions

import org.eclipse.collections.impl.factory.primitive.LongLists
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.exceptions.EntityNotFoundException
import org.neo4j.internal.kernel.api.RelationshipScanCursor
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.ListValue
import org.neo4j.values.virtual.VirtualNodeValue
import org.neo4j.values.virtual.VirtualRelationshipValue
import org.neo4j.values.virtual.VirtualValues

import scala.collection.JavaConverters.asScalaIteratorConverter

final class PathValueBuilder(state: QueryState) {
  private val nodes = LongLists.mutable.empty
  private val rels = LongLists.mutable.empty
  private var nulled = false

  def result(): AnyValue = if (nulled) Values.NO_VALUE else VirtualValues.pathReference(nodes.toArray, rels.toArray)

  def clear(): PathValueBuilder =  {
    nodes.clear()
    rels.clear()
    nulled = false
    this
  }

  def previousNode: Long = nodes.getLast

  def addNoValue(): PathValueBuilder = {
    nulled = true
    this
  }

  def addNode(nodeOrNull: AnyValue): PathValueBuilder = nullCheck(nodeOrNull) {
    val node = nodeOrNull.asInstanceOf[VirtualNodeValue]
    nodes.add(node.id())
    this
  }

  def addRelationship(relOrNull: AnyValue): PathValueBuilder = nullCheck(relOrNull) {
    addRelationship(relOrNull.asInstanceOf[VirtualRelationshipValue])
  }

  def addRelationship(rel: VirtualRelationshipValue): PathValueBuilder  = {
    rels.add(rel.id())
    this
  }

  def addIncomingRelationship(relOrNull: AnyValue): PathValueBuilder = nullCheck(relOrNull) {
    val rel = relOrNull.asInstanceOf[VirtualRelationshipValue]
    rels.add(rel.id())
    nodes.add(startNode(rel))
    this
  }

  def addOutgoingRelationship(relOrNull: AnyValue): PathValueBuilder = nullCheck(relOrNull) {
    val rel = relOrNull.asInstanceOf[VirtualRelationshipValue]
    rels.add(rel.id())
    nodes.add(endNode(rel))
    this
  }

  def addUndirectedRelationship(relOrNull: AnyValue): PathValueBuilder = nullCheck(relOrNull) {
    val rel = relOrNull.asInstanceOf[VirtualRelationshipValue]
    val previousNode = nodes.getLast
    val (start, end) = startAndEndNode(rel)
    if (start == previousNode) addOutgoingRelationship(rel)
    else if (end == previousNode) addIncomingRelationship(rel)
    else throw new IllegalArgumentException(s"Invalid usage of PathValueBuilder, $previousNode must be a node in $rel")
  }

  def addIncomingRelationships(relsOrNull: AnyValue): PathValueBuilder = nullCheck(relsOrNull) {
    val relsToAdd = relsOrNull.asInstanceOf[ListValue]
    val iterator = relsToAdd.iterator
    while (iterator.hasNext)
      addIncomingRelationship(iterator.next().asInstanceOf[VirtualRelationshipValue])
    this
  }

  def addOutgoingRelationships(relsOrNull: AnyValue): PathValueBuilder = nullCheck(relsOrNull) {
    val relsToAdd = relsOrNull.asInstanceOf[ListValue]
    val iterator = relsToAdd.iterator
    while (iterator.hasNext)
      addOutgoingRelationship(iterator.next().asInstanceOf[VirtualRelationshipValue])
    this
  }

  def addUndirectedRelationships(relsOrNull: AnyValue): PathValueBuilder = nullCheck(relsOrNull) {
    val relsToAdd = relsOrNull.asInstanceOf[ListValue]
    val relIterator = relsToAdd.iterator

    def consumeIterator(i: Iterator[AnyValue]): Unit =
      while (i.hasNext)
        addUndirectedRelationship(i.next().asInstanceOf[VirtualRelationshipValue])


    if (relIterator.hasNext) {
      val first = relIterator.next().asInstanceOf[VirtualRelationshipValue]
      val previousNode = nodes.getLast
      if (correctDirection(first, previousNode)) {
        addUndirectedRelationship(first)
        consumeIterator(relIterator.asScala)
      } else {
        consumeIterator(relIterator.asScala.toIndexedSeq.reverseIterator)
        addUndirectedRelationship(first)
      }
    }
    this
  }

  def correctDirection(rel: VirtualRelationshipValue, prevId: Long): Boolean = {
    val (start, end) = startAndEndNode(rel)
    start == prevId ||end == prevId
  }

  private def startNode(rel: VirtualRelationshipValue): Long = {
    val cursor = state.cursors.relationshipScanCursor
    singleRelationship(rel, cursor)
    cursor.sourceNodeReference()
  }

  private def endNode(rel: VirtualRelationshipValue): Long = {
    val cursor = state.cursors.relationshipScanCursor
    singleRelationship(rel, cursor)
    cursor.targetNodeReference()
  }

  private def startAndEndNode(rel: VirtualRelationshipValue): (Long, Long) = {
    val cursor = state.cursors.relationshipScanCursor
    singleRelationship(rel, cursor)
    (cursor.sourceNodeReference(), cursor.targetNodeReference())
  }

  private def singleRelationship(rel: VirtualRelationshipValue, cursor: RelationshipScanCursor): Unit = {
    state.query.singleRelationship(rel.id(), cursor)
    if (!cursor.next()) {
      throw new EntityNotFoundException(s"Relationship with id=${rel.id()}has been deleted in this transaction")
    }
  }

  private def nullCheck[A <: AnyRef](value: A)(f: => PathValueBuilder):PathValueBuilder = value match {
    case x if (x == null) || (Values.NO_VALUE eq x) =>
      nulled = true
      this

    case _ => f
  }
}
