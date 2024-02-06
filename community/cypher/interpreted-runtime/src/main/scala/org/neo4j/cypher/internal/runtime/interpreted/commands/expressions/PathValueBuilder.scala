/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.runtime.interpreted.commands.expressions

import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.ListValue
import org.neo4j.values.virtual.RelationshipVisitor
import org.neo4j.values.virtual.VirtualNodeValue
import org.neo4j.values.virtual.VirtualRelationshipValue
import org.neo4j.values.virtual.VirtualValues

import java.util
import java.util.function.Consumer

import scala.jdk.CollectionConverters.IteratorHasAsScala

final class PathValueBuilder(state: QueryState) extends Consumer[RelationshipVisitor] {
  private var nodes = new util.ArrayList[VirtualNodeValue]()
  private var rels = new util.ArrayList[VirtualRelationshipValue]()
  private var nulled = false

  def result(): AnyValue = {
    if (nulled) Values.NO_VALUE
    else VirtualValues.pathReference(nodes, rels)
  }

  def clear(): PathValueBuilder = {
    nodes = new util.ArrayList[VirtualNodeValue](nodes.size())
    rels = new util.ArrayList[VirtualRelationshipValue](rels.size())
    nulled = false
    this
  }

  def previousNode: VirtualNodeValue = nodes.get(nodes.size() - 1)

  def addNoValue(): PathValueBuilder = {
    nulled = true
    this
  }

  def addNode(nodeOrNull: AnyValue): PathValueBuilder = nullCheck(nodeOrNull) {
    val node = nodeOrNull.asInstanceOf[VirtualNodeValue]
    nodes.add(node)
    this
  }

  def addRelationship(relOrNull: AnyValue): PathValueBuilder = nullCheck(relOrNull) {
    addRelationship(relOrNull.asInstanceOf[VirtualRelationshipValue])
  }

  def addRelationship(rel: VirtualRelationshipValue): PathValueBuilder = {
    rels.add(rel)
    this
  }

  def addIncomingRelationship(relOrNull: AnyValue): PathValueBuilder = nullCheck(relOrNull) {
    val rel = relOrNull.asInstanceOf[VirtualRelationshipValue]
    rels.add(rel)
    nodes.add(VirtualValues.node(rel.startNodeId(this)))
    this
  }

  def addOutgoingRelationship(relOrNull: AnyValue): PathValueBuilder = nullCheck(relOrNull) {
    val rel = relOrNull.asInstanceOf[VirtualRelationshipValue]
    rels.add(rel)
    nodes.add(VirtualValues.node(rel.endNodeId(this)))
    this
  }

  def addUndirectedRelationship(relOrNull: AnyValue): PathValueBuilder = nullCheck(relOrNull) {
    val rel = relOrNull.asInstanceOf[VirtualRelationshipValue]
    val start = rel.startNodeId(this)
    if (start == previousNode.id()) addOutgoingRelationship(rel)
    else addIncomingRelationship(rel)
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
      addUndirectedRelationship(first)
      consumeIterator(relIterator.asScala)
    }
    this
  }

  override def accept(t: RelationshipVisitor): Unit = {
    val cursor = state.cursors.relationshipScanCursor
    state.query.singleRelationship(t.id(), cursor)
    // This ignores that a relationship might have been deleted here, this is weird but it is backwards compatible
    cursor.next()
    t.visit(cursor.sourceNodeReference(), cursor.targetNodeReference(), cursor.`type`())
  }

  private def nullCheck[A <: AnyRef](value: A)(f: => PathValueBuilder): PathValueBuilder = value match {
    case x if (x == null) || (Values.NO_VALUE eq x) =>
      nulled = true
      this

    case _ => f
  }
}
