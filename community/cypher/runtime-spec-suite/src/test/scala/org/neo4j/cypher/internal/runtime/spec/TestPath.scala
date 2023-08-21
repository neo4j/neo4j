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
package org.neo4j.cypher.internal.runtime.spec

import org.neo4j.graphdb.Entity
import org.neo4j.graphdb.Node
import org.neo4j.graphdb.Path
import org.neo4j.graphdb.Relationship

import java.lang
import java.util

import scala.jdk.CollectionConverters.SeqHasAsJava

/**
 * Test org.neo4j.graphdb.Path implementation, used for runtime spec suite assertions.
 */
case class TestPath(override val startNode: Node, _relationships: Seq[Relationship]) extends Path {

  private val _nodes =
    _relationships.foldLeft(List(startNode))((nodes, r) => r.getOtherNode(nodes.head) :: nodes).reverse

  override def endNode(): Node = _nodes.last

  override def lastRelationship(): Relationship = _relationships.last

  override def relationships(): lang.Iterable[Relationship] = _relationships.asJava

  override def reverseRelationships(): lang.Iterable[Relationship] = _relationships.reverse.asJava

  override def nodes(): lang.Iterable[Node] = _nodes.asJava

  override def reverseNodes(): lang.Iterable[Node] = _nodes.reverse.asJava

  override def length(): Int = _relationships.size

  override def iterator(): util.Iterator[Entity] = ???

  def take(n: Int): TestPath =
    TestPath(startNode, _relationships.take(n))

  def slice(from: Int, until: Int): TestPath =
    TestPath(_relationships(from).getStartNode, _relationships.slice(from, until))

  def reverse: TestPath =
    TestPath(endNode(), _relationships.reverse)

  def nodeAt(offset: Int): Node = _nodes(offset)

  def relationshipAt(offset: Int): Relationship = _relationships(offset)
}
