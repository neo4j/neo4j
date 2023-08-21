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
package org.neo4j.cypher.internal.runtime

import org.neo4j.graphdb.Entity
import org.neo4j.graphdb.Node
import org.neo4j.graphdb.Path
import org.neo4j.graphdb.Relationship
import org.neo4j.graphdb.traversal.Paths

import java.lang
import java.util

import scala.collection.mutable
import scala.jdk.CollectionConverters.IterableHasAsJava
import scala.jdk.CollectionConverters.IterableHasAsScala

case class PathImpl(pathEntities: Entity*)
    extends org.neo4j.graphdb.Path {

  val sz = pathEntities.size

  val (nodeList, relList) = {
    if (sz % 2 == 0)
      throw new IllegalArgumentException(
        s"Tried to construct a path that is not built like a path: even number of elements ($sz)"
      )
    var x = 0
    val nodes = new Array[Node](pathEntities.size / 2 + 1)
    val rels = new Array[Relationship](pathEntities.size / 2)
    try {
      pathEntities.foreach(e => {
        if ((x % 2) == 0) nodes.update(x / 2, e.asInstanceOf[Node])
        else rels.update((x - 1) / 2, e.asInstanceOf[Relationship])
        x += 1
      })
    } catch {
      case e: ClassCastException =>
        throw new IllegalArgumentException(
          s"Tried to construct a path that is not built like a path: $pathEntities",
          e
        )
    }
    (new mutable.WrappedArray.ofRef(nodes), new mutable.WrappedArray.ofRef(rels))
  }

  require(isProperPath, s"Tried to construct a path that is not built like a path: $pathEntities")

  def isProperPath: Boolean = {
    val atLeastOneNode = nodeList.nonEmpty
    val relsLengthEqualsToNodesLengthMinusOne = relList.length == nodeList.length - 1
    atLeastOneNode && relsLengthEqualsToNodesLengthMinusOne
  }

  def startNode(): Node = nodeList.head

  def endNode(): Node = nodeList.last

  def lastRelationship(): Relationship = if (relList.isEmpty) null else relList.last

  def relationships(): lang.Iterable[Relationship] = relList.asJava

  def nodes(): lang.Iterable[Node] = nodeList.asJava

  def length(): Int = relList.size

  override def iterator(): util.Iterator[Entity] = pathEntities.asJava.iterator()

  override def toString: String = Paths.defaultPathToString(this)

  override def canEqual(that: Any): Boolean = that != null && that.isInstanceOf[Path]

  override def equals(p1: Any): Boolean = {
    if (!canEqual(p1)) return false

    val that = p1.asInstanceOf[Path]

    that.asScala.toList == pathEntities.toList
  }

  def reverseNodes(): lang.Iterable[Node] = nodeList.reverse.asJava

  def reverseRelationships(): lang.Iterable[Relationship] = relList.reverse.asJava
}
