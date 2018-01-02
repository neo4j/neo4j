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
package org.neo4j.cypher.internal

import java.lang.{Iterable => JavaIterable}
import java.util.{Iterator => JavaIterator}

import org.neo4j.cypher.internal.compiler.v2_2.helpers.CypherArray
import org.neo4j.graphdb.{Node, Path, PropertyContainer, Relationship}
import org.neo4j.kernel.Traversal

import scala.collection.JavaConverters._
import scala.collection.mutable

// TODO: This is only here for accomodating cypher-compiler-1.9. Do not touch, do not import, purge it with fire post 2.3
case class PathImpl(pathEntities: PropertyContainer*)
  extends org.neo4j.graphdb.Path with Traversable[PropertyContainer] with CypherArray {

  val (nodeList,relList) = {
    if (pathEntities.size % 2 == 0) throw new IllegalArgumentException("Tried to construct a path that is not built like a path: even number of elements.");
    var x = 0
    val nodes = new Array[Node](pathEntities.size/2+1)
    val rels = new Array[Relationship](pathEntities.size/2)
    try {
      pathEntities.foreach(e => {
        if ((x % 2) == 0) nodes.update(x/2, e.asInstanceOf[Node])
        else rels.update((x-1)/2, e.asInstanceOf[Relationship])
        x+=1
      })
    } catch {
      case e: ClassCastException => throw new IllegalArgumentException("Tried to construct a path that is not built like a path",e);
    }
    (new mutable.WrappedArray.ofRef(nodes),new mutable.WrappedArray.ofRef(rels))
  }

  require(isProperPath, "Tried to construct a path that is not built like a path")

  def isProperPath: Boolean = {
    val atLeastOneNode = nodeList.length > 0
    val relsLengthEqualsToNodesLengthMinusOne = relList.length == nodeList.length - 1
    atLeastOneNode && relsLengthEqualsToNodesLengthMinusOne
  }

  def startNode(): Node = nodeList.head

  def endNode(): Node = nodeList.last

  def lastRelationship(): Relationship = if (relList.isEmpty) null else relList.last

  def relationships(): JavaIterable[Relationship] = relList.asJava

  def nodes(): JavaIterable[Node] = nodeList.asJava

  def length(): Int = relList.size

  def iterator(): JavaIterator[PropertyContainer] = pathEntities.asJava.iterator()

  def foreach[U](f: (PropertyContainer) => U) {
    pathEntities.foreach(f(_))
  }

  override def toString(): String = Traversal.defaultPathToString(this)

  override def canEqual(that: Any) = that != null && that.isInstanceOf[Path]

  override def equals(p1: Any):Boolean = {
    if (!canEqual(p1)) return false

    val that = p1.asInstanceOf[Path]

    that.asScala.toList == pathEntities.toList
  }

  def reverseNodes(): JavaIterable[Node] = nodeList.reverse.asJava

  def reverseRelationships(): JavaIterable[Relationship] = relList.reverse.asJava
}
