/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
package org.neo4j.cypher

import java.util.Iterator
import java.lang.Iterable
import org.neo4j.graphdb.{Relationship, PropertyContainer, Node}
import scala.collection.JavaConverters._

case class PathImpl(pathEntities: PropertyContainer*)
  extends org.neo4j.graphdb.Path
  with Traversable[PropertyContainer]
  with CypherArray {

  assert( isProperPath )

  def isProperPath: Boolean = {
    var x = true
    val (nodes,rels) = pathEntities.partition(e=>{
      x = !x
      !x
    })

    val nodesContainOnlyNodes = nodes.forall(_.isInstanceOf[Node])
    val relsAreAllRels = rels.forall(_.isInstanceOf[Relationship])
    val atLeastOneNode = nodes.length > 0
    val relsLengthEqualsToNodesLengthMinusOne = rels.length == nodes.length - 1
    nodesContainOnlyNodes && relsAreAllRels && atLeastOneNode && relsLengthEqualsToNodesLengthMinusOne
  }

  def startNode(): Node = pathEntities.head.asInstanceOf[Node]

  def endNode(): Node = pathEntities.last.asInstanceOf[Node]

  def lastRelationship(): Relationship = null

  def relationships(): Iterable[Relationship] = entities[Relationship]

  def entities[T <: PropertyContainer](implicit m: Manifest[T]): Iterable[T] = pathEntities.filter(m.erasure.isInstance(_)).map(_.asInstanceOf[T]).toIterable.asJava

  def nodes(): Iterable[Node] = entities[Node]

  def length(): Int = pathEntities.length

  def iterator(): Iterator[PropertyContainer] = null

  def foreach[U](f: (PropertyContainer) => U) {
    pathEntities.foreach(f(_))
  }

  override def toString(): String = "Path(" + pathEntities.mkString(", ") + ")"
}