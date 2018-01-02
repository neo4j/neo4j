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
package org.neo4j.cypher.internal.compiler.v2_3.commands

import org.neo4j.cypher.internal.compiler.v2_3.commands.expressions.PathImpl
import org.neo4j.graphdb.{PropertyContainer, Path}
import scala.collection.Map
import collection.JavaConverters._

// TODO: This is duplicated with NamedPathPipe
trait PathExtractor {
  def pathPattern:Seq[Pattern]
  def getPath(ctx: Map[String, Any]): Path = {
    def get(x: String): PropertyContainer = ctx(x).asInstanceOf[PropertyContainer]

    val firstNode: String = getFirstNode

    val p: Seq[PropertyContainer] = pathPattern.foldLeft(get(firstNode) :: Nil)((soFar, p) => p match {
      case SingleNode(name, _, _)                => soFar :+ get(name)
      case RelatedTo(_, right, relName, _, _, _) => soFar :+ get(relName) :+ get(right.name)
      case path: PathPattern                     => getPath(ctx, path.pathName, soFar)
    })

    buildPath(p)
  }

  private def getFirstNode[U]: String =
    pathPattern.head match {
      case RelatedTo(left, _, _, _, _, _) => left.name
      case SingleNode(name, _, _)         => name
      case path: PathPattern              => path.left.name
    }

  private def buildPath(pieces: Seq[PropertyContainer]): Path =
    if (pieces.contains(null))
      null
    else
      new PathImpl(pieces: _*)

  //WARNING: This method can return NULL
  private def getPath(m: Map[String, Any], key: String, soFar: List[PropertyContainer]): List[PropertyContainer] = {
    val m1 = m(key)

    if (m1 == null)
      return null::Nil

    val path = m1.asInstanceOf[Path].iterator().asScala.toList
    val pathTail = if (path.head == soFar.last) {
      path.tail
    } else {
      path.reverse.tail
    }

    soFar ++ pathTail
  }
}
