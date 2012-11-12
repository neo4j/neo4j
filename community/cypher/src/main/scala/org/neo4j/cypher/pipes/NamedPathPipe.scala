/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package org.neo4j.cypher.pipes

import org.neo4j.cypher.{PathImpl, SymbolTable}
import org.neo4j.graphdb.{Path, PropertyContainer}
import scala.collection.JavaConverters._
import org.neo4j.cypher.commands._

class NamedPathPipe(source: Pipe, path: NamedPath) extends Pipe {
  def getFirstNode[U]: String = {
    val firstNode = path.pathPattern.head match {
      case RelatedTo(left, right, relName, x, xx, optional) => left
      case VarLengthRelatedTo(pathName, start, end, minHops, maxHops, relType, direction, iterableRel, optional) => start
      case ShortestPath(_, start, _, _, _, _, _) => start
    }
    firstNode
  }

  def foreach[U](f: (Map[String, Any]) => U) {

    source.foreach(m => {
      def get(x: String): PropertyContainer = m(x).asInstanceOf[PropertyContainer]

      val firstNode: String = getFirstNode

      val p = path.pathPattern.foldLeft(Seq(get(firstNode)))((soFar, p) => p match {
        case RelatedTo(left, right, relName, x, xx, optional) => soFar ++ Seq(get(relName), get(right))
        case VarLengthRelatedTo(pathName, start, end, minHops, maxHops, relType, direction, iterableRel, optional) => getPath(m, pathName, soFar)
        case ShortestPath(pathName, _, _, _, _, _, _) => getPath(m, pathName, soFar)
      })

      f(m + (path.pathName -> new PathImpl(p: _*)))
    })
  }

  def getPath(m: Map[String, Any], key: String, soFar: Seq[PropertyContainer]): Seq[PropertyContainer] = {
    val path = m(key).asInstanceOf[Path].iterator().asScala.toSeq
    val pathTail = if (path.head == soFar.last) {
      path.tail
    } else {
      path.reverse.tail
    }

    soFar ++ pathTail
  }

  val symbols: SymbolTable = source.symbols.add(Seq(PathIdentifier(path.pathName)))
}