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
package org.neo4j.cypher.internal.pipes

import org.neo4j.cypher.PathImpl
import org.neo4j.graphdb.{Path, PropertyContainer}
import scala.collection.JavaConverters._
import collection.Seq
import java.lang.String
import org.neo4j.cypher.internal.commands.{PathPattern, RelatedTo, NamedPath}
import org.neo4j.cypher.internal.symbols.{PathType, Identifier}
import collection.mutable.Map

class NamedPathPipe(source: Pipe, path: NamedPath) extends Pipe {
  def getFirstNode[U]: String = {
    val firstNode = path.pathPattern.head match {
      case RelatedTo(left, _, _, _, _, _, _) => left
      case path:PathPattern => path.start
    }
    firstNode
  }

  def createResults[U](params: Map[String, Any]): Traversable[Map[String, Any]] = source.createResults(params).map(m => {
    def get(x: String): PropertyContainer = m(x).asInstanceOf[PropertyContainer]

    val firstNode: String = getFirstNode

    val p: Seq[PropertyContainer] = path.pathPattern.foldLeft(Seq(get(firstNode)))((soFar, p) => p match {
      case RelatedTo(_, right, relName, _, _, _, _) => soFar ++ Seq(get(relName), get(right))
      case path:PathPattern => getPath(m, path.pathName, soFar)
    })

    m += (path.pathName -> buildPath(p))
  })


  private def buildPath(pieces: Seq[PropertyContainer]): Path =
    if (pieces.contains(null))
      null
    else
      new PathImpl(pieces: _*)

  //WARNING: This method can return NULL
  def getPath(m: Map[String, Any], key: String, soFar: Seq[PropertyContainer]): Seq[PropertyContainer] = {
    val m1 = m(key)

    if (m1 == null)
      return Seq(null)

    val path = m1.asInstanceOf[Path].iterator().asScala.toSeq
    val pathTail = if (path.head == soFar.last) {
      path.tail
    } else {
      path.reverse.tail
    }

    soFar ++ pathTail
  }

  val symbols = source.symbols.add(Identifier(path.pathName, PathType()))

  override def executionPlan(): String = source.executionPlan() + "\r\nExtractPath(" + path.pathName + " = " + path.pathPattern.mkString(", ") + ")"
}