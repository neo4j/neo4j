/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.cypher.internal.commands

import expressions.Expression
import org.neo4j.graphdb.{Path, PropertyContainer}
import org.neo4j.cypher.PathImpl
import scala.collection.JavaConverters._
import collection.Map

case class NamedPath(pathName: String, pathPattern: Pattern*) extends Traversable[Pattern] with PathExtractor {
  def foreach[U](f: (Pattern) => U) {
    pathPattern.foreach(f)
  }

  def rewrite(f: Expression => Expression) = NamedPath(pathName, pathPattern.map(_.rewrite(f)): _*)

  override def toString() = "NamedPath(%s = %s)".format(pathName, pathPattern.mkString(","))
}

trait PathExtractor {
  def pathPattern:Seq[Pattern]
  def getPath(ctx: Map[String, Any]): Path = {
    def get(x: String): PropertyContainer = ctx(x).asInstanceOf[PropertyContainer]

    val firstNode: String = getFirstNode

    val p: Seq[PropertyContainer] = pathPattern.foldLeft(Seq(get(firstNode)))((soFar, p) => p match {
      case SingleNode(name)                         => Seq(get(name))
      case RelatedTo(_, right, relName, _, _, _, _) => soFar ++ Seq(get(relName), get(right))
      case path: PathPattern                        => getPath(ctx, path.pathName, soFar)
    })

    buildPath(p)
  }

  private def getFirstNode[U]: String =
    pathPattern.head match {
      case RelatedTo(left, _, _, _, _, _, _) => left
      case SingleNode(name)                  => name
      case path: PathPattern                 => path.start
    }

  private def buildPath(pieces: Seq[PropertyContainer]): Path =
    if (pieces.contains(null))
      null
    else
      new PathImpl(pieces: _*)

  //WARNING: This method can return NULL
  private def getPath(m: Map[String, Any], key: String, soFar: Seq[PropertyContainer]): Seq[PropertyContainer] = {
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
}