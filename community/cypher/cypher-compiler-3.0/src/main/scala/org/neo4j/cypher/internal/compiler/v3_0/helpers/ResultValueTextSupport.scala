/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v3_0.helpers

import org.neo4j.cypher.internal.compiler.v3_0.commands.values.KeyToken
import org.neo4j.cypher.internal.compiler.v3_0.spi.QueryContext
import org.neo4j.graphdb.{Path, Node, Relationship}

import scala.collection.Map

object ResultValueTextSupport {

  import org.neo4j.cypher.internal.compiler.v3_0.helpers.ScalaValueCompatibility._

  def text(a: Any)(implicit context: QueryContext): String = {
    val scalaValue = asShallowScalaValue(a)
    scalaValue match {
      case node: Node => s"$node${props(node)}"
      case relationship: Relationship => s":${relationship.getType.name()}[${relationship.getId}]${props(relationship)}"
      case path: Path => path.toString
      case map: Map[_, _] => makeString(map)
      case opt: Option[_] => opt.map(text).getOrElse("None")
      case array: Array[_] => array.map(elem => text(elem)).mkString("[", ",", "]")
      case iterable: Iterable[_] => iterable.map(elem => text(elem)).mkString("[", ",", "]")
      case str: String => "\"" + str + "\""
      case token: KeyToken => token.name
      case null => "<null>"
      case value => value.toString
    }
  }

  private def makeString(m: Map[_, _])(implicit context: QueryContext) = m.map { case (k, v) => s"$k -> ${text(v)}" }.mkString("{", ", ", "}")

  private def props(n: Node)(implicit context: QueryContext): String = {
    val ops = context.nodeOps
    val properties = ops.propertyKeyIds(n.getId)
    val keyValStrings = properties.map(pkId => s"${context.getPropertyKeyName(pkId)}:${text(ops.getProperty(n.getId, pkId))}")
    keyValStrings.mkString("{", ",", "}")
  }

  private def props(r: Relationship)(implicit context: QueryContext): String = {
    val ops = context.relationshipOps
    val properties = ops.propertyKeyIds(r.getId)
    val keyValStrings = properties.map(pkId => s"${context.getPropertyKeyName(pkId)}:${text(ops.getProperty(r.getId, pkId))}")
    keyValStrings.mkString("{", ",", "}")
  }
}
